use std::time::SystemTime;

use backoff::ExponentialBackoff;
use bytes::Bytes;
use futures::{
    channel::mpsc::{Receiver, Sender},
    SinkExt, StreamExt,
};
use mouseion::units::Slot;
use object_store::{aws::AmazonS3, ObjectStore};
use tokio::{spawn, task::JoinHandle};
use tracing::{debug, error, info, instrument, warn};

use crate::ObjectPath;

#[instrument(skip(bundle_gz, object_store), fields(path = path.to_string(), %slot))]
async fn store_bundle(
    bundle_gz: Bytes,
    object_store: &AmazonS3,
    path: ObjectPath,
    slot: Slot,
) -> anyhow::Result<()> {
    backoff::future::retry(ExponentialBackoff::default(), || async {
        object_store
            .put(&path, bundle_gz.clone())
            .await
            .map_err(|err| {
                if err.to_string().contains("409 Conflict")
                    || err.to_string().contains("connection closed")
                    || err
                        .to_string()
                        .contains("connection closed before message completed")
                {
                    warn!("failed to execute OVH put operation: {}, retrying", err);
                    backoff::Error::Transient {
                        err,
                        retry_after: None,
                    }
                } else {
                    error!("{}", err);
                    backoff::Error::Permanent(err)
                }
            })
    })
    .await?;

    debug!("stored bundle");

    Ok(())
}

pub fn run_store_bundles_thread(
    bundles_stroe: AmazonS3,
    mut compressed_bundles_rx: Receiver<(Slot, ObjectPath, Bytes)>,
    mut slots_to_delete_tx: Sender<Slot>,
) -> JoinHandle<()> {
    spawn(async move {
        let mut count = 0;
        let start = SystemTime::now();

        while let Some((slot, path, bundle_gz)) = compressed_bundles_rx.next().await {
            match store_bundle(bundle_gz, &bundles_stroe, path, slot).await {
                Ok(_) => slots_to_delete_tx.send(slot).await.unwrap(),
                Err(e) => {
                    panic!("store_bundle failed: {:?}", e);
                }
            };

            count += 1;

            if count % 10 == 0 {
                let elapsed = SystemTime::now().duration_since(start).unwrap();
                let rate = count as f64 / elapsed.as_secs_f64();
                info!("stored {} bundles at {:.2} bundles/sec", count, rate);
            }
        }
    })
}
