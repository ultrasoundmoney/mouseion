use std::{io::Read, sync::Arc};

use bytes::Bytes;
use chrono::{Datelike, Timelike};
use futures::{
    channel::mpsc::{Receiver, Sender},
    SinkExt, StreamExt,
};
use mouseion::{units::Slot, BlockSubmission};
use object_store::path::Path;
use tokio::{
    spawn,
    sync::Semaphore,
    task::{spawn_blocking, JoinHandle},
};
use tracing::{debug, info, instrument};

use crate::ObjectPath;

fn bundle_path_from_slot(slot: Slot) -> Path {
    let slot_date_time = slot.date_time();
    let year = slot_date_time.year();
    let month = slot_date_time.month();
    let day = slot_date_time.day();
    let hour = slot_date_time.hour();
    let minute = slot_date_time.minute();

    let path_string = format!("{year}/{month:02}/{day:02}/{hour:02}/{minute:02}/{slot}.ndjson.gz");
    Path::from(path_string)
}

#[instrument(skip(bundle), fields(%slot))]
async fn compress_bundle(bundle: Vec<BlockSubmission>, slot: Slot) -> anyhow::Result<Bytes> {
    // convert each block submission to a json string, then join them with newline
    let ndjson: String = bundle
        .into_iter()
        .map(|item| match serde_json::to_string(&item) {
            Ok(json_string) => Ok(json_string + "\n"),
            Err(_) => Err(anyhow::anyhow!("Failed to serialize item")),
        })
        .collect::<Result<Vec<String>, _>>()?
        .join("");

    let bytes_gz = spawn_blocking(move || {
        // get the bytes
        let bytes = ndjson.as_bytes();

        // start the encoder with the bytes
        let mut encoder = flate2::read::GzEncoder::new(bytes, flate2::Compression::default());
        let mut bytes_gz = vec![];

        // encode (compress) the bytes
        match encoder.read_to_end(&mut bytes_gz) {
            Ok(_) => Ok(bytes_gz.into()),
            Err(err) => Err(anyhow::anyhow!("Failed to compress data: {:?}", err)),
        }
    })
    .await??;

    debug!("compressed bundle");

    Ok(bytes_gz)
}

const COMPRESSION_CONCURRENCY: usize = 8;

pub fn run_compression_thread(
    mut bundles_rx: Receiver<(Slot, Vec<BlockSubmission>)>,
    compressed_bundles_tx: Sender<(Slot, ObjectPath, Bytes)>,
) -> JoinHandle<()> {
    let semaphore = Arc::new(Semaphore::new(COMPRESSION_CONCURRENCY));
    spawn(async move {
        while let Some((slot, bundle)) = bundles_rx.next().await {
            let mut compressed_bundles_tx = compressed_bundles_tx.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            spawn(async move {
                match compress_bundle(bundle, slot).await {
                    Ok(bytes_gz) => {
                        let path = bundle_path_from_slot(slot);

                        compressed_bundles_tx
                            .send((slot, path, bytes_gz))
                            .await
                            .unwrap();
                    }
                    Err(e) => {
                        panic!("compress_bundle failed: {:?}", e);
                    }
                }
                drop(permit);
            });
        }

        info!("finished compressing bundles");
    })
}
