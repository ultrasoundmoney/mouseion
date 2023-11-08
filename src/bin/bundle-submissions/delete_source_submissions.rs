use std::sync::Arc;

use ::object_store as object_store_lib;
use anyhow::Context;
use backoff::ExponentialBackoff;
use futures::{channel::mpsc::Receiver, StreamExt, TryStreamExt};
use mouseion::units::Slot;
use object_store_lib::aws::AmazonS3;
use object_store_lib::ObjectStore;
use tokio::{spawn, task::JoinHandle};
use tracing::{debug, error, info, instrument, trace, warn};

#[instrument(skip(object_store), fields(%slot))]
async fn delete_slot(object_store: Arc<AmazonS3>, slot: Slot) -> anyhow::Result<()> {
    let path = slot.partial_s3_path();
    let mut block_submission_meta_stream = object_store.list(Some(&path)).await?;

    while let Some(object_meta) = block_submission_meta_stream.try_next().await? {
        trace!(location = %object_meta.location, "deleting submission");

        backoff::future::retry(ExponentialBackoff::default(), || async {
            object_store
                .delete(&object_meta.location)
                .await
                .context("failed to execute object store delete operation")
                .map_err(|err| {
                    let err_str = err.to_string().to_lowercase();
                    let is_retryable = err_str.contains("409 conflict")
                        || err_str.contains("connection closed")
                        || err_str.contains("connection reset by peer")
                        || err_str.contains("503 service unavailable");
                    if is_retryable {
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
    }

    debug!("deleted all submissions for slot");

    Ok(())
}

const DELETE_SLOT_CONCURRENCY: usize = 32;

pub fn run_delete_source_submissions_thread(
    object_store: Arc<AmazonS3>,
    slots_to_delete_rx: Receiver<Slot>,
) -> JoinHandle<()> {
    spawn(async move {
        slots_to_delete_rx
            .map(Ok)
            .try_for_each_concurrent(DELETE_SLOT_CONCURRENCY, |slot| {
                let object_store = object_store.clone();
                delete_slot(object_store, slot)
            })
            .await
            .unwrap();
    })
}
