use std::sync::Arc;

use ::object_store::{aws::AmazonS3, ObjectStore};
use anyhow::Context;
use futures::{
    channel::mpsc::{Receiver, Sender},
    stream, SinkExt, StreamExt, TryStreamExt,
};
use mouseion::{units::Slot, BlockSubmission};
use tokio::{
    spawn,
    task::{spawn_blocking, JoinHandle},
};
use tracing::{debug, instrument, trace, warn};

const FETCH_SUBMISSION_CONCURRENCY: usize = 16;

/// Does three things given a slot.
/// 1. Fetch all block submissions for the slot.
/// 2. Decompress them.
/// 3. Bundle them together in a Vec.
#[instrument(skip(object_store), fields(%slot))]
async fn bundle_slot(object_store: &AmazonS3, slot: Slot) -> anyhow::Result<Vec<BlockSubmission>> {
    let path = slot.partial_s3_path();

    debug!(%path, "fetching all submissions for slot");

    // Fetching the full list of paths fails sometimes mid-stream. As it is tricky to retry at that
    // point, we fetch the full list and retry this whole operation. Having a retrying stream
    // somehow would be nice.
    let list_with_retry = || async {
        let metas = object_store
            .list(Some(&path))
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let paths = metas
            .iter()
            .map(|meta| meta.location.clone())
            .collect::<Vec<_>>();
        Ok(paths)
    };

    let paths =
        backoff::future::retry(backoff::ExponentialBackoff::default(), list_with_retry).await?;

    let block_submissions = stream::iter(paths)
        .map(|path| async move {
            let get_with_retry = || async {
                let bytes = object_store.get(&path).await?.bytes().await?;
                Ok(bytes)
            };
            let bytes_gz =
                backoff::future::retry(backoff::ExponentialBackoff::default(), get_with_retry)
                    .await?;

            let block_submission: BlockSubmission = spawn_blocking(move || {
                let decoder = flate2::read::GzDecoder::new(&bytes_gz[..]);
                let block_submission: BlockSubmission = serde_json::from_reader(decoder)?;
                Ok::<_, anyhow::Error>(block_submission)
            })
            .await
            .context("failed to join block submission decompression thread")?
            .context("failed to decompress block submission")?;

            trace!(
                block_hash = block_submission.block_hash(),
                "fetched block submission"
            );

            Ok::<_, anyhow::Error>(block_submission)
        })
        .buffered(FETCH_SUBMISSION_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    debug!("fetched all bundles for slot");

    if block_submissions.is_empty() {
        warn!("no submissions found for slot");
    }

    Ok(block_submissions)
}

const FETCH_BUNDLE_CONCURRENCY: usize = 16;

pub fn run_bundle_slots_thread(
    bundles_tx: Sender<(Slot, Vec<BlockSubmission>)>,
    object_store: Arc<AmazonS3>,
    slots_rx: Receiver<Slot>,
) -> JoinHandle<()> {
    spawn(async move {
        slots_rx
            .map(Ok)
            .try_for_each_concurrent(FETCH_BUNDLE_CONCURRENCY, |slot| {
                let mut bundles_tx = bundles_tx.clone();
                let object_store = object_store.clone();
                async move {
                    let bundle = bundle_slot(&object_store, slot).await?;
                    bundles_tx.send((slot, bundle)).await?;
                    Ok::<_, anyhow::Error>(())
                }
            })
            .await
            .unwrap();
    })
}
