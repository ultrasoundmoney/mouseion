use std::sync::Arc;

use futures::{
    channel::mpsc::{Receiver, Sender},
    SinkExt, StreamExt, TryStreamExt,
};
use mouseion::{units::Slot, BlockSubmission};
use object_store::{aws::AmazonS3, ObjectStore};
use tokio::{spawn, task::JoinHandle};
use tracing::{debug, instrument, trace, warn};

/// Does three things given a slot.
/// 1. Fetch all block submissions for the slot.
/// 2. Decompress them.
/// 3. Bundle them together in a Vec.
#[instrument(skip(object_store), fields(%slot))]
async fn bundle_slot(
    object_store: Arc<AmazonS3>,
    slot: Slot,
) -> anyhow::Result<Vec<BlockSubmission>> {
    let path = slot.partial_s3_path();

    debug!(%path, "fetching all submissions for slot");

    let mut block_submission_meta_stream = object_store.list(Some(&path)).await?;

    let mut block_submissions = Vec::new();

    while let Some(block_submission_meta) = block_submission_meta_stream.try_next().await? {
        let bytes_gz = object_store
            .get(&block_submission_meta.location)
            .await?
            .bytes()
            .await?;

        let decoder = flate2::read::GzDecoder::new(&bytes_gz[..]);
        let block_submission: BlockSubmission = serde_json::from_reader(decoder)?;

        trace!(
            block_hash = block_submission.block_hash(),
            "fetched block submission"
        );

        block_submissions.push(block_submission);
    }

    debug!("fetched all bundles for slot");

    if block_submissions.is_empty() {
        warn!("no submissions found for slot");
    }

    Ok(block_submissions)
}

const FETCH_BUNDLE_CONCURRENCY: usize = 8;

pub fn run_bundle_slots_thread(
    object_store: Arc<AmazonS3>,
    slots_rx: Receiver<Slot>,
    bundles_tx: Sender<(Slot, Vec<BlockSubmission>)>,
) -> JoinHandle<()> {
    spawn(async move {
        slots_rx
            .map(Ok)
            .try_for_each_concurrent(FETCH_BUNDLE_CONCURRENCY, |slot| {
                let object_store = object_store.clone();
                let mut bundles_tx = bundles_tx.clone();
                async move {
                    let bundle = bundle_slot(object_store, slot).await?;
                    bundles_tx.send((slot, bundle)).await?;
                    Ok::<_, anyhow::Error>(())
                }
            })
            .await
            .unwrap();
    })
}