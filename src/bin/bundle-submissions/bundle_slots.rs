use ::object_store::{aws::AmazonS3, ObjectStore};
use anyhow::Context;
use futures::{
    channel::mpsc::{Receiver, Sender},
    SinkExt, StreamExt, TryStreamExt,
};
use mouseion::{object_store, units::Slot, BlockSubmission};
use tokio::{
    spawn,
    task::{spawn_blocking, JoinHandle},
};
use tracing::{debug, instrument, trace, warn};

/// Does three things given a slot.
/// 1. Fetch all block submissions for the slot.
/// 2. Decompress them.
/// 3. Bundle them together in a Vec.
#[instrument(skip(object_store), fields(%slot))]
async fn bundle_slot(object_store: AmazonS3, slot: Slot) -> anyhow::Result<Vec<BlockSubmission>> {
    let path = slot.partial_s3_path();

    debug!(%path, "fetching all submissions for slot");

    let mut block_submission_meta_stream = object_store.list(Some(&path)).await?;

    let mut block_submissions = Vec::new();

    while let Some(block_submission_meta) = block_submission_meta_stream.try_next().await? {
        let get_with_retry = || async {
            let location = &block_submission_meta.location;
            let bytes = object_store.get(location).await?.bytes().await?;
            Ok(bytes)
        };
        let bytes_gz =
            backoff::future::retry(backoff::ExponentialBackoff::default(), get_with_retry).await?;

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
    slots_rx: Receiver<Slot>,
    bundles_tx: Sender<(Slot, Vec<BlockSubmission>)>,
) -> JoinHandle<()> {
    spawn(async move {
        slots_rx
            .map(Ok)
            .try_for_each_concurrent(FETCH_BUNDLE_CONCURRENCY, |slot| {
                let object_store = object_store::build_submissions_store().unwrap();
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
