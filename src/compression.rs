use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use futures::{
    channel::mpsc::{Receiver, Sender},
    SinkExt, StreamExt, TryStreamExt,
};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{error, info};

use crate::{redis_consumer::IdBlockSubmission, BlockSubmission};

pub type IdBlockSubmissionCompressed = (String, BlockSubmission, Bytes);

const ARCHIVING_MAX_CONCURRENCY: usize = 16;

async fn compress_submissions(
    submissions_rx: Receiver<IdBlockSubmission>,
    compressed_submissions_tx: Sender<IdBlockSubmissionCompressed>,
) -> Result<()> {
    submissions_rx
        .map(Ok)
        .try_for_each_concurrent(ARCHIVING_MAX_CONCURRENCY, |(id, block_submission)| {
            let mut compressed_submissions_tx = compressed_submissions_tx.clone();
            async move {
                let json_gz_bytes = block_submission.compress().await?;

                compressed_submissions_tx
                    .send((id, block_submission, json_gz_bytes))
                    .await?;

                Ok(())
            }
        })
        .await
}

pub fn run_compression_thread(
    submissions_rx: Receiver<IdBlockSubmission>,
    compressed_submissions_tx: Sender<IdBlockSubmissionCompressed>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        match compress_submissions(submissions_rx, compressed_submissions_tx).await {
            Ok(()) => {
                info!("compression thread exiting");
            }
            Err(e) => {
                error!("compression thread exiting with error: {:?}", e);
                shutdown_notify.notify_waiters();
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc;
    use std::sync::Arc;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn test_run_compression_thread() {
        let (mut submissions_tx, submissions_rx) = mpsc::channel(32);
        let (compressed_submissions_tx, _) = mpsc::channel(32);
        let shutdown_notify = Arc::new(Notify::new());

        let handle = run_compression_thread(
            submissions_rx,
            compressed_submissions_tx.clone(),
            shutdown_notify.clone(),
        );
        submissions_tx.close_channel();
        handle.await.unwrap();
    }
}
