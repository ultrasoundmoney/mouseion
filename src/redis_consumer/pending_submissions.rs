use std::sync::Arc;

use anyhow::Result;
use fred::{pool::RedisPool, prelude::StreamsInterface};
use futures::{channel::mpsc::Sender, select, FutureExt, SinkExt};
use tokio::{sync::Notify, task::JoinHandle, time::sleep};
use tracing::{debug, error, info, trace};

use crate::STREAM_NAME;

use super::{
    decoding::XAutoClaimResponse, IdBlockSubmission, CONSUMER_ID, GROUP_NAME,
    MAX_MESSAGE_ACK_DURATION_MS, MESSAGE_BATCH_SIZE, PULL_PENDING_TIMEOUT,
};

pub struct PendingSubmissionRedisConsumer {
    // Redis scans a finite number of pending messages and provides us with an ID to continue
    // in case there were more messages pending than we claimed. Redis loops it back around to 0-0
    // whenever we've claimed all pending messages.
    autoclaim_id: String,
    client: RedisPool,
}

impl PendingSubmissionRedisConsumer {
    pub fn new(client: RedisPool) -> Self {
        Self {
            autoclaim_id: "0-0".to_string(),
            client,
        }
    }

    // When a consumer crashes, it forgets its ID and any delivered but unacked messages will sit
    // forever as pending messages for the consumer. Although it is easy to give consumers stable IDs
    // we'd still have the same problem when scaling down the number of active consumers. Instead, we
    // try not to crash, and have this function which claims any pending messages that have hit
    // MAX_MESSAGE_PROCESS_DURATION_MS. A message getting processed twice is fine.
    pub async fn pull_pending_submissions(
        &mut self,
        mut submissions_tx: Sender<IdBlockSubmission>,
    ) -> Result<Vec<IdBlockSubmission>> {
        loop {
            let XAutoClaimResponse(next_autoclaim_id, id_block_submissions) = self
                .client
                .xautoclaim(
                    STREAM_NAME,
                    GROUP_NAME,
                    &*CONSUMER_ID,
                    MAX_MESSAGE_ACK_DURATION_MS,
                    &self.autoclaim_id,
                    Some(MESSAGE_BATCH_SIZE),
                    false,
                )
                .await?;

            self.autoclaim_id = next_autoclaim_id;

            if id_block_submissions.is_empty() {
                debug!("no pending messages to claim");
                // We sleep here to avoid busy waiting.
                trace!(
                    duration_ms = PULL_PENDING_TIMEOUT.as_millis(),
                    "completed pending submissions pull cycle, sleeping"
                );
                sleep(*PULL_PENDING_TIMEOUT).await;
            } else {
                debug!(
                    next_autoclaim_id = self.autoclaim_id,
                    count = id_block_submissions.len(),
                    "claimed pending messages"
                );
                for id_block_submission in id_block_submissions {
                    submissions_tx.feed(id_block_submission).await?;
                }
                submissions_tx.flush().await?;
            }
        }
    }
}

pub fn run_pending_submissions_thread(
    client: RedisPool,
    shutdown_notify: Arc<Notify>,
    submissions_tx: Sender<IdBlockSubmission>,
) -> JoinHandle<()> {
    let mut consumer = PendingSubmissionRedisConsumer::new(client);
    tokio::spawn(async move {
        // When the new submissions loop hits an error, we wouldn't know about it and keep the
        // submissions channel open, keeping the program alive. We listen for a shutdown signal
        // and shut down the thread when we receive it.
        select! {
            _ = shutdown_notify.notified().fuse() => {
                info!("pull pending submissions thread exiting");
            },
            result = consumer.pull_pending_submissions(submissions_tx).fuse() => {
                match result {
                    Ok(_) => {
                        error!("pending submissions thread exited unexpectedly");
                        shutdown_notify.notify_waiters();
                    }
                    Err(e) => {
                        error!(?e, "failed to pull pending submissions");
                        shutdown_notify.notify_waiters();
                    }
                }

            }
        }
    })
}
