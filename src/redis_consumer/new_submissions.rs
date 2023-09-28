use std::sync::Arc;

use anyhow::Result;
use fred::{pool::RedisPool, prelude::StreamsInterface};
use futures::{channel::mpsc::Sender, select, FutureExt, SinkExt};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{debug, error, info, trace};

use crate::{health::RedisConsumerHealth, BlockSubmission, STREAM_NAME};

use super::{
    decoding::XReadGroupResponse, IdBlockSubmission, BLOCK_DURATION_MS, CONSUMER_ID, GROUP_NAME,
    MESSAGE_BATCH_SIZE,
};

pub struct NewSubmissionRedisConsumer {
    client: RedisPool,
    message_health: RedisConsumerHealth,
}

impl NewSubmissionRedisConsumer {
    pub fn new(client: RedisPool, message_health: RedisConsumerHealth) -> Self {
        Self {
            client,
            message_health,
        }
    }

    /// Pulls new block submissions from Redis.
    /// We use a BLOCK_DURATION_MS and therefore do not need to worry about busy waiting.
    pub async fn pull_new_submissions(
        &self,
        mut submissions_tx: Sender<IdBlockSubmission>,
    ) -> Result<Vec<(String, BlockSubmission)>> {
        loop {
            let result: XReadGroupResponse = self
                .client
                .xreadgroup(
                    GROUP_NAME,
                    &*CONSUMER_ID,
                    Some(MESSAGE_BATCH_SIZE),
                    Some(BLOCK_DURATION_MS),
                    false,
                    STREAM_NAME,
                    // The > operator means we only get new messages, not old ones. This also means if
                    // any messages are pending for our consumer, we won't get them. This is fine.
                    // We're set up such that if we crash, we appear with a new consumer ID. This
                    // means the pending messages will be 'forgotten', and after MIN_UNACKED_TIME_MS
                    // will be claimed by another consumer.
                    ">",
                )
                .await?;

            match result.0 {
                Some(id_block_submissions) => {
                    debug!(
                        count = id_block_submissions.len(),
                        "received new block submissions"
                    );

                    self.message_health.set_last_message_received_now();

                    for id_block_submission in id_block_submissions {
                        trace!(id = %id_block_submission.0, block_submission = ?id_block_submission.1, "consumed new block submission");
                        submissions_tx.feed(id_block_submission).await?;
                    }
                    submissions_tx.flush().await?;
                }
                None => {
                    debug!("block submissions stream empty");
                }
            }
        }
    }
}

pub fn run_new_submissions_thread(
    client: RedisPool,
    message_health: RedisConsumerHealth,
    shutdown_notify: Arc<Notify>,
    submissions_tx: Sender<IdBlockSubmission>,
) -> JoinHandle<()> {
    let consumer = NewSubmissionRedisConsumer::new(client, message_health);
    tokio::spawn(async move {
        // When the pending loop hits an error, we wouldn't know about it and keep the
        // submissions channel open, keeping the program alive. We listen for a shutdown signal
        // and shut down the thread when we receive it.
        select! {
            _ = shutdown_notify.notified().fuse() => {
                info!("pull new submissions thread exiting");
            },
            result = consumer.pull_new_submissions(submissions_tx).fuse() => {
                match result {
                    Ok(_) => {
                        error!("new submissions thread exited unexpectedly");
                        shutdown_notify.notify_waiters();
                    }
                    Err(e) => {
                        error!("failed to pull new block submissions: {:?}", e);
                        shutdown_notify.notify_waiters();
                    }
                }

            }
        }
    })
}
