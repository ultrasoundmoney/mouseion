//! Pulls new block submissions from Redis.
//!
//! ## Technical Notes
//! - The consumer is built for Redis v7, it works with Redis v6 but encounters a subtle issue
//! where pending entries lists become bloated with deleted message IDs under heavy load. Further
//! details for how to implement v6 support can be found in the decoding module. Happy to support
//! and merge any implementation.
mod decoding;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{bail, Result};
use block_submission_archiver::{env::ENV_CONFIG, BlockSubmission, STREAM_NAME};
use fred::prelude::{RedisClient, RedisResult, StreamsInterface};
use futures::{
    channel::mpsc::{Receiver, Sender},
    select, FutureExt, SinkExt,
};
use lazy_static::lazy_static;
use nanoid::nanoid;
use tokio::{sync::Notify, task::JoinHandle, time::sleep};
use tracing::{debug, error, info, instrument, trace};

use crate::{health::RedisConsumerHealth, GROUP_NAME};

use decoding::{ConsumerInfo, XAutoClaimResponse, XReadGroupResponse};

// Claim pending messages that are more than 1 minutes old.
const MAX_MESSAGE_PROCESS_DURATION_MS: u64 = 60 * 1000;
// In order to avoid polling we use Redis' BLOCK option. This is the maximum time we'll want Redis
// to wait for new data before returning an empty response.
const BLOCK_DURATION_MS: u64 = 8000;
// After a consumer has been idle for 8 minutes, we consider it crashed and remove it.
const MAX_CONSUMER_IDLE_MS: u64 = 8 * 60 * 1000;
// Number of messages to pull from Redis at a time.
const MESSAGE_BATCH_SIZE: u64 = 8;
const ACK_SUBMISSIONS_DURATION_MS: u64 = 200;

lazy_static! {
    static ref CONSUMER_ID: String = ENV_CONFIG.pod_name.clone().unwrap_or(nanoid!(4));
    static ref PULL_PENDING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);
}

pub type IdBlockSubmission = (String, BlockSubmission);

#[derive(Clone)]
pub struct RedisConsumer {
    // Redis scans a finite number of pending messages and provides us with an ID to continue
    // in case there were more messages pending than we claimed. Redis loops it back around to 0-0
    // whenever we've claimed all pending messages.
    autoclaim_id: Arc<Mutex<String>>,
    client: RedisClient,
    message_health: RedisConsumerHealth,
    shutdown_notify: Arc<Notify>,
}

impl RedisConsumer {
    pub fn new(
        client: RedisClient,
        message_health: RedisConsumerHealth,
        shutdown_notify: Arc<Notify>,
    ) -> Self {
        Self {
            autoclaim_id: Arc::new(Mutex::new("0-0".to_string())),
            client,
            message_health,
            shutdown_notify,
        }
    }

    // If no consumer group exists, create one, if it already exists, we'll get an error we can
    // ignore.
    #[instrument(skip(self))]
    pub async fn ensure_consumer_group_exists(&self) -> Result<()> {
        let result: RedisResult<()> = self
            .client
            .xgroup_create(STREAM_NAME, GROUP_NAME, "0", false)
            .await;

        if let Err(e) = result {
            if e.to_string().contains("BUSYGROUP") {
                // The group already exists. This is fine.
                debug!("consumer group already exists");
            } else {
                // Propagate the original error
                bail!(e);
            }
        }
        Ok(())
    }

    // When a consumer gets terminated it leaves behind its consumer group entry. This fn cleans up
    // dead consumers.
    #[instrument(skip(self))]
    pub async fn delete_dead_consumers(&self) -> Result<()> {
        let consumers: Vec<ConsumerInfo> =
            self.client.xinfo_consumers(STREAM_NAME, GROUP_NAME).await?;

        if consumers.is_empty() {
            // No consumers, nothing to do.
            debug!("no consumers, nothing to clean up");
            return Ok(());
        }

        debug!(count = consumers.len(), "found consumers");

        for consumer in consumers {
            if consumer.pending == 0 && consumer.idle > MAX_CONSUMER_IDLE_MS {
                info!(
                    name = consumer.name,
                    idle_seconds = consumer.idle / 1000,
                    "removing long-idle, likely dead consumer"
                );
                self.client
                    .xgroup_delconsumer(STREAM_NAME, GROUP_NAME, &consumer.name)
                    .await?;
            } else {
                trace!(
                    name = consumer.name,
                    pending = consumer.pending,
                    idle = consumer.idle,
                    "consumer looks alive, leaving it"
                );
            }
        }

        Ok(())
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
                        submissions_tx.send(id_block_submission).await?;
                    }
                }
                None => {
                    debug!("block submissions stream empty");
                }
            }
        }
    }

    // When a consumer crashes, it forgets its ID and any delivered but unacked messages will sit
    // forever as pending messages for the consumer. Although it is easy to give consumers stable IDs
    // we'd still have the same problem when scaling down the number of active consumers. Instead, we
    // try not to crash, and have this function which claims any pending messages that have hit
    // MAX_MESSAGE_PROCESS_DURATION_MS. A message getting processed twice is fine.
    pub async fn pull_pending_submissions(
        &self,
        mut submissions_tx: Sender<IdBlockSubmission>,
    ) -> Result<Vec<IdBlockSubmission>> {
        loop {
            let autoclaim_id = self.autoclaim_id.lock().unwrap().clone();

            let XAutoClaimResponse(next_autoclaim_id, id_block_submissions) = self
                .client
                .xautoclaim(
                    STREAM_NAME,
                    GROUP_NAME,
                    &*CONSUMER_ID,
                    MAX_MESSAGE_PROCESS_DURATION_MS,
                    &autoclaim_id,
                    Some(MESSAGE_BATCH_SIZE),
                    false,
                )
                .await?;

            *self.autoclaim_id.lock().unwrap() = next_autoclaim_id;

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
                    next_autoclaim_id = *self.autoclaim_id.lock().unwrap(),
                    count = id_block_submissions.len(),
                    "claimed pending messages"
                );
                for id_block_submission in id_block_submissions {
                    submissions_tx.send(id_block_submission).await?;
                }
            }
        }
    }

    pub fn run_new_submissions_thread(
        self: Arc<Self>,
        submissions_tx: Sender<IdBlockSubmission>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            // When the pending loop hits an error, we wouldn't know about it and keep the
            // submissions channel open, keeping the program alive. We listen for a shutdown signal
            // and shut down the thread when we receive it.
            select! {
                _ = self.shutdown_notify.notified().fuse() => {
                    info!("pull new submissions thread exiting");
                },
                result = self.pull_new_submissions(submissions_tx).fuse() => {
                    match result {
                        Ok(_) => {
                            error!("new submissions thread exited unexpectedly");
                            self.shutdown_notify.notify_waiters();
                        }
                        Err(e) => {
                            error!("failed to pull new block submissions: {:?}", e);
                            self.shutdown_notify.notify_waiters();
                        }
                    }

                }
            }
        })
    }

    pub fn run_pending_submissions_thread(
        self: Arc<Self>,
        submissions_tx: Sender<IdBlockSubmission>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            // When the new submissions loop hits an error, we wouldn't know about it and keep the
            // submissions channel open, keeping the program alive. We listen for a shutdown signal
            // and shut down the thread when we receive it.
            select! {
                _ = self.shutdown_notify.notified().fuse() => {
                    info!("pull pending submissions thread exiting");
                },
                result = self.pull_pending_submissions(submissions_tx).fuse() => {
                    match result {
                        Ok(_) => {
                            error!("pending submissions thread exited unexpectedly");
                            self.shutdown_notify.notify_waiters();
                        }
                        Err(e) => {
                            error!("failed to pull pending block submissions: {:?}", e);
                            self.shutdown_notify.notify_waiters();
                        }
                    }

                }
            }
        })
    }

    pub fn run_ack_submissions_thread(
        self: Arc<Self>,
        mut stored_submissions_rx: Receiver<String>,
    ) -> JoinHandle<()> {
        tokio::spawn({
            async move {
                // In order to acknowledge efficiently, we take all pending messages from the
                // channel. This means we can't use a `while let Some(msg)` loop to automatically
                // shut down when the channel closes. We also won't receive wake-ups but instead
                // poll the channel every 200ms.
                loop {
                    let mut ids = Vec::new();

                    while let Ok(id) = stored_submissions_rx.try_next() {
                        match id {
                            Some(id) => {
                                ids.push(id);
                            }
                            None => {
                                info!("no more block submissions to ack, thread exiting");
                                return;
                            }
                        }
                    }

                    if !ids.is_empty() {
                        let ids_len = ids.len();
                        let result: RedisResult<()> =
                            self.client.xack(STREAM_NAME, GROUP_NAME, ids).await;

                        match result {
                            Ok(()) => {
                                debug!(count = ids_len, "acked block submissions");
                            }
                            Err(e) => {
                                error!("failed to ack block submission: {:?}", e);
                                self.shutdown_notify.notify_waiters();
                                break;
                            }
                        }
                    } else {
                        trace!("no block submissions to ack");
                    }

                    trace!(
                        duration_ms = ACK_SUBMISSIONS_DURATION_MS,
                        "completed ack cycle, sleeping"
                    );
                    sleep(Duration::from_millis(ACK_SUBMISSIONS_DURATION_MS)).await;
                }
            }
        })
    }
}
