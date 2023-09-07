//! Pulls new block submissions from Redis.
//!
//! ## Technical Notes
//! - The consumer is built for Redis v7, it works with Redis v6 but encounters a subtle issue
//! where pending entries lists become bloated with deleted message IDs under heavy load. Further
//! details for how to implement v6 support can be found in the decoding module. Happy to support
//! and merge any implementation.
mod decoding;

use anyhow::{bail, Result};
use block_submission_archiver::{env::ENV_CONFIG, BlockSubmission, STREAM_NAME};
use fred::prelude::{RedisClient, RedisResult, StreamsInterface};
use lazy_static::lazy_static;
use nanoid::nanoid;
use tracing::{debug, info, instrument, trace};

use crate::{health::RedisConsumerHealth, performance::TimedExt, GROUP_NAME};

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

lazy_static! {
    static ref CONSUMER_ID: String = ENV_CONFIG.pod_name.clone().unwrap_or(nanoid!(4));
    static ref PULL_PENDING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);
}

pub type IdBlockSubmissionPairs = (String, BlockSubmission);

#[derive(Clone)]
pub struct RedisConsumer {
    client: RedisClient,
    message_health: RedisConsumerHealth,
    // Redis scans a finite number of pending messages and provides us with an ID to continue
    // in case there were more messages pending than we claimed. Redis loops it back around to 0-0
    // whenever we've claimed all pending messages.
    autoclaim_id: String,
}

impl RedisConsumer {
    pub fn new(client: RedisClient, message_health: RedisConsumerHealth) -> Self {
        Self {
            client,
            message_health,
            autoclaim_id: "0-0".to_string(),
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

    // Pulls new block submissions from Redis.
    async fn pull_new_submissions(&self) -> Result<Vec<(String, BlockSubmission)>> {
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
            Some(id_archive_entry_pairs) => {
                debug!(
                    count = id_archive_entry_pairs.len(),
                    "received new block submissions"
                );

                self.message_health.set_last_message_received_now();

                Ok(id_archive_entry_pairs)
            }
            None => {
                debug!("block submissions stream empty");
                Ok(Vec::new())
            }
        }
    }

    // When a consumer crashes, it forgets its ID and any delivered but unacked messages will sit
    // forever as pending messages for the consumer. Although it is easy to give consumers stable IDs
    // we'd still have the same problem when scaling down the number of active consumers. Instead, we
    // try not to crash, and have this function which claims any pending messages that have hit
    // MAX_MESSAGE_PROCESS_DURATION_MS. A message getting processed twice is fine.
    async fn pull_pending_submissions(&mut self) -> Result<Vec<IdBlockSubmissionPairs>> {
        let XAutoClaimResponse(next_autoclaim_id, id_archive_entry_pairs) = self
            .client
            .xautoclaim(
                STREAM_NAME,
                GROUP_NAME,
                &*CONSUMER_ID,
                MAX_MESSAGE_PROCESS_DURATION_MS,
                &self.autoclaim_id,
                Some(MESSAGE_BATCH_SIZE),
                false,
            )
            .await?;

        self.autoclaim_id = next_autoclaim_id;

        if id_archive_entry_pairs.is_empty() {
            debug!("no pending messages to claim");
            Ok(id_archive_entry_pairs)
        } else {
            debug!(
                next_autoclaim_id = self.autoclaim_id,
                count = id_archive_entry_pairs.len(),
                "claimed pending messages"
            );
            Ok(id_archive_entry_pairs)
        }
    }

    pub async fn pull_id_block_submission_pairs(&mut self) -> Result<Vec<IdBlockSubmissionPairs>> {
        let new_messages = self
            .pull_new_submissions()
            .timed("pull_new_submissions")
            .await?;
        let pending_messages = self
            .pull_pending_submissions()
            .timed("pull_pending_submissions")
            .await?;
        Ok(new_messages.into_iter().chain(pending_messages).collect())
    }
}
