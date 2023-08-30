//! Pulls new messages from Redis and sends them to the archiver.
//! TODO: instead of using of using XAUTOCLAIM, use XPENDING to find out which consumers are
//! holding pending messages. Claim their messages, process them, and then delete the consumer.
//! Consider switching to more popular `redis` crate.
mod decoding;

use std::{fmt::Debug, sync::Arc};

use anyhow::{bail, Result};
use fred::prelude::{RedisClient, StreamsInterface};
use lazy_static::lazy_static;
use nanoid::nanoid;
use payload_archiver::{ArchiveEntry, STREAM_NAME};
use tokio::{
    select,
    sync::{mpsc, Notify},
};
use tracing::{debug, error, info, trace};

use crate::{health::MessageConsumerHealth, GROUP_NAME, MESSAGE_BATCH_SIZE};

use decoding::{ConsumerInfo, XAutoClaimResponse, XReadGroupResponse};

// Claim pending messages that are more than 1 minutes old.
const MAX_MESSAGE_PROCESS_DURATION_MS: u64 = 60 * 1000;
// In order to avoid polling we use Redis' BLOCK option. This is the maximum time we'll want Redis
// to wait for new data before returning an empty response.
const BLOCK_DURATION_MS: u64 = 8000;
// After a consumer has been idle for 8 minutes, we consider it crashed and remove it.
const MAX_CONSUMER_IDLE_MS: u64 = 8 * 60 * 1000;

lazy_static! {
    // 64^4 collision space.
    static ref CONSUMER_ID: String = nanoid!(4);
    static ref PULL_PENDING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);
}

#[derive(Debug)]
pub struct IdArchiveEntryPair {
    pub id: String,
    pub entry: ArchiveEntry,
}

impl IdArchiveEntryPair {
    pub async fn ack(&self, client: &RedisClient) -> Result<()> {
        client.xack(STREAM_NAME, GROUP_NAME, &self.id).await?;
        Ok(())
    }
}

pub struct MessageConsumer {
    client: RedisClient,
    message_health: MessageConsumerHealth,
    shutdown_notify: Arc<Notify>,
    tx: mpsc::Sender<IdArchiveEntryPair>,
}

impl MessageConsumer {
    pub fn new(
        client: RedisClient,
        message_health: MessageConsumerHealth,
        shutdown_notify: Arc<Notify>,
        tx: mpsc::Sender<IdArchiveEntryPair>,
    ) -> Self {
        Self {
            client,
            message_health,
            shutdown_notify,
            tx,
        }
    }

    async fn ensure_consumer_group_exists(client: &RedisClient) -> Result<()> {
        debug!("ensuring consumer group exists");

        // If no consumer group exists, create one, if it already exists, we'll get an error we can
        // ignore.
        let result: std::result::Result<(), fred::prelude::RedisError> = client
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

    /// Pulls new messages from Redis and sends them to the archiver.
    async fn retrieve_new_messages(&self) -> Result<()> {
        Self::ensure_consumer_group_exists(&self.client).await?;

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

            let id_archive_entry_pairs: Vec<IdArchiveEntryPair> = match result.0 {
                Some(id_archive_entry_pairs) => id_archive_entry_pairs,
                None => {
                    // No new messages available.
                    trace!("no new messages, sleeping for 1s");
                    continue;
                }
            };

            debug!(
                count = id_archive_entry_pairs.len(),
                "received new messages"
            );

            for id_archive_entry_pair in id_archive_entry_pairs {
                self.tx.send(id_archive_entry_pair).await?;
            }

            self.message_health.set_last_message_received_now();
        }
    }

    pub async fn run_consume_new_messages(&self) {
        debug!("pulling new messages");
        select! {
            _ = self.shutdown_notify.notified() => {
                info!("pulling new messages thread shutting down");
            }
            result = self.retrieve_new_messages() => {
                match result {
                    Ok(_) => info!("stopped pulling new messages"),
                    Err(e) => {
                        error!("error while pulling new messages: {:?}", e);
                        self.shutdown_notify.notify_waiters();
                    }
                }
            }

        }
    }

    pub async fn delete_dead_consumers(&self) -> Result<()> {
        Self::ensure_consumer_group_exists(&self.client).await?;

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

    // When a consumer crashes, it forgets its ID and any delivered but unacked messages will sit
    // forever as pending messages for the consumer. Although it is easy to give consumers stable IDs
    // we'd still have the same problem when scaling down the number of active consumers. Instead, we
    // try not to crash, and have this function which claims any pending messages that have hit
    // MAX_MESSAGE_PROCESS_DURATION_MS. A message getting processed twice is fine.
    pub async fn consume_pending_messages(&self) -> Result<()> {
        Self::ensure_consumer_group_exists(&self.client).await?;

        // Redis scans a finite number of pending messages and provides us with an ID to continue
        // in case there were more messages pending than we claimed.
        let mut autoclaim_id: String = "0-0".to_string();

        loop {
            let XAutoClaimResponse(next_autoclaim_id, messages) = self
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

            autoclaim_id = next_autoclaim_id;

            if messages.is_empty() {
                // No pending messages, sleep for a bit.
                trace!(
                    "no pending messages, sleeping for {}s",
                    PULL_PENDING_TIMEOUT.as_secs()
                );
                tokio::time::sleep(*PULL_PENDING_TIMEOUT).await;
            } else {
                debug!(count = messages.len(), "claimed pending messages");
                for id_archive_entry_pair in messages {
                    self.tx.send(id_archive_entry_pair).await?;
                }
            }
        }
    }

    pub async fn run_consume_pending_messages(&self) {
        debug!("claiming pending messages");
        select! {
                _ = self.shutdown_notify.notified() => {
                    debug!("shutting down claim pending messages thread");
                }
                result = self.consume_pending_messages() => {
                    match result {
                        Ok(_) => info!("stopped pulling pending messages"),
                        Err(e) => {
                            error!("error while pulling pending messages: {:?}", e);
                            self.shutdown_notify.notify_waiters();
                        }
                    }
                }
        }
    }
}
