//! Pulls new messages from Redis and sends them to the archiver.
//! TODO: instead of using of using XAUTOCLAIM, use XPENDING to find out which consumers are
//! holding pending messages. Claim their messages, process them, and then delete the consumer.
//! Consider switching to more popular `redis` crate.
use std::{collections::HashMap, sync::Arc};

use anyhow::{bail, Result};
use fred::{
    prelude::{RedisClient, RedisResult, StreamsInterface},
    types::RedisValue,
};
use lazy_static::lazy_static;
use nanoid::nanoid;
use payload_archiver::{ArchiveEntry, STREAM_NAME};
use tokio::{
    select,
    sync::{mpsc, Notify},
};
use tracing::{debug, error, info, trace};

use crate::{GROUP_NAME, MESSAGE_BATCH_SIZE};

// Claim pending messages that are more than 1 minutes old.
const MAX_MESSAGE_PROCESS_DURATION_MS: u64 = 60 * 1000;

lazy_static! {
    // 64^4 collision space.
    static ref CONSUMER_ID: String = nanoid!(4);
    static ref PULL_PENDING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);
}

pub struct IdArchiveEntryPair {
    pub id: String,
    pub entry: ArchiveEntry,
}

impl From<RedisValue> for IdArchiveEntryPair {
    fn from(value: RedisValue) -> Self {
        match value {
            RedisValue::Array(mut array) => {
                let id: String = array.remove(0).convert().unwrap();
                let raw_entry: HashMap<String, String> = array.remove(0).convert().unwrap();
                let entry = raw_entry.into();

                Self { id, entry }
            }
            _ => panic!("expected RedisValue::Array with id and message"),
        }
    }
}

impl IdArchiveEntryPair {
    pub async fn ack(&self, client: &RedisClient) -> Result<()> {
        client.xack(STREAM_NAME, GROUP_NAME, &self.id).await?;
        Ok(())
    }
}

// Consider converting from RedisValue directly.
type PendingMessages = (String, Vec<(String, HashMap<String, String>)>);

pub struct MessageConsumer {
    client: RedisClient,
    shutdown_notify: Arc<Notify>,
    tx: mpsc::Sender<IdArchiveEntryPair>,
}

impl MessageConsumer {
    pub fn new(
        client: RedisClient,
        shutdown_notify: Arc<Notify>,
        tx: mpsc::Sender<IdArchiveEntryPair>,
    ) -> Self {
        Self {
            client,
            shutdown_notify,
            tx,
        }
    }

    async fn ensure_consumer_group_exists(client: &RedisClient) -> Result<()> {
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

    async fn retrieve_new_messages(&self) -> Result<()> {
        Self::ensure_consumer_group_exists(&self.client).await?;

        loop {
            let result: RedisResult<RedisValue> = self
                .client
                .xreadgroup(
                    GROUP_NAME,
                    &*CONSUMER_ID,
                    Some(MESSAGE_BATCH_SIZE as u64),
                    None,
                    false,
                    STREAM_NAME,
                    // The > operator means we only get new messages, not old ones. This also means if
                    // any messages are pending for our consumer, we won't get them. This is fine.
                    // We're set up such that if we crash, we appear with a new consumer ID. This
                    // means the pending messages will be 'forgotten', and after MIN_UNACKED_TIME_MS
                    // will be claimed by another consumer.
                    ">",
                )
                .await;

            let messages = {
                match result? {
                    RedisValue::Array(mut streams) => {
                        // This consumer group only returns results for one stream. Take the first.
                        let stream = streams.remove(0);
                        match stream {
                            RedisValue::Array(mut unknown) => {
                                // Is always STREAM_NAME
                                let messages = unknown.remove(1);
                                match messages {
                                    RedisValue::Array(messages) => messages,
                                    _ => bail!("expected RedisValue::Array"),
                                }
                            }
                            _ => bail!("expected RedisValue::Array containing name and messages"),
                        }
                    }
                    RedisValue::Null => {
                        // No new messages, sleep for a bit.
                        trace!("no new messages, sleeping for 1s");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                    _ => bail!("expected RedisValue::Array containing streams"),
                }
            };

            debug!(count = messages.len(), "received new messages");

            for message in messages {
                self.tx.send(message.into()).await?;
            }
        }
    }

    pub async fn run_consume_new_messages(&self) {
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

    // When a consumer crashes, it forgets its ID and any delivered but unacked messages will sit
    // forever as pending messages for the consumer. Although it is easy to give consumers stable IDs
    // we'd still have the same problem when scaling down the number of active consumers. Instead, we
    // try not to crash, and have this function which claims any pending messages that have hit
    // MAX_MESSAGE_PROCESS_DURATION_MS. A message getting processed twice is fine.
    pub async fn consume_pending_messages(&self) -> Result<()> {
        loop {
            let result: RedisResult<PendingMessages> = self
                .client
                .xautoclaim_values(
                    STREAM_NAME,
                    GROUP_NAME,
                    &*CONSUMER_ID,
                    MAX_MESSAGE_PROCESS_DURATION_MS,
                    "0-0",
                    None,
                    false,
                )
                .await;

            let (_stream_key, messages) = result?;

            if messages.is_empty() {
                trace!(
                    "no pending messages, sleeping for {}s",
                    PULL_PENDING_TIMEOUT.as_secs()
                );
                tokio::time::sleep(*PULL_PENDING_TIMEOUT).await;
            } else {
                debug!(count = messages.len(), "claimed pending messages");
                for (id, map) in messages {
                    self.tx
                        .send(IdArchiveEntryPair {
                            id,
                            entry: map.into(),
                        })
                        .await?;
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
