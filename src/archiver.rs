use std::{sync::Arc, time::Duration};

use anyhow::Result;
use fred::prelude::RedisClient;
use futures::{channel::mpsc::Receiver, select, FutureExt, StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use object_store::ObjectStore;
use tokio::sync::Notify;
use tracing::{debug, error, info};

use crate::{message_consumer::IdArchiveEntryPair, performance::TimedExt};

const MAX_CONCURRENCY: usize = 4;
lazy_static! {
    static ref MAX_RETRY_DURATION_MS: Duration = Duration::from_secs(8);
}

pub struct Archiver<OS: ObjectStore> {
    client: RedisClient,
    object_store: Arc<OS>,
    shutdown_notify: Arc<Notify>,
}

impl<OS: ObjectStore> Archiver<OS> {
    pub fn new(client: RedisClient, object_store: OS, shutdown_notify: Arc<Notify>) -> Self {
        Self {
            client,
            object_store: Arc::new(object_store),
            shutdown_notify,
        }
    }

    async fn store(
        &self,
        id_archive_entry_pair: &IdArchiveEntryPair,
        json_gz_bytes: bytes::Bytes,
    ) -> Result<()> {
        let result = self
            .object_store
            .put(
                &id_archive_entry_pair.entry.bundle_path(),
                json_gz_bytes.clone(),
            )
            .await;
        match result {
            Ok(_) => {
                debug!(
                    state_root = ?id_archive_entry_pair.entry.state_root(),
                    "stored bundle",
                );
                Ok(())
            }
            Err(e) => {
                // Sometimes archivers manage to grab and attempt to store the same message. As
                // the operations are idempotent, we simply skip 409s.
                if e.to_string().contains("409 Conflict") {
                    debug!(
                        state_root = ?id_archive_entry_pair.entry.state_root(),
                        "hit 409 - conflict when storing bundle, ignoring"
                    );
                    Ok(())
                } else {
                    Err(e.into())
                }
            }
        }
    }

    async fn archive_entry(&self, id_archive_entry_pair: &IdArchiveEntryPair) -> Result<()> {
        let json_gz_bytes = id_archive_entry_pair.entry.compress().await?;

        self.store(&id_archive_entry_pair, json_gz_bytes)
            .timed("store_with_backoff")
            .await?;

        id_archive_entry_pair
            .ack(&self.client)
            .timed("ack_archive_entry")
            .await?;

        Ok::<_, anyhow::Error>(())
    }

    async fn archive_entries(
        &self,
        archive_entries_rx: Receiver<IdArchiveEntryPair>,
    ) -> Result<()> {
        archive_entries_rx
            .map(Ok)
            .try_for_each_concurrent(MAX_CONCURRENCY, |id_archive_entry_pair| async move {
                self.archive_entry(&id_archive_entry_pair).await
            })
            .await?;

        Ok(())
    }

    pub async fn run_archive_entries(&self, archive_entries_rx: Receiver<IdArchiveEntryPair>) {
        debug!("archiving received IdArchiveEntryPairs");
        let shutdown_notify = self.shutdown_notify.clone();
        select! {
            _ = shutdown_notify.notified().fuse() => {
                debug!("shutting down process messages thread");
            }
            result = self.archive_entries(archive_entries_rx).fuse() => {
                match result {
                    Ok(_) => info!("stopped processing messages"),
                    Err(e) => {
                        error!("error while processing messages: {:?}", e);
                        shutdown_notify.notify_waiters();
                    }
                }
            }
        }
    }
}
