use std::sync::Arc;

use anyhow::Result;
use fred::prelude::RedisClient;
use futures::{channel::mpsc::Receiver, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use tokio::{select, sync::Notify};
use tracing::{debug, error, info};

use crate::{message_consumer::IdArchiveEntryPair, performance::TimedExt};

const MAX_CONCURRENCY: usize = 4;

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

    async fn archive_entry(&self, id_archive_entry_pair: IdArchiveEntryPair) -> Result<()> {
        let json_gz_bytes = id_archive_entry_pair.entry.compress().await?;

        self.object_store
            .put(&id_archive_entry_pair.entry.bundle_path(), json_gz_bytes)
            .timed("object_store_put")
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
                self.archive_entry(id_archive_entry_pair).await
            })
            .await?;

        Ok(())
    }

    pub async fn run_archive_entries(&self, archive_entries_rx: Receiver<IdArchiveEntryPair>) {
        debug!("archiving received IdArchiveEntryPairs");
        let shutdown_notify = self.shutdown_notify.clone();
        select! {
            _ = shutdown_notify.notified() => {
                debug!("shutting down process messages thread");
            }
            result = self.archive_entries(archive_entries_rx) => {
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
