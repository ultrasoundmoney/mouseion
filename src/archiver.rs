use std::sync::Arc;

use anyhow::Result;
use fred::prelude::RedisClient;
use futures::StreamExt;
use object_store::ObjectStore;
use tokio::{
    select,
    sync::{mpsc::Receiver, Notify},
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info};

use crate::{message_consumer::IdArchiveEntryPair, performance::TimedExt};

const MAX_CONCURRENCY: usize = 4;

pub struct Archiver<OS: ObjectStore> {
    client: RedisClient,
    archive_entries_rx: ReceiverStream<IdArchiveEntryPair>,
    object_store: Arc<OS>,
    shutdown_notify: Arc<Notify>,
}

impl<OS: ObjectStore> Archiver<OS> {
    pub fn new(
        client: RedisClient,
        archive_entries_rx: Receiver<IdArchiveEntryPair>,
        object_store: OS,
        shutdown_notify: Arc<Notify>,
    ) -> Self {
        Self {
            client,
            archive_entries_rx: ReceiverStream::new(archive_entries_rx),
            object_store: Arc::new(object_store),
            shutdown_notify,
        }
    }

    async fn archive_entry(
        object_store: &OS,
        client: &RedisClient,
        id_archive_entry_pair: IdArchiveEntryPair,
    ) -> Result<()> {
        let json_gz_bytes = id_archive_entry_pair.entry.compress().await?;

        object_store
            .put(&id_archive_entry_pair.entry.bundle_path(), json_gz_bytes)
            .timed("object_store_put")
            .await?;

        id_archive_entry_pair
            .ack(&client)
            .timed("ack_archive_entry")
            .await?;

        Ok::<_, anyhow::Error>(())
    }

    async fn archive_entries(self) -> Result<()> {
        self.archive_entries_rx
            .for_each_concurrent(MAX_CONCURRENCY, |id_archive_entry_pair| async {
                match Self::archive_entry(&self.object_store, &self.client, id_archive_entry_pair)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Error while archiving entry: {:?}", e);
                        self.shutdown_notify.notify_waiters();
                    }
                }
            })
            .await;

        Ok(())
    }

    pub async fn run_archive_entries(self) {
        debug!("archiving received IdArchiveEntryPairs");
        let shutdown_notify = self.shutdown_notify.clone();
        select! {
            _ = shutdown_notify.notified() => {
                debug!("shutting down process messages thread");
            }
            result = self.archive_entries() => {
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
