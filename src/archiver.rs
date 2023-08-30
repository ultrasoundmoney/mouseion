use std::sync::Arc;

use anyhow::Result;
use fred::prelude::RedisClient;
use object_store::ObjectStore;
use tokio::{
    select,
    sync::{
        mpsc::{self, Receiver},
        Notify, Semaphore,
    },
};
use tracing::{debug, error, info};

use crate::message_consumer::IdArchiveEntryPair;

const MAX_CONCURRENT: usize = 4;

pub struct Archiver<OS: ObjectStore> {
    client: RedisClient,
    archive_entries_rx: Receiver<IdArchiveEntryPair>,
    object_store: Arc<OS>,
    shutdown_notify: Arc<Notify>,
}

impl<OS: ObjectStore> Archiver<OS> {
    pub fn new(
        client: RedisClient,
        archive_entries_rx: mpsc::Receiver<IdArchiveEntryPair>,
        object_store: OS,
        shutdown_notify: Arc<Notify>,
    ) -> Self {
        Self {
            client,
            archive_entries_rx,
            object_store: Arc::new(object_store),
            shutdown_notify,
        }
    }

    async fn archive_entries(&mut self) -> Result<()> {
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));

        while let Some(id_archive_entry_pair) = self.archive_entries_rx.recv().await {
            tokio::spawn({
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let object_store = self.object_store.clone();
                let client = self.client.clone();
                async move {
                    let json_gz_bytes = id_archive_entry_pair.entry.compress().await?;

                    object_store
                        .put(&id_archive_entry_pair.entry.bundle_path(), json_gz_bytes)
                        .await?;

                    id_archive_entry_pair.ack(&client).await?;

                    drop(permit);

                    Ok::<_, anyhow::Error>(())
                }
            });
        }

        Ok(())
    }

    pub async fn run_archive_entries(&mut self) {
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
                        self.shutdown_notify.notify_waiters();
                    }
                }
            }
        }
    }
}
