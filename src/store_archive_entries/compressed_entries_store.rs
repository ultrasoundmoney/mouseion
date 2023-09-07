use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{
    channel::mpsc::{Receiver, Sender},
    SinkExt, StreamExt, TryStreamExt,
};
use lazy_static::lazy_static;
use object_store::ObjectStore;
use tokio::sync::Notify;
use tracing::{error, info};

use crate::compress_archive_entries::IdArchiveEntryCompressed;

use super::StoreArchiveEntries;

const MAX_CONCURRENCY: usize = 2;

lazy_static! {
    static ref MAX_RETRY_DURATION_MS: Duration = Duration::from_secs(8);
}

pub struct ArchiveEntriesObjectStore<OS: ObjectStore> {
    object_store: Arc<OS>,
    shutdown_notify: Arc<Notify>,
}

impl<OS: ObjectStore> ArchiveEntriesObjectStore<OS> {
    pub fn new(object_store: OS, shutdown_notify: Arc<Notify>) -> Self {
        Self {
            object_store: Arc::new(object_store),
            shutdown_notify,
        }
    }
}

#[async_trait]
impl<OS: ObjectStore> StoreArchiveEntries for ArchiveEntriesObjectStore<OS> {
    async fn store_archive_entries(
        &self,
        compressed_archive_entries_rx: Receiver<IdArchiveEntryCompressed>,
        stored_archive_entries_tx: Sender<String>,
    ) {
        let result = compressed_archive_entries_rx
            .map(Ok)
            .try_for_each_concurrent(MAX_CONCURRENCY, |id_archive_entry_compressed| {
                let mut stored_archive_entries_tx = stored_archive_entries_tx.clone();
                async move {
                    self.object_store
                        .put(
                            &id_archive_entry_compressed.bundle_path,
                            id_archive_entry_compressed.json_gz_bytes,
                        )
                        .await?;

                    stored_archive_entries_tx
                        .send(id_archive_entry_compressed.id)
                        .await?;

                    anyhow::Ok(())
                }
            })
            .await;

        match result {
            Ok(_) => info!("finished storing archive entries"),
            Err(e) => {
                error!("failed to store archive entries: {:?}", e);
                self.shutdown_notify.notify_waiters();
            }
        }
    }
}
