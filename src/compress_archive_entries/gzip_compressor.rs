use std::sync::Arc;

use async_trait::async_trait;
use futures::{
    channel::mpsc::{Receiver, Sender},
    SinkExt, StreamExt, TryStreamExt,
};
use tokio::sync::Notify;
use tracing::{error, info};

use crate::pull_archive_entries::IdArchiveEntry;

use super::{CompressArchiveEntries, IdArchiveEntryCompressed};

const MAX_CONCURRENCY: usize = 4;

pub struct GzipCompressor {
    shutdown_notify: Arc<Notify>,
}

impl GzipCompressor {
    pub fn new(shutdown_notify: Arc<Notify>) -> Self {
        Self { shutdown_notify }
    }
}

#[async_trait]
impl CompressArchiveEntries for GzipCompressor {
    async fn compress_archive_entries(
        &self,
        archive_entries_rx: Receiver<IdArchiveEntry>,
        compressed_archive_entries_tx: Sender<IdArchiveEntryCompressed>,
    ) {
        let result = archive_entries_rx
            .map(Ok)
            .try_for_each_concurrent(MAX_CONCURRENCY, |id_archive_entry| {
                let mut compressed_archive_entries_tx = compressed_archive_entries_tx.clone();
                async move {
                    let json_gz_bytes = id_archive_entry.entry.compress().await?;

                    compressed_archive_entries_tx
                        .send(IdArchiveEntryCompressed {
                            bundle_path: id_archive_entry.entry.bundle_path(),
                            id: id_archive_entry.id,
                            json_gz_bytes,
                        })
                        .await?;

                    anyhow::Ok(())
                }
            })
            .await;

        match result {
            Ok(_) => info!("finished compressing archive entries"),
            Err(e) => {
                error!("failed to compress archive entries: {:?}", e);
                self.shutdown_notify.notify_waiters();
            }
        }
    }
}
