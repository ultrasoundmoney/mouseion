mod compressed_entries_store;

pub use compressed_entries_store::ArchiveEntriesObjectStore;

use async_trait::async_trait;
use futures::channel::mpsc::{Receiver, Sender};

use crate::compress_archive_entries::IdArchiveEntryCompressed;

#[async_trait]
pub trait StoreArchiveEntries {
    async fn store_archive_entries(
        &self,
        compressed_archive_entries_rx: Receiver<IdArchiveEntryCompressed>,
        stored_archive_entries_tx: Sender<String>,
    );
}
