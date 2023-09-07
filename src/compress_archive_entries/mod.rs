mod gzip_compressor;

use async_trait::async_trait;
use futures::channel::mpsc::{Receiver, Sender};

pub use gzip_compressor::GzipCompressor;
use object_store::path::Path;

use crate::pull_archive_entries::IdArchiveEntry;

pub struct IdArchiveEntryCompressed {
    pub bundle_path: Path,
    pub id: String,
    pub json_gz_bytes: bytes::Bytes,
}

#[async_trait]
pub trait CompressArchiveEntries {
    async fn compress_archive_entries(
        &self,
        archive_entries_rx: Receiver<IdArchiveEntry>,
        compressed_archive_entries_tx: Sender<IdArchiveEntryCompressed>,
    );
}
