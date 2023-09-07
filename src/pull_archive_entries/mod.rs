mod redis_consumer;

use std::sync::Arc;

use async_trait::async_trait;
use block_submission_archiver::BlockSubmission;
use futures::channel::mpsc::{Receiver, Sender};

pub use redis_consumer::RedisConsumer;

/// Pair of an block submission and its message ID.
#[derive(Debug)]
pub struct IdArchiveEntry {
    /// Used to acknowledge the entry has been archived.
    pub id: String,
    pub entry: BlockSubmission,
}

// impl IdArchiveEntry {
//     pub async fn ack(&self, client: &RedisClient) -> Result<()> {
//         client.xack(STREAM_NAME, GROUP_NAME, &self.id).await?;
//         Ok(())
//     }
// }

#[async_trait]
pub trait PullArchiveEntries {
    async fn pull_archive_entries(
        self: Arc<Self>,
        archive_entries_tx: Sender<IdArchiveEntry>,
        stored_archive_entries_rx: Receiver<String>,
    );
}
