//! # Block Submission Archiver
//! Takes the many block submissions that the relay receives, over a message queue, bundles them
//! by slot, and stores them in cheap Object Storage.
//!
//! ## Architecture
//! The archiver is composed of three main components:
//! - A component which pulls in new messages and parses them.
//! - A component which claims pending messages and parses them.
//! - A component which takes parsed IdArchiveEntryPairs and compresses their archive entry.
//! - A component which takes the compressed IdArchiveEntryPairs and stores them in object storage.
//! - A component which acknowledges messages which have been successfully stored.
//!
//! The full process should always be able to complete pulled messages within
//! MAX_MESSAGE_PROCESS_DURATION_MS. If it doesn't another consumer will claim the message and
//! process it also. This is however no big deal, as the storage process is idempotent.
mod compress_archive_entries;
mod health;
mod object_stores;
mod performance;
mod pull_archive_entries;
mod server;
mod store_archive_entries;
mod trace_memory;

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use block_submission_archiver::{
    env::{self, ENV_CONFIG},
    log,
};
use bytes::Bytes;
use fred::{
    prelude::{ClientLike, RedisClient},
    types::RedisConfig,
};
use futures::channel::mpsc::{self, channel};
use health::{RedisConsumerHealth, RedisHealth};
use object_store::path::Path;
use tokio::{sync::Notify, try_join};
use tracing::{info, Level};

use crate::{
    compress_archive_entries::{CompressArchiveEntries, GzipCompressor},
    pull_archive_entries::PullArchiveEntries,
};
use crate::{
    pull_archive_entries::RedisConsumer,
    store_archive_entries::{ArchiveEntriesObjectStore, StoreArchiveEntries},
};

const GROUP_NAME: &str = "default-group";
const MESSAGE_BATCH_SIZE: u64 = 8;
const ARCHIVE_ENTRIES_BUFFER_SIZE: u64 = MESSAGE_BATCH_SIZE * 4;

#[derive(Clone)]
pub struct AppState {
    message_health: RedisConsumerHealth,
    redis_health: RedisHealth,
}
#[async_trait]
trait StoreArchiveEntry {
    async fn store_entry(&self, path: &Path, bytes: &Bytes);
}

#[tokio::main]
async fn main() -> Result<()> {
    log::init();

    info!("starting block submission archiver");

    let shutdown_notify = Arc::new(Notify::new());

    if tracing::enabled!(Level::TRACE) {
        let handle = tokio::spawn(async move {
            trace_memory::report_memory_periodically().await;
        });
        let shutdown_notify = shutdown_notify.clone();
        tokio::spawn(async move {
            shutdown_notify.notified().await;
            handle.abort();
        });
    }

    let object_store = object_stores::build_env_based_store()?;

    let config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    let redis_client = RedisClient::new(config, None, None);
    redis_client.connect();
    redis_client
        .wait_for_connect()
        .await
        .context("failed to connect to Redis")?;

    let redis_health = RedisHealth::new(redis_client.clone());
    let message_health = RedisConsumerHealth::new();

    let (archive_entries_tx, archive_entries_rx) =
        mpsc::channel(ARCHIVE_ENTRIES_BUFFER_SIZE as usize);

    let (stored_ids_tx, stored_ids_rx) = channel::<String>(ARCHIVE_ENTRIES_BUFFER_SIZE as usize);

    let message_consumer = Arc::new(RedisConsumer::new(
        redis_client.clone(),
        message_health.clone(),
        shutdown_notify.clone(),
    ));

    message_consumer.ensure_consumer_group_exists().await?;

    message_consumer.delete_dead_consumers().await?;

    let pull_archive_entries_thread = tokio::spawn(async move {
        message_consumer
            .pull_archive_entries(archive_entries_tx, stored_ids_rx)
            .await
    });

    let (compressed_archive_entries_tx, compressed_archive_entries_rx) =
        channel(ARCHIVE_ENTRIES_BUFFER_SIZE as usize);

    let compressor = GzipCompressor::new(shutdown_notify.clone());

    let compress_archive_entries_thread = tokio::spawn(async move {
        compressor
            .compress_archive_entries(archive_entries_rx, compressed_archive_entries_tx)
            .await
    });

    let compressed_archive_entries_store =
        ArchiveEntriesObjectStore::new(object_store, shutdown_notify.clone());

    let store_entries_thread = tokio::spawn(async move {
        compressed_archive_entries_store
            .store_archive_entries(compressed_archive_entries_rx, stored_ids_tx)
            .await
    });

    let app_state = AppState {
        redis_health,
        message_health,
    };

    let server_thread = tokio::spawn({
        let shutdown_notify = shutdown_notify.clone();
        async move {
            server::serve(app_state, &shutdown_notify).await;
        }
    });

    try_join!(
        pull_archive_entries_thread,
        compress_archive_entries_thread,
        store_entries_thread,
        server_thread,
    )?;

    Ok(())
}
