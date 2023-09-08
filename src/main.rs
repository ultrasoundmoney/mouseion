//! # Block Submission Archiver
//! Takes the many block submissions that the relay receives, over a message queue, bundles them
//! by slot, and stores them in cheap Object Storage.
//!
//! ## Architecture
//! The archiver is composed of three main components:
//! - A component which pulls in new messages and parses them.
//! - A component which claims pending messages and parses them.
//! - A component which takes parsed IdBlockSubmissionPairs and compresses the block submission.
//! - A component which takes the compressed IdBlockSubmissionPairs and stores them in object storage.
//! - A component which acknowledges messages which have been successfully stored.
//!
//! The full process should always be able to complete pulled messages within
//! MAX_MESSAGE_PROCESS_DURATION_MS. If it doesn't another consumer will claim the message and
//! process it also. This is however no big deal, as the storage process is idempotent.
mod compression;
mod health;
mod object_store;
mod performance;
mod redis_consumer;
mod server;

use std::sync::Arc;

use anyhow::{Context, Result};
use block_submission_archiver::{
    env::{self, ENV_CONFIG},
    log, BlockSubmission,
};
use fred::{
    prelude::{ClientLike, RedisClient},
    types::RedisConfig,
};
use futures::channel::mpsc::{self};
use health::{RedisConsumerHealth, RedisHealth};
use redis_consumer::IdBlockSubmission;
use tokio::{sync::Notify, try_join};
use tracing::info;

use crate::{compression::IdBlockSubmissionCompressed, redis_consumer::RedisConsumer};

const GROUP_NAME: &str = "default-group";

#[derive(Clone)]
pub struct AppState {
    redis_consumer_health: RedisConsumerHealth,
    redis_health: RedisHealth,
}

#[tokio::main]
async fn main() -> Result<()> {
    log::init();

    info!("starting block submission archiver");

    // When one of our threads panics, we want to shutdown the entire program. Most threads
    // communicate over channels, and so will naturally shut down as the channels close. However,
    // the server thread does not. We use this notify to shutdown the server thread when any other
    // thread panics.
    let shutdown_notify = Arc::new(Notify::new());

    // Set up the shared Redis client.
    let config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    let redis_client = RedisClient::new(config, None, None);
    redis_client.connect();
    redis_client
        .wait_for_connect()
        .await
        .context("failed to connect to Redis")?;

    // Initialize health check components.
    let redis_health = RedisHealth::new(redis_client.clone());
    let redis_consumer_health = RedisConsumerHealth::new();

    // The Redis consumer is responsible for pulling in new messages and claiming pending messages.
    // It also acknowledges messages which have been successfully stored.
    let redis_consumer = Arc::new(RedisConsumer::new(
        redis_client.clone(),
        redis_consumer_health.clone(),
        shutdown_notify.clone(),
    ));

    redis_consumer.ensure_consumer_group_exists().await?;

    redis_consumer.delete_dead_consumers().await?;

    const BLOCK_SUBMISSIONS_BUFFER_SIZE: usize = 32;
    let (submissions_tx, submissions_rx) =
        mpsc::channel::<IdBlockSubmission>(BLOCK_SUBMISSIONS_BUFFER_SIZE);

    let new_messages_thread = redis_consumer
        .clone()
        .run_new_submissions_thread(submissions_tx.clone());

    let pending_messages_thread = redis_consumer
        .clone()
        .run_pending_submissions_thread(submissions_tx);

    let (compressed_submissions_tx, compressed_submissions_rx) =
        mpsc::channel::<IdBlockSubmissionCompressed>(BLOCK_SUBMISSIONS_BUFFER_SIZE);

    // The compression thread takes new submissions and compresses them.
    let compression_thread = compression::run_compression_thread(
        submissions_rx,
        compressed_submissions_tx,
        shutdown_notify.clone(),
    );

    let (stored_submissions_tx, stored_submissions_rx) =
        mpsc::channel::<String>(BLOCK_SUBMISSIONS_BUFFER_SIZE);

    // The storage thread takes compressed submissions and stores them in object storage.
    let storage_thread = object_store::run_store_submissions_thread(
        compressed_submissions_rx,
        stored_submissions_tx,
        shutdown_notify.clone(),
    );

    // The ack thread takes stored submissions and acknowledges them.
    let ack_thread = redis_consumer.run_ack_submissions_thread(stored_submissions_rx);

    let app_state = AppState {
        redis_health,
        redis_consumer_health,
    };

    let server_thread = tokio::spawn({
        let shutdown_notify = shutdown_notify.clone();
        async move {
            server::serve(app_state, &shutdown_notify).await;
        }
    });

    try_join!(
        ack_thread,
        compression_thread,
        new_messages_thread,
        pending_messages_thread,
        server_thread,
        storage_thread,
    )?;

    Ok(())
}
