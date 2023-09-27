//! Module which helps us run our integration test

use std::sync::Arc;

use anyhow::{Context, Result};
use fred::{pool::RedisPool, types::RedisConfig};
use futures::channel::mpsc::channel;
use tokio::sync::Notify;

use crate::{
    compression::{self, IdBlockSubmissionCompressed},
    env::ENV_CONFIG,
    health, object_store,
    performance::BlockCounter,
    redis_consumer::{self, IdBlockSubmission},
};

pub async fn run_all() -> Result<()> {
    let shutdown_notify = Arc::new(Notify::new());

    // Track our block archival count.
    let block_counter = Arc::new(BlockCounter::new());

    let config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    let redis_pool = RedisPool::new(config, None, None, 4)?;
    redis_pool.connect();
    redis_pool
        .wait_for_connect()
        .await
        .context("failed to connect to redis")?;
    let redis_consumer_health = health::RedisConsumerHealth::new();

    redis_consumer::ensure_consumer_group_exists(&redis_pool).await?;

    redis_consumer::delete_dead_consumers(&redis_pool).await?;

    const BLOCK_SUBMISSIONS_BUFFER_SIZE: usize = 32;
    let (submissions_tx, submissions_rx) =
        channel::<IdBlockSubmission>(BLOCK_SUBMISSIONS_BUFFER_SIZE);

    redis_consumer::run_new_submissions_thread(
        redis_pool.clone(),
        redis_consumer_health.clone(),
        shutdown_notify.clone(),
        submissions_tx.clone(),
    );

    redis_consumer::run_pending_submissions_thread(
        redis_pool.clone(),
        shutdown_notify.clone(),
        submissions_tx,
    );

    let (compressed_submissions_tx, compressed_submissions_rx) =
        channel::<IdBlockSubmissionCompressed>(BLOCK_SUBMISSIONS_BUFFER_SIZE);

    // The compression thread takes new submissions and compresses them.
    compression::run_compression_thread(
        submissions_rx,
        compressed_submissions_tx,
        shutdown_notify.clone(),
    );

    let (stored_submissions_tx, stored_submissions_rx) =
        channel::<String>(BLOCK_SUBMISSIONS_BUFFER_SIZE);

    // The storage thread takes compressed submissions and stores them in object storage.
    object_store::run_store_submissions_thread(
        block_counter,
        compressed_submissions_rx,
        stored_submissions_tx,
        shutdown_notify.clone(),
    );

    // The ack thread takes stored submissions and acknowledges them.
    redis_consumer::run_ack_submissions_thread(
        redis_pool,
        shutdown_notify.clone(),
        stored_submissions_rx,
    );

    Ok(())
}
