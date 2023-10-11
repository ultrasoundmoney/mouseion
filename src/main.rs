//! # Mouseion
//! Takes the many block submissions that the relay receives, over a Redis stream, and stores them
//! in cheap Object Storage.
//!
//! ## Architecture
//! - A component which pulls in new messages and parses them.
//! - A component which claims pending messages and parses them.
//! - A component which takes parsed block submissions and compresses them.
//! - A component which stores the compressed block submissions in object storage.
//! - A component which acknowledges messages which have been successfully stored.
use std::sync::Arc;

use anyhow::{Context, Result};
use fred::{pool::RedisPool, types::RedisConfig};
use futures::channel::mpsc::{self};
use mouseion::{
    compression,
    env::ENV_CONFIG,
    health::{RedisConsumerHealth, RedisHealth},
    log, object_store, performance, redis_consumer, server,
};
use redis_consumer::IdBlockSubmission;
use tokio::{sync::Notify, try_join};
use tracing::{info, trace};

use crate::{compression::IdBlockSubmissionCompressed, performance::BlockCounter};

#[tokio::main]
async fn main() -> Result<()> {
    log::init();

    info!("starting block submission archiver");

    // When one of our threads panics, we want to shutdown the entire program. Most threads
    // communicate over channels, and so will naturally shut down as the channels close. However,
    // the server thread does not. We use this notify to shutdown the server thread when any other
    // thread panics.
    let shutdown_notify = Arc::new(Notify::new());

    // Track our block archival count.
    let block_counter = Arc::new(BlockCounter::new());
    let log_block_counter_thread = {
        if tracing::enabled!(tracing::Level::INFO) || ENV_CONFIG.log_perf {
            let handle = tokio::spawn({
                let block_counter = block_counter.clone();
                async move {
                    performance::report_archive_rate_periodically(&block_counter).await;
                }
            });
            let shutdown_notify = shutdown_notify.clone();
            tokio::spawn(async move {
                shutdown_notify.notified().await;
                trace!("shutting down block counter thread");
                handle.abort();
            })
        } else {
            // Return a future which immediately completes.
            trace!("not starting block counter thread");
            tokio::spawn(async {})
        }
    };

    // Set up the shared Redis pool.
    let config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    // We use a pool of 3 connections to make sure new, pending, and ack redis consumer components
    // can all run concurrently.
    let redis_pool = RedisPool::new(config, None, None, 3)?;
    redis_pool.connect();
    redis_pool
        .wait_for_connect()
        .await
        .context("failed to connect to redis")?;

    // Initialize health check components.
    let redis_health = RedisHealth::new(redis_pool.clone());
    let redis_consumer_health = RedisConsumerHealth::new();

    redis_consumer::ensure_consumer_group_exists(&redis_pool).await?;

    redis_consumer::delete_dead_consumers(&redis_pool).await?;

    const BLOCK_SUBMISSIONS_BUFFER_SIZE: usize = 32;
    let (submissions_tx, submissions_rx) =
        mpsc::channel::<IdBlockSubmission>(BLOCK_SUBMISSIONS_BUFFER_SIZE);

    let new_messages_thread = redis_consumer::run_new_submissions_thread(
        redis_pool.clone(),
        redis_consumer_health.clone(),
        shutdown_notify.clone(),
        submissions_tx.clone(),
    );

    let pending_messages_thread = redis_consumer::run_pending_submissions_thread(
        redis_pool.clone(),
        shutdown_notify.clone(),
        submissions_tx,
    );

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
        block_counter,
        compressed_submissions_rx,
        stored_submissions_tx,
        shutdown_notify.clone(),
    );

    // The ack thread takes stored submissions and acknowledges them.
    let ack_thread = redis_consumer::run_ack_submissions_thread(
        redis_pool,
        shutdown_notify.clone(),
        stored_submissions_rx,
    );

    let server_thread = tokio::spawn({
        let shutdown_notify = shutdown_notify.clone();
        async move {
            server::serve(redis_consumer_health, redis_health, &shutdown_notify).await;
        }
    });

    try_join!(
        ack_thread,
        compression_thread,
        log_block_counter_thread,
        new_messages_thread,
        pending_messages_thread,
        server_thread,
        storage_thread,
    )?;

    Ok(())
}
