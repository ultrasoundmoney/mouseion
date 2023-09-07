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
mod health;
mod object_stores;
mod performance;
mod redis_consumer;
mod server;
mod trace_memory;

use std::sync::Arc;

use anyhow::{Context, Result};
use block_submission_archiver::{
    env::{self, ENV_CONFIG},
    log, STREAM_NAME,
};
use fred::{
    prelude::{ClientLike, RedisClient, StreamsInterface},
    types::RedisConfig,
};
use futures::{StreamExt, TryStreamExt};
use health::{RedisConsumerHealth, RedisHealth};
use object_store::ObjectStore;
use tokio::{sync::Notify, try_join};
use tracing::{info, Level};

use crate::redis_consumer::RedisConsumer;

const GROUP_NAME: &str = "default-group";
const ARCHIVING_MAX_CONCURRENCY: usize = 8;

#[derive(Clone)]
pub struct AppState {
    message_health: RedisConsumerHealth,
    redis_health: RedisHealth,
}

async fn archive_block_submissions(
    message_consumer: &mut RedisConsumer,
    object_store: &impl ObjectStore,
    redis_client: &RedisClient,
) -> Result<()> {
    let id_block_submission_pairs = message_consumer.pull_id_block_submission_pairs().await?;

    let stream = futures::stream::iter(id_block_submission_pairs);
    stream
        .map(Ok)
        .try_for_each_concurrent(
            ARCHIVING_MAX_CONCURRENCY,
            |(id, block_submission)| async move {
                let json_gz_bytes = block_submission.compress().await?;

                object_store
                    .put(&block_submission.bundle_path(), json_gz_bytes)
                    .await?;

                redis_client.xack(STREAM_NAME, GROUP_NAME, &id).await?;

                Ok(())
            },
        )
        .await
}

#[tokio::main]
async fn main() -> Result<()> {
    log::init();

    info!("starting block submission archiver");

    let shutdown_notify = Arc::new(Notify::new());

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

    let mut message_consumer = RedisConsumer::new(redis_client.clone(), message_health.clone());

    message_consumer.ensure_consumer_group_exists().await?;

    message_consumer.delete_dead_consumers().await?;

    let archive_thread = tokio::spawn({
        let shutdown_notify = shutdown_notify.clone();
        async move {
            loop {
                match archive_block_submissions(&mut message_consumer, &object_store, &redis_client)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        info!("failed to archive messages: {:?}", e);
                        shutdown_notify.notify_waiters();
                        break;
                    }
                }
            }
        }
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

    try_join!(archive_thread, server_thread)?;

    Ok(())
}
