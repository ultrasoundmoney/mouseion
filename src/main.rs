//! # Block Submission Archiver
//! Takes the many block submissions that the relay receives, over a message queue, bundles them
//! by slot, and stores them in cheap Object Storage.
//!
//! ## Architecture
//! The archiver is composed of three main components:
//! - A component which pulls in new messages and parses them.
//! - A component which claims pending messages and parses them.
//! - A component which takes parsed IdBlockSubmissionPairs and compresses their archive entry.
//! - A component which takes the compressed IdBlockSubmissionPairs and stores them in object storage.
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

use std::sync::Arc;

use anyhow::{Context, Result};
use block_submission_archiver::{
    env::{self, ENV_CONFIG},
    log, BlockSubmission, STREAM_NAME,
};
use bytes::Bytes;
use fred::{
    prelude::{ClientLike, RedisClient, StreamsInterface},
    types::RedisConfig,
};
use futures::{
    channel::mpsc::{self},
    SinkExt, Stream, StreamExt, TryStreamExt,
};
use health::{RedisConsumerHealth, RedisHealth};
use object_store::ObjectStore;
use performance::TimedExt;
use redis_consumer::IdBlockSubmissionPairs;
use tokio::{sync::Notify, try_join};
use tracing::{debug, error, info};

use crate::redis_consumer::RedisConsumer;

const GROUP_NAME: &str = "default-group";
const ARCHIVING_MAX_CONCURRENCY: usize = 16;

#[derive(Clone)]
pub struct AppState {
    redis_consumer_health: RedisConsumerHealth,
    redis_health: RedisHealth,
}

async fn store_block_submission(
    object_store: &impl ObjectStore,
    block_submission: &BlockSubmission,
    json_gz_bytes: Bytes,
) -> Result<()> {
    let store_result = object_store
        .put(&block_submission.bundle_path(), json_gz_bytes)
        .timed("store_block_submission")
        .await
        .context("failed to store block submission");

    match store_result {
        Ok(_) => anyhow::Ok(()),
        Err(e) => {
            // Sometimes archivers manage to grab and attempt to store the same message. As
            // the operations are idempotent, we simply skip 409s.
            if format!("{:?}", &e).contains("OperationAborted") {
                debug!(
                    state_root = block_submission.state_root(),
                    "hit 409 - conflict when storing bundle, ignoring"
                );
                Ok(())
            } else {
                Err(e.into())
            }
        }
    }?;

    Ok(())
}

async fn compress_and_store_submissions(
    object_store: &impl ObjectStore,
    redis_client: &RedisClient,
    submissions_stream: impl Stream<Item = IdBlockSubmissionPairs>,
) -> Result<()> {
    submissions_stream
        .map(Ok)
        .try_for_each_concurrent(
            ARCHIVING_MAX_CONCURRENCY,
            |(id, block_submission): IdBlockSubmissionPairs| async move {
                // Compress
                let json_gz_bytes = block_submission
                    .compress()
                    .timed("compress_block_submission")
                    .await?;

                // Store
                store_block_submission(object_store, &block_submission, json_gz_bytes).await?;

                // Acknowledge
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
    let redis_consumer_health = RedisConsumerHealth::new();

    let mut redis_consumer =
        RedisConsumer::new(redis_client.clone(), redis_consumer_health.clone());

    redis_consumer.ensure_consumer_group_exists().await?;

    redis_consumer.delete_dead_consumers().await?;

    const BLOCK_SUBMISSIONS_BUFFER_SIZE: usize = 32;

    let (mut submissions_tx, submissions_rx) =
        mpsc::channel::<IdBlockSubmissionPairs>(BLOCK_SUBMISSIONS_BUFFER_SIZE);

    let submissions_thread = tokio::spawn({
        let shutdown_notify = shutdown_notify.clone();
        async move {
            loop {
                let submissions = redis_consumer
                    // This method blocks when Redis has no new messages available. No need to worry
                    // about busy waiting.
                    .pull_id_block_submission_pairs()
                    .await
                    .context("failed to pull block submissions");
                match submissions {
                    Ok(submissions) => {
                        for message in submissions {
                            submissions_tx.send(message).await.unwrap();
                        }
                    }
                    Err(e) => {
                        error!("failed to pull block submissions: {:?}", e);
                        shutdown_notify.notify_waiters();
                        break;
                    }
                }
            }
        }
    });

    let archive_thread = tokio::spawn({
        let shutdown_notify = shutdown_notify.clone();
        async move {
            match compress_and_store_submissions(&object_store, &redis_client, submissions_rx).await
            {
                Ok(_) => {
                    info!("archiving thread finished successfully");
                }
                Err(e) => {
                    error!("failed to archive block submissions: {:?}", e);
                    shutdown_notify.notify_waiters();
                }
            }
        }
    });

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

    try_join!(archive_thread, server_thread, submissions_thread)?;

    Ok(())
}
