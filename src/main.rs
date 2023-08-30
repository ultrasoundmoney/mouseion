//! # Payload Archiver
//! Takes the many execution payloads that the relay receives, over a message queue, bundles them
//! by slot, and stores them in cheap Object Storage.
//!
//! ## Architecture
//! The archiver is composed of three main components:
//! - A component which pulls in new messages.
//! - A component which claims pending messages.
//! - A component which takes the received messages, compresses them, and stores them in object
//!   storage.
//!
//! The first two components push messages onto a channel. The third component pulls messages off.
//! The third channel should always be able to process pulled messages within
//! MAX_MESSAGE_PROCESS_DURATION_MS. If it doesn't another consumer will claim the message and
//! process it also. This is however no big deal, as the storage process is idempotent.
//!
//! TODO: the core program is done, there is a lovely amount of polishing which can still be done.
mod health;
mod message_archiver;
mod message_consumer;
mod object_stores;
mod performance;
mod server;
mod trace_memory;

use std::sync::Arc;

use anyhow::{Context, Result};
use fred::{
    prelude::{ClientLike, RedisClient},
    types::RedisConfig,
};
use health::{MessageConsumerHealth, RedisHealth};
use payload_archiver::{
    env::{self, ENV_CONFIG},
    log,
};
use tokio::{
    sync::{mpsc, Notify},
    try_join,
};
use tracing::{debug, info, Level};

use crate::{message_archiver::MessageArchiver, message_consumer::MessageConsumer};

const GROUP_NAME: &str = "default-group";
const MESSAGE_BATCH_SIZE: u64 = 16;

#[derive(Clone)]
pub struct AppState {
    message_health: MessageConsumerHealth,
    redis_health: RedisHealth,
}

#[tokio::main]
async fn main() -> Result<()> {
    log::init_with_env();

    info!("starting payload archiver");

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

    let env_config = &env::ENV_CONFIG;

    let object_store = object_stores::build_env_based_store(env_config)?;

    let config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    let redis_client = RedisClient::new(config, None, None);
    redis_client.connect();
    redis_client
        .wait_for_connect()
        .await
        .context("failed to connect to Redis")?;

    let redis_health = RedisHealth::new(redis_client.clone());
    let message_health = MessageConsumerHealth::new();

    let (message_tx, message_rx) = mpsc::channel(MESSAGE_BATCH_SIZE as usize);

    let message_consumer = Arc::new(MessageConsumer::new(
        redis_client.clone(),
        shutdown_notify.clone(),
        message_tx,
    ));

    debug!("deleting dead consumers");
    message_consumer.delete_dead_consumers().await?;

    let pull_new_messages_thread = tokio::spawn({
        let message_consumer = message_consumer.clone();
        async move { message_consumer.run_consume_new_messages().await }
    });

    let pull_pending_messages_thread = tokio::spawn({
        let message_consumer = message_consumer.clone();
        async move {
            message_consumer.run_consume_pending_messages().await;
        }
    });

    let mut message_archiver = MessageArchiver::new(
        redis_client.clone(),
        message_rx,
        message_health.clone(),
        object_store,
        shutdown_notify.clone(),
    );

    let process_messages_thread = tokio::spawn({
        async move {
            message_archiver.run_archive_messages().await;
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

    try_join!(
        pull_new_messages_thread,
        pull_pending_messages_thread,
        process_messages_thread,
        server_thread,
    )?;

    Ok(())
}
