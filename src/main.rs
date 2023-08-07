//! # Payload Archiver
//! Takes the many execution payloads that the relay receives, over a message queue, bundles them
//! by slot, and stores them in cheap Object Storage.
//!
//! ## Architecture
//! The archiver is composed of three main components:
//! - A message consumer, which consumes messages from the message queue, and puts them on the
//!   ackable payload queue.
//! - A bundle aggregator, takes ackable payloads, and aggregates them into bundles by slot.
//!   Complete bundles are put on the bundle queue.
//! - An object storage, which takes bundles from the bundle queue, and stores them in object
//!   storage.
mod bundle_aggregator;
mod bundle_shipper;
mod health;
mod message_consumer;
mod object_stores;
mod operation_constants;
mod performance;
mod server;

use std::sync::Arc;

use anyhow::{Context, Result};
use futures::channel::mpsc::{self};
use health::{MessageConsumerHealth, NatsHealth};
use payload_archiver::env;
use tokio::{sync::Notify, try_join};
use tracing::info;

use crate::{
    bundle_aggregator::BundleAggregator, bundle_shipper::BundleShipper,
    message_consumer::MessageConsumer,
};

// Maximum number of bundles that can be waiting for shipping. Set to limit memory usage and provide
// backpressure.
const MAX_BUNDLE_QUEUE_SIZE: usize = 4;

// Maximum number of ackable payloads that can be waiting for aggregation. Set to limit memory
// usage and provide backpressure.
const MAX_ACKABLE_PAYLOAD_QUEUE_SIZE: usize = 1024;

#[derive(Debug, Clone)]
pub struct AppState {
    message_health: Arc<MessageConsumerHealth>,
    nats_health: NatsHealth,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("starting payload archiver");

    let shutdown_notify = Arc::new(Notify::new());

    let env_config = &env::ENV_CONFIG;

    let nats_uri = std::env::var("NATS_URI").unwrap_or_else(|_| "localhost:4222".to_string());
    let nats_client = async_nats::connect(nats_uri)
        .await
        .context("connecting to NATS")?;

    let nats_health = NatsHealth::new(nats_client.clone());
    let message_health = Arc::new(MessageConsumerHealth::new());

    // Ackable payload queue
    let (ackable_payload_tx, ackable_payload_rx) = mpsc::channel(MAX_ACKABLE_PAYLOAD_QUEUE_SIZE);
    let message_consumer = MessageConsumer::new(
        message_health.clone(),
        nats_client,
        ackable_payload_tx,
        shutdown_notify.clone(),
    )
    .await;

    // Bundle queue
    let (bundle_tx, bundle_rx) = mpsc::channel(MAX_BUNDLE_QUEUE_SIZE);
    let bundle_aggregator = Arc::new(BundleAggregator::new(
        ackable_payload_rx,
        bundle_tx,
        shutdown_notify.clone(),
    ));
    let bundle_aggregator_clone = bundle_aggregator.clone();

    let object_store = object_stores::build_env_based_store(env_config)?;
    let bundle_shipper = BundleShipper::new(bundle_rx, object_store);

    let message_consumer_thread = tokio::spawn(async move { message_consumer.run().await });

    let bundle_aggregator_complete_bundle_check_thread = tokio::spawn(async move {
        bundle_aggregator.run_complete_bundle_check().await;
    });

    let bundle_aggregator_consume_ackable_payloads_thread = tokio::spawn(async move {
        bundle_aggregator_clone.run_consume_ackable_payloads().await;
    });

    let app_state = AppState {
        nats_health,
        message_health,
    };

    let server_thread = tokio::spawn({
        let shutdown_notify = shutdown_notify.clone();
        async move {
            server::serve(app_state, &shutdown_notify).await;
        }
    });

    let bundle_shipper_thread = tokio::spawn({
        let shutdown_notify = shutdown_notify.clone();
        async move {
            bundle_shipper.run(&shutdown_notify).await;
        }
    });

    try_join!(
        bundle_aggregator_complete_bundle_check_thread,
        bundle_aggregator_consume_ackable_payloads_thread,
        bundle_shipper_thread,
        message_consumer_thread,
        server_thread,
    )?;

    Ok(())
}
