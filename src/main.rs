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
mod env;
mod health;
mod messages;
mod object_stores;
mod serve;
mod units;

use std::sync::Arc;

use anyhow::{Context, Result};
use bundle_aggregator::SlotBundle;
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    try_join,
};
use health::{MessageHealth, NatsHealth};
use tracing::info;

use crate::{
    bundle_aggregator::BundleAggregator,
    bundle_shipper::BundleShipper,
    messages::{AckablePayload, MessageConsumer},
};

#[derive(Debug, Clone)]
pub struct AppState {
    message_health: Arc<MessageHealth>,
    nats_health: NatsHealth,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("starting payload archiver");

    let env_config = &env::ENV_CONFIG;

    let nats_uri = std::env::var("NATS_URI").unwrap_or_else(|_| "localhost:4222".to_string());
    let nats_client = async_nats::connect(nats_uri)
        .await
        .context("connecting to NATS")?;

    let nats_health = NatsHealth::new(nats_client.clone());
    let message_health = Arc::new(MessageHealth::new());

    // Ackable payload queue
    type MessageQueue = (Sender<AckablePayload>, Receiver<AckablePayload>);
    let (ackable_payload_tx, ackable_payload_rx): MessageQueue = mpsc::channel(512);
    let message_consumer =
        MessageConsumer::new(message_health.clone(), nats_client, ackable_payload_tx).await;

    // Bundle queue
    type BundleQueue = (Sender<SlotBundle>, Receiver<SlotBundle>);
    let (bundle_tx, bundle_rx): BundleQueue = mpsc::channel(16);
    let bundle_aggregator = BundleAggregator::new(bundle_tx);

    let object_store = object_stores::build_env_based_store(env_config)?;
    let mut bundle_shipper = BundleShipper::new(bundle_rx, object_store);

    let app_state = AppState {
        nats_health,
        message_health,
    };

    let messages_thread = message_consumer.run();

    let bundle_aggregator_complete_bundle_check_thread =
        bundle_aggregator.run_complete_bundle_check();

    let bundle_aggregator_consume_bundles_thread =
        bundle_aggregator.run_consume_bundles(ackable_payload_rx);

    let server_thread = serve::serve(app_state);

    let bundle_shipper_thread = bundle_shipper.run();

    try_join!(
        messages_thread,
        server_thread,
        bundle_aggregator_complete_bundle_check_thread,
        bundle_aggregator_consume_bundles_thread,
        bundle_shipper_thread,
    )
    .context("joining message, server, and bundle_aggregator tasks, and object storage thread")?;

    info!("payload archiver exiting");

    Ok(())
}
