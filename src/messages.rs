use anyhow::{anyhow, Result};
use async_nats::jetstream::{
    self,
    consumer::pull::{self},
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, trace};

use crate::{bundle_aggregator::BundleAggregator, units::Slot, AppState};

type JsonValue = serde_json::Value;

pub const STREAM_NAME: &str = "payload-archive";

#[derive(Debug, Deserialize, Serialize)]
struct Withdrawal {
    address: String,
    amount: String,
    index: String,
    validator_index: String,
}

// Archive messages carry a slot number, and a execution payload. The slot number is used to make
// storage simple. Bundle messages with the same slot number together. The execution payload is
// what we receive from builders. This also means we do not handle re-orgs, but store all data.
#[derive(Debug, Serialize, Deserialize)]
struct ArchivePayload {
    // To avoid unneccesary work we do not deserialize the payload.
    payload: JsonValue,
    slot: Slot,
}

pub async fn process_messages(state: AppState, bundle_aggregator: &BundleAggregator) -> Result<()> {
    debug!("connecting to NATS");
    let nats_context = jetstream::new(state.nats_client);
    let stream = nats_context.get_stream(STREAM_NAME).await?;

    let consumer = stream
        .get_or_create_consumer(
            "ack-all-consumer",
            pull::Config {
                durable_name: Some("ack-all-consumer".to_string()),
                ..Default::default()
            },
        )
        .await?;

    info!("starting to process messages");

    let mut message_stream = consumer.messages().await?;
    while let Some(message) = message_stream.try_next().await? {
        trace!(?message, "received message");
        let (message, acker) = message.split();

        // Get block_number and state_root from message payload JSON
        let payload = message.payload;
        let archive_payload = serde_json::from_slice::<ArchivePayload>(&payload)?;

        let state_root = archive_payload
            .payload
            .get("state_root")
            .ok_or_else(|| anyhow!("state_root not found in payload"))?
            .as_str()
            .ok_or_else(|| anyhow!("state_root is not a string"))?;

        debug!(
            slot = %archive_payload.slot,
            state_root, "received new execution payload"
        );

        bundle_aggregator
            .add_execution_payload(archive_payload.slot, acker, archive_payload.payload)
            .await?;

        // Update last_message_received to now
        state.message_health.set_last_message_received_now();
    }

    Ok(())
}
