use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::{
    self,
    consumer::pull::{self},
    message::Acker,
};
use futures::{channel::mpsc, SinkExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, trace};

use crate::{health::MessageConsumerHealth, units::Slot};

type JsonValue = serde_json::Value;

pub const STREAM_NAME: &str = "payload-archive";
const CONSUMER_NAME: &str = "payload-archive-consumer";

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
pub struct ArchivePayload {
    // To avoid unneccesary work we do not deserialize the payload.
    /// This is the execution payload we'd like to store.
    pub payload: JsonValue,
    pub slot: Slot,
}

pub type AckablePayload = (Acker, ArchivePayload);

pub struct MessageConsumer {
    message_health: Arc<MessageConsumerHealth>,
    nats_client: async_nats::Client,
    archive_payload_tx: mpsc::Sender<AckablePayload>,
}

impl MessageConsumer {
    pub async fn new(
        message_health: Arc<MessageConsumerHealth>,
        nats_client: async_nats::Client,
        archive_payload_tx: mpsc::Sender<AckablePayload>,
    ) -> Self {
        Self {
            message_health,
            nats_client,
            archive_payload_tx,
        }
    }

    pub async fn run(&self) -> Result<()> {
        debug!("connecting to NATS");
        let nats_context = jetstream::new(self.nats_client.clone());
        let stream = nats_context.get_stream(STREAM_NAME).await?;

        let consumer = stream
            .get_or_create_consumer(
                CONSUMER_NAME,
                pull::Config {
                    durable_name: Some(CONSUMER_NAME.to_string()),
                    ..Default::default()
                },
            )
            .await?;

        info!("starting to process messages");

        let mut tx = self.archive_payload_tx.clone();
        let mut message_stream = consumer.messages().await?;

        while let Some(message) = message_stream.try_next().await? {
            trace!(?message, "received message");
            let (message, acker) = message.split();

            // Get block_number and state_root from message payload JSON
            let payload = message.payload;
            let archive_payload = serde_json::from_slice::<ArchivePayload>(&payload)
                .context("parsing message payload")?;

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

            tx.send((acker, archive_payload)).await?;

            // Update last_message_received to now
            self.message_health.set_last_message_received_now();
        }

        Ok(())
    }
}
