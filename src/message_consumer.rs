use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::{
    self,
    consumer::pull::{self},
    message::Acker,
};
use futures::{channel::mpsc, SinkExt, TryStreamExt};
use payload_archiver::ArchivePayload;
use serde::{Deserialize, Serialize};
use tokio::{select, sync::Notify};
use tracing::{debug, error, info, trace};

use crate::{health::MessageConsumerHealth, operation_constants::MAX_ACK_PENDING};

pub const STREAM_NAME: &str = "payload-archive";
const CONSUMER_NAME: &str = "payload-archive-consumer";

#[derive(Debug, Deserialize, Serialize)]
struct Withdrawal {
    address: String,
    amount: String,
    index: String,
    validator_index: String,
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

    async fn consume_messages(&self) -> Result<()> {
        debug!("connecting to NATS");
        let nats_context = jetstream::new(self.nats_client.clone());
        let stream = nats_context.get_stream(STREAM_NAME).await?;

        let consumer = stream
            .get_or_create_consumer(
                CONSUMER_NAME,
                pull::Config {
                    durable_name: Some(CONSUMER_NAME.to_string()),
                    max_ack_pending: MAX_ACK_PENDING,
                    ..Default::default()
                },
            )
            .await?;

        info!("starting to process messages");

        let mut tx = self.archive_payload_tx.clone();
        let mut message_stream = consumer.messages().await?;

        while let Some(message) = message_stream.try_next().await? {
            trace!(message_size_kb = message.length / 1000, "received message");

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

            debug!(slot = %archive_payload.slot, size_kb = payload.len() / 1000, state_root, "queueing message for bundling");

            tx.send((acker, archive_payload)).await?;

            // Update last_message_received to now
            self.message_health.set_last_message_received_now();
        }

        Ok(())
    }

    pub async fn run(&self, shutdown_notify: &Notify) {
        select! {
            result = self.consume_messages() => {
                match result {
                    Ok(_) => info!("message consumer stopped"),
                    Err(e) => error!(%e, "message consumer exited with error"),
                }
                shutdown_notify.notify_waiters();
            }
            _ = shutdown_notify.notified() => {
                info!("message consumer shutting down");
            }
        }
    }
}
