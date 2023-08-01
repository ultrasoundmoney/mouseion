use async_nats::jetstream::{
    self,
    consumer::pull::{self},
};
use eyre::{eyre, Context, Result};
use futures::TryStreamExt;
use serde_json::Value;
use tracing::{debug, info};

use crate::AppState;

const STREAM_NAME: &str = "payload-archive";

pub async fn process_messages(state: AppState) -> Result<()> {
    info!("starting message processing");

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

    let mut message_stream = consumer.messages().await?;
    while let Some(message) = message_stream.try_next().await? {
        let (message, acker) = message.split();

        // Acknowledge the message
        let ack_handle = tokio::spawn(async move {
            acker
                .ack()
                .await
                .map_err(|e| eyre!(e))
                .context("trying to ack message")
        });

        // Get block_number and state_root from message payload JSON
        let payload = message.payload;
        let payload_json = serde_json::from_slice::<Value>(&payload)?;
        let block_number = payload_json["block_number"]
            .as_str()
            .ok_or_else(|| eyre!("block_number missing from payload"))?
            .parse::<u64>()
            .context("failed to parse block_number string as u64")?;
        let state_root = payload_json["state_root"]
            .as_str()
            .ok_or_else(|| eyre!("state_root missing from payload"))?;

        debug!(block_number, state_root, "acked payload");

        // Wait for ack to be completed before moving to next message.
        ack_handle.await.context("joining ack message thread")??;

        // Update last_message_received to now
        state.message_health.set_last_message_received_now();
    }

    Ok(())
}