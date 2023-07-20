mod env;
mod health;

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::{
    self,
    consumer::pull::{self},
};
use axum::{routing::get, Router, Server};
use futures::{try_join, TryStreamExt};
use health::{MessageHealth, NatsHealth};
use serde_json::Value;
use tracing::{debug, info};

use crate::health::get_healthz;

#[derive(Debug, Clone)]
pub struct AppState {
    message_health: MessageHealth,
    nats_client: async_nats::Client,
    nats_health: NatsHealth,
}

async fn process_messages(state: AppState) -> Result<()> {
    let jetstream = jetstream::new(state.nats_client);

    let stream = jetstream.get_stream("payload-archive").await?;

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
                .map_err(|e| anyhow!(e))
                .context("trying to ack message")
        });

        // Get block_number and state_root from message payload JSON
        let payload = message.payload;
        let payload_json = serde_json::from_slice::<Value>(&payload)?;
        let block_number = payload_json["block_number"]
            .as_str()
            .ok_or_else(|| anyhow!("block_number missing from payload"))?
            .parse::<u64>()
            .context("failed to parse block_number string as u64")?;
        let state_root = payload_json["state_root"]
            .as_str()
            .ok_or_else(|| anyhow!("state_root missing from payload"))?;

        debug!(block_number, state_root, "acked payload");

        // Wait for ack to be completed before moving to next message.
        ack_handle.await.context("joining ack message thread")??;

        // Update last_message_received to now
        state.message_health.set_last_message_received_now();
    }

    Ok(())
}

async fn serve(state: AppState) -> Result<()> {
    let app = Router::new()
        .route("/healthz", get(get_healthz))
        .with_state(state);

    let port = env::get_env_var("PORT").unwrap_or_else(|| "3003".to_string());
    info!(port, "server listening");
    let socket_addr = format!("0.0.0.0:{port}").parse().unwrap();

    Server::bind(&socket_addr)
        .serve(app.into_make_service())
        .await
        .context("running server")
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("starting payload archiver");

    let last_message_received: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));

    let nats_uri = std::env::var("NATS_URI").unwrap_or_else(|_| "localhost:4222".to_string());
    let nats_client = async_nats::connect(nats_uri).await?;

    let nats_health = NatsHealth::new(nats_client.clone());
    let message_health = MessageHealth::new(last_message_received);

    let app_state = AppState {
        nats_client,
        nats_health,
        message_health,
    };

    let messages_thread = tokio::spawn(process_messages(app_state.clone()));

    let server_thread = tokio::spawn(serve(app_state));

    let (messages_thread_result, server_thread_result) =
        try_join!(messages_thread, server_thread).context("joining message and server threads")?;

    messages_thread_result?;
    server_thread_result?;

    info!("payload archiver exiting");

    Ok(())
}
