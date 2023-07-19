mod env;

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::{
    self,
    consumer::pull::{self},
};
use axum::{
    extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router, Server,
};
use futures::{try_join, TryStreamExt};
use serde_json::{json, Value};
use tracing::{debug, info};

use crate::env::Env;

#[derive(Debug, Clone)]
struct AppState {
    last_message_received: Arc<Mutex<Option<Instant>>>,
    started_on: Instant,
}

async fn get_healthz(State(state): State<AppState>) -> impl IntoResponse {
    let time_since_start = Instant::now() - state.started_on;
    let last_message_recieved_inner_clone = *state.last_message_received.lock().unwrap();
    let time_since_last_message =
        last_message_recieved_inner_clone.map(|instant| Instant::now() - instant);
    let max_silence_duration = match env::get_env() {
        Env::Dev => Duration::from_secs(60),
        Env::Stag => Duration::from_secs(60),
        Env::Prod => Duration::from_secs(24),
    };

    let is_healthy = match time_since_last_message {
        None => time_since_start <= max_silence_duration,
        Some(time_since_last_message) => time_since_last_message <= max_silence_duration,
    };

    if is_healthy {
        (
            StatusCode::OK,
            Json(json!({
                "message_queue": "healthy"
            })),
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "message_queue":
                    format!(
                        "no message seen for more than {} seconds",
                        max_silence_duration.as_secs()
                    )
            })),
        )
            .into_response()
    }
}

async fn process_messages(last_message_received: Arc<Mutex<Option<Instant>>>) -> Result<()> {
    let nats_uri = std::env::var("NATS_URI").unwrap_or_else(|_| "localhost:4222".to_string());

    let client = async_nats::connect(nats_uri).await?;

    let jetstream = jetstream::new(client);

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
        last_message_received
            .lock()
            .unwrap()
            .replace(Instant::now());
    }

    Ok(())
}

async fn serve(
    started_on: Instant,
    last_message_received: Arc<Mutex<Option<Instant>>>,
) -> Result<()> {
    let state: AppState = AppState {
        started_on,
        last_message_received,
    };

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

    let started_on = Instant::now();
    let last_message_received: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let last_message_received_serve = last_message_received.clone();

    let server_thread =
        tokio::spawn(async move { serve(started_on, last_message_received_serve).await });

    let messages_thread = tokio::spawn(process_messages(last_message_received.clone()));

    let (messages_thread_result, server_thread_result) =
        try_join!(messages_thread, server_thread).context("joining message and server threads")?;

    messages_thread_result?;
    server_thread_result?;

    Ok(())
}
