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
    nats_health: NatsHealth,
    message_health: MessageHealth,
}

trait HealthCheckable {
    fn health_status(&self) -> (bool, String);
}

#[derive(Debug, Clone)]
struct MessageHealth {
    started_on: Instant,
    last_message_received: Arc<Mutex<Option<Instant>>>,
}

impl MessageHealth {
    fn new(last_message_received: Arc<Mutex<Option<Instant>>>) -> Self {
        Self {
            started_on: Instant::now(),
            last_message_received,
        }
    }

    fn set_last_message_received(&self, instant: Instant) {
        self.last_message_received.lock().unwrap().replace(instant);
    }

    pub fn set_last_message_received_now(&self) {
        self.set_last_message_received(Instant::now());
    }
}

impl HealthCheckable for MessageHealth {
    fn health_status(&self) -> (bool, String) {
        let time_since_start = Instant::now() - self.started_on;

        // To avoid blocking the constant writes to this value we clone.
        let last_message_recieved_clone = *self
            .last_message_received
            .lock()
            .expect("expect to acquire lock on last_message_received in health check");
        let time_since_last_message =
            last_message_recieved_clone.map(|instant| Instant::now() - instant);
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
            (true, "healthy".to_string())
        } else {
            (
                false,
                format!(
                    "no message seen for more than {} seconds",
                    max_silence_duration.as_secs()
                ),
            )
        }
    }
}

#[derive(Debug, Clone)]
struct NatsHealth {
    nats: async_nats::Client,
}

impl NatsHealth {
    fn new(nats: async_nats::Client) -> Self {
        Self { nats }
    }
}

impl HealthCheckable for NatsHealth {
    fn health_status(&self) -> (bool, String) {
        use async_nats::connection::State;
        match self.nats.connection_state() {
            State::Connected => (true, "connected".to_string()),
            State::Disconnected => (false, "disconnected".to_string()),
            State::Pending => (false, "reconnecting".to_string()),
        }
    }
}

async fn get_healthz(State(state): State<AppState>) -> impl IntoResponse {
    let (is_nats_healthy, nats_health_status) = state.nats_health.health_status();

    if is_nats_healthy {
        (
            StatusCode::OK,
            Json(json!({ "message_queue": nats_health_status })),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "message_queue": nats_health_status })),
        )
    }
}

async fn process_messages(client: async_nats::Client, message_health: MessageHealth) -> Result<()> {
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
        message_health.set_last_message_received_now();
    }

    Ok(())
}

async fn serve(message_health: MessageHealth, nats_health: NatsHealth) -> Result<()> {
    let state = AppState {
        message_health,
        nats_health,
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

    let last_message_received: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));

    let nats_uri = std::env::var("NATS_URI").unwrap_or_else(|_| "localhost:4222".to_string());
    let client = async_nats::connect(nats_uri).await?;

    let nats_health = NatsHealth::new(client.clone());
    let message_health = MessageHealth::new(last_message_received);

    let messages_thread = tokio::spawn(process_messages(client.clone(), message_health.clone()));

    let server_thread = tokio::spawn(async move { serve(message_health, nats_health).await });

    let (messages_thread_result, server_thread_result) =
        try_join!(messages_thread, server_thread).context("joining message and server threads")?;

    messages_thread_result?;
    server_thread_result?;

    Ok(())
}
