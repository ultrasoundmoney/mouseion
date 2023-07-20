use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use tracing::{debug, warn};

use crate::{
    env::{self, Env},
    AppState,
};

trait HealthCheckable {
    fn health_status(&self) -> (bool, String);
}

#[derive(Debug, Clone)]
pub struct MessageHealth {
    started_on: Instant,
    last_message_received: Arc<Mutex<Option<Instant>>>,
}

impl MessageHealth {
    pub fn new(last_message_received: Arc<Mutex<Option<Instant>>>) -> Self {
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
pub struct NatsHealth {
    nats: async_nats::Client,
}

impl NatsHealth {
    pub fn new(nats: async_nats::Client) -> Self {
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

pub async fn get_healthz(State(state): State<AppState>) -> impl IntoResponse {
    let (is_nats_healthy, nats_health_status) = state.nats_health.health_status();
    // We check but don't count message health. Messages should be continuously coming in on
    // production. Until we know whether that is reliable we don't want to block the health
    // check.
    let (_is_messages_healthy, messages_health_status) = state.message_health.health_status();

    let message = json!({ "nats": nats_health_status, "messages": messages_health_status });

    if is_nats_healthy {
        debug!("health check: ok");
        (StatusCode::OK, Json(message))
    } else {
        warn!("health check: {}", message);
        (StatusCode::SERVICE_UNAVAILABLE, Json(message))
    }
}
