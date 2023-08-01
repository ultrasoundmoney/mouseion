mod message;
mod nats;

pub use message::MessageHealth;
pub use nats::NatsHealth;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use tracing::{debug, warn};

use crate::AppState;

trait HealthCheck {
    fn health_status(&self) -> (bool, String);
}

pub async fn get_healthz(State(state): State<AppState>) -> impl IntoResponse {
    let (is_nats_healthy, nats_health_status) = state.nats_health.health_status();
    // We check but don't count message health. Messages should be continuously coming in on
    // production. Until we know whether that is reliable we don't want to fail the health
    // check.
    let (_is_messages_healthy, messages_health_status) = state.message_health.health_status();

    let message = json!({ "nats": nats_health_status, "messages": messages_health_status });

    if is_nats_healthy {
        debug!("health check: {}", message);
        (StatusCode::OK, Json(message))
    } else {
        warn!("health check: {}", message);
        (StatusCode::SERVICE_UNAVAILABLE, Json(message))
    }
}
