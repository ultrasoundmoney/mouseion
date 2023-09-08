mod redis;
mod redis_consumer;

pub use redis::RedisHealth;
pub use redis_consumer::RedisConsumerHealth;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use tracing::{debug, warn};

use crate::AppState;

trait HealthCheck {
    fn health_status(&self) -> (bool, String);
}

pub async fn get_livez(State(state): State<AppState>) -> impl IntoResponse {
    let (is_redis_healthy, redis_health_status) = state.redis_health.health_status();
    // We check but don't count message health. Messages should be continuously coming in on
    // production. Until we know whether that is reliable we don't want to fail the health
    // check.
    let (_is_messages_healthy, messages_health_status) =
        state.redis_consumer_health.health_status();

    let message = json!({ "redis": redis_health_status, "messages": messages_health_status });

    if is_redis_healthy {
        debug!(
            redis = redis_health_status,
            messages = messages_health_status
        );
        (StatusCode::OK, Json(message))
    } else {
        warn!(
            redis = redis_health_status,
            messages = messages_health_status
        );
        (StatusCode::SERVICE_UNAVAILABLE, Json(message))
    }
}
