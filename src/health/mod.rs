mod redis;
mod redis_consumer;

pub use redis::RedisHealth;
pub use redis_consumer::RedisConsumerHealth;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use tracing::{debug, warn};

use crate::{
    env::{Env, ENV_CONFIG},
    server::AppState,
};

trait HealthCheck {
    fn health_status(&self) -> (bool, String);
}

pub async fn get_livez(State(state): State<AppState>) -> impl IntoResponse {
    let (is_redis_healthy, redis_health_status) = state.redis_health.health_status();
    let (is_messages_healthy, messages_health_status) = state.redis_consumer_health.health_status();

    let message = json!({ "redis": redis_health_status, "messages": messages_health_status });

    // Until the turbo-relay running on staging archives submissions we don't expect any messages
    // on staging. Although we'd still like to report the archiver is in an unhealthy state, we
    // don't want k8s to consider us unhealthy and restart. Therefore we ignore message health.
    let is_messages_healthy = {
        debug!("env is staging, ignoring message health in health status");
        ENV_CONFIG.env == Env::Stag || is_messages_healthy
    };

    if is_redis_healthy && is_messages_healthy {
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
