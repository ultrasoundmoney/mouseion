use fred::{
    prelude::{ClientLike, RedisClient},
    types::ClientState,
};

use super::HealthCheck;

#[derive(Clone)]
pub struct RedisHealth {
    redis: RedisClient,
}

impl RedisHealth {
    pub fn new(redis: RedisClient) -> Self {
        Self { redis }
    }
}

impl HealthCheck for RedisHealth {
    fn health_status(&self) -> (bool, String) {
        match self.redis.state() {
            ClientState::Connected => (true, "healthy, connected".to_owned()),
            ClientState::Connecting => (false, "unhealthy, connecting".to_owned()),
            ClientState::Disconnecting => (false, "unhealthy, disconnecting".to_owned()),
            ClientState::Disconnected => (false, "unhealthy, disconnected".to_owned()),
        }
    }
}
