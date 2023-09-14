use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use lazy_static::lazy_static;

use crate::env::{Env, ENV_CONFIG};

use super::HealthCheck;

#[derive(Debug, Clone)]
pub struct RedisConsumerHealth {
    last_message_received: Arc<Mutex<Option<Instant>>>,
    started_on: Instant,
}

impl RedisConsumerHealth {
    pub fn new() -> Self {
        Self {
            last_message_received: Arc::new(Mutex::new(None)),
            started_on: Instant::now(),
        }
    }

    fn set_last_message_received(&self, instant: Instant) {
        self.last_message_received
            .lock()
            .expect("unable to lock last message received")
            .replace(instant);
    }

    pub fn set_last_message_received_now(&self) {
        self.set_last_message_received(Instant::now());
    }
}

lazy_static! {
    static ref MAX_SILENCE_DURATION: Duration = match ENV_CONFIG.env {
        Env::Dev | Env::Stag => Duration::from_secs(60),
        Env::Prod => Duration::from_secs(24),
    };
}

impl HealthCheck for RedisConsumerHealth {
    fn health_status(&self) -> (bool, String) {
        let now = Instant::now();
        let time_since_start = now - self.started_on;

        let last_message_received = self
            .last_message_received
            .lock()
            .expect("unable to lock last message received");
        let time_since_last_message = last_message_received.map(|instant| now - instant);

        match time_since_last_message {
            None => {
                if time_since_start > *MAX_SILENCE_DURATION {
                    (
                        false,
                        format!(
                            "unhealthy, started {} seconds ago, but no message seen",
                            MAX_SILENCE_DURATION.as_secs()
                        ),
                    )
                } else {
                    (
                        true,
                        format!(
                            "healthy, started {} seconds ago, waiting for first message until {}",
                            time_since_start.as_secs(),
                            MAX_SILENCE_DURATION.as_secs(),
                        ),
                    )
                }
            }
            Some(time_since_last_message) => {
                if time_since_last_message > *MAX_SILENCE_DURATION {
                    (
                        false,
                        format!(
                            "unhealthy, last message seen {} seconds ago",
                            time_since_last_message.as_secs()
                        ),
                    )
                } else {
                    (
                        true,
                        format!(
                            "healthy, last message seen {} seconds ago",
                            time_since_last_message.as_secs()
                        ),
                    )
                }
            }
        }
    }
}
