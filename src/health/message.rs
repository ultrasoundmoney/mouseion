use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use lazy_static::lazy_static;

use crate::env::{self, Env};

use super::HealthCheck;

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

lazy_static! {
    static ref MAX_SILENCE_DURATION: Duration = match env::get_env() {
        Env::Dev => Duration::from_secs(60),
        Env::Stag => Duration::from_secs(60),
        Env::Prod => Duration::from_secs(24),
    };
}

impl HealthCheck for MessageHealth {
    fn health_status(&self) -> (bool, String) {
        let time_since_start = Instant::now() - self.started_on;

        // To avoid blocking the constant writes to this value we clone.
        let last_message_recieved_clone = *self
            .last_message_received
            .lock()
            .expect("expect to acquire lock on last_message_received in health check");
        let time_since_last_message =
            last_message_recieved_clone.map(|instant| Instant::now() - instant);

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
