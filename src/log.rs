use tracing_subscriber::EnvFilter;

use crate::env::{Env, ENV_CONFIG};

pub fn init() {
    if ENV_CONFIG.env == Env::Dev {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .json()
            .init();
    }
}
