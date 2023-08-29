use lazy_static::lazy_static;
use tracing::Subscriber;
use tracing_subscriber::{fmt, EnvFilter};

use crate::env::{self, Env, ENV_CONFIG};

lazy_static! {
    static ref PRETTY_PRINT: bool = env::get_env_bool("PRETTY_PRINT");
}

pub fn init_with_env() {
    let env_filter = EnvFilter::from_default_env();

    let subscriber: Box<dyn Subscriber + Send + Sync> = if (*ENV_CONFIG).pretty_print {
        Box::new(
            fmt::Subscriber::builder()
                .with_env_filter(env_filter)
                .finish(),
        )
    } else {
        match (*ENV_CONFIG).env {
            Env::Dev => Box::new(
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .finish(),
            ),
            Env::Stag | Env::Prod => Box::new(
                fmt::Subscriber::builder()
                    .json()
                    .with_env_filter(env_filter)
                    .finish(),
            ),
        }
    };

    tracing::subscriber::set_global_default(subscriber)
        .expect("expect to be able to set global default subscriber");
}
