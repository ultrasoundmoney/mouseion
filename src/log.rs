use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use crate::env::{Env, ENV_CONFIG};

pub fn init() {
    let span_format = if ENV_CONFIG.log_perf {
        FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };

    if ENV_CONFIG.env == Env::Dev {
        tracing_subscriber::fmt()
            .with_span_events(span_format)
            .with_env_filter(EnvFilter::from_default_env())
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_span_events(span_format)
            .with_env_filter(EnvFilter::from_default_env())
            .json()
            .init();
    }
}
