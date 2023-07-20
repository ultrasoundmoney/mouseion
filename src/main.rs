mod env;
mod health;
mod messages;
mod serve;

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use eyre::{Context, Result};
use health::{MessageHealth, NatsHealth};
use tokio::try_join;
use tracing::info;

#[derive(Debug, Clone)]
pub struct AppState {
    message_health: MessageHealth,
    nats_client: async_nats::Client,
    nats_health: NatsHealth,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("starting payload archiver");

    let last_message_received: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));

    let nats_uri = std::env::var("NATS_URI").unwrap_or_else(|_| "localhost:4222".to_string());
    let nats_client = async_nats::connect(nats_uri)
        .await
        .context("connecting to NATS")?;

    let nats_health = NatsHealth::new(nats_client.clone());
    let message_health = MessageHealth::new(last_message_received);

    let app_state = AppState {
        nats_client,
        nats_health,
        message_health,
    };

    let messages_thread = tokio::spawn(messages::process_messages(app_state.clone()));

    let server_thread = tokio::spawn(serve::serve(app_state));

    let (messages_thread_result, server_thread_result) =
        try_join!(messages_thread, server_thread).context("joining message and server threads")?;

    messages_thread_result?;
    server_thread_result?;

    info!("payload archiver exiting");

    Ok(())
}
