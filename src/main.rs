mod env;
mod health;
mod messages;
mod serve;

use eyre::{Context, Result};
use futures::try_join;
use health::{MessageHealth, NatsHealth};
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

    let nats_uri = std::env::var("NATS_URI").unwrap_or_else(|_| "localhost:4222".to_string());
    let nats_client = async_nats::connect(nats_uri)
        .await
        .context("connecting to NATS")?;

    let nats_health = NatsHealth::new(nats_client.clone());
    let message_health = MessageHealth::new();

    let app_state = AppState {
        nats_client,
        nats_health,
        message_health,
    };

    let messages_thread = messages::process_messages(app_state.clone());

    let server_thread = serve::serve(app_state);

    try_join!(messages_thread, server_thread).context("joining message and server threads")?;

    info!("payload archiver exiting");

    Ok(())
}
