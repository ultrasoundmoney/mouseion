use anyhow::{Context, Result};
use async_nats::jetstream;
use tracing::{debug, info};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("publishing dummy messages");

    let nats_uri = std::env::var("NATS_URI").unwrap_or_else(|_| "localhost:4222".to_string());
    let nats_client = async_nats::connect(nats_uri)
        .await
        .context("connecting to NATS")?;
    let nats_context = jetstream::new(nats_client);

    let messages_file = std::fs::File::open("./dummy_data/2023-08-01_messages.json")?;
    let messages: Vec<serde_json::Value> = serde_json::from_reader(messages_file)?;
    let messages_len = messages.len();

    for payload in messages {
        debug!("publishing payload for slot: {:?}", payload["slot"]);
        nats_context
            .publish(
                "payload-archive".to_string(),
                serde_json::to_vec(&payload).unwrap().into(),
            )
            .await?;
    }

    info!("done publishing {} dummy messages", messages_len);

    Ok(())
}
