mod bundle_aggregator;
mod env;
mod health;
mod messages;
mod object_storage;
mod serve;
mod units;

use anyhow::{Context, Result};
use bundle_aggregator::ArchiveBundle;
use futures::{channel::mpsc, try_join, FutureExt, StreamExt};
use health::{MessageHealth, NatsHealth};
use object_store::ObjectStore;
use tracing::info;

use crate::bundle_aggregator::BundleAggregator;

#[derive(Debug, Clone)]
pub struct AppState {
    message_health: MessageHealth,
    nats_client: async_nats::Client,
    nats_health: NatsHealth,
}

async fn run_object_storage(
    object_store: impl ObjectStore,
    mut rx: mpsc::UnboundedReceiver<ArchiveBundle>,
) -> Result<()> {
    while let Some(archive_bundle) = rx.next().await {
        object_store
            .put(
                &archive_bundle.path(),
                archive_bundle.to_ndjson_gz()?.into(),
            )
            .await?;

        // Bundle has been successfully stored, ack the messages.
        archive_bundle.ack().await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("starting payload archiver");

    let nats_uri = std::env::var("NATS_URI").unwrap_or_else(|_| "localhost:4222".to_string());
    let nats_client = async_nats::connect(nats_uri)
        .await
        .context("connecting to NATS")?;

    let (tx, rx) = mpsc::unbounded();
    let bundle_aggregator = BundleAggregator::new(tx);

    let object_store = object_storage::build_ovh_object_store()?;

    let nats_health = NatsHealth::new(nats_client.clone());
    let message_health = MessageHealth::new();

    let app_state = AppState {
        nats_client,
        nats_health,
        message_health,
    };

    let messages_thread = messages::process_messages(app_state.clone(), &bundle_aggregator);

    let bundle_aggregator_thread = bundle_aggregator.run();

    let server_thread = serve::serve(app_state);

    let object_storage_thread = tokio::spawn(run_object_storage(object_store, rx));

    try_join!(
        messages_thread,
        server_thread,
        bundle_aggregator_thread,
        object_storage_thread.map(|res| res?)
    )
    .context("joining message, server, and bundle_aggregator tasks, and object storage thread")?;

    info!("payload archiver exiting");

    Ok(())
}
