mod bundle_aggregator;
mod env;
mod health;
mod messages;
mod serve;
mod units;

use anyhow::{Context, Result};
use bundle_aggregator::ArchiveBundle;
use chrono::Datelike;
use futures::{channel::mpsc, try_join, FutureExt, StreamExt};
use health::{MessageHealth, NatsHealth};
use object_store::{path::Path, ObjectStore};
use tracing::info;
use units::Slot;

use crate::{bundle_aggregator::BundleAggregator, env::Env};

#[derive(Debug, Clone)]
pub struct AppState {
    message_health: MessageHealth,
    nats_client: async_nats::Client,
    nats_health: NatsHealth,
}

async fn run_object_storage(
    mut rx: mpsc::UnboundedReceiver<(Slot, ArchiveBundle)>,
    file_based_object_store: impl ObjectStore,
) -> Result<()> {
    while let Some((slot, archive_bundle)) = rx.next().await {
        let slot_date_time = slot.date_time();
        let year = slot_date_time.year();
        let month = slot_date_time.month();
        let day = slot_date_time.day();
        let path = Path::from(format!("{year}/{month}/{day}/{slot}.ndjson.gz"));
        file_based_object_store
            .put(&path, archive_bundle.to_ndjson_gz()?.into())
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

    let bucket_name = {
        let env = env::get_env();
        if env == Env::Prod {
            "execution-payload-archive"
        } else {
            "execution-payload-archive-dev"
        }
    };

    let ovh_object_store = object_store::aws::AmazonS3Builder::new()
        .with_endpoint("https://s3.rbx.io.cloud.ovh.net/")
        .with_region("rbx")
        .with_bucket_name(bucket_name)
        .with_secret_access_key(env::get_env_var_unsafe("S3_SECRET_ACCESS_KEY"))
        .with_access_key_id("3a7f56c872164eeb9ea200823ad7b403")
        .build()?;

    let (tx, rx) = mpsc::unbounded();
    let bundle_aggregator = BundleAggregator::new(tx);

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

    let object_storage_thread = tokio::spawn(run_object_storage(rx, ovh_object_store));

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
