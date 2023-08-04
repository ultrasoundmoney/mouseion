use std::io::Write;

use anyhow::Result;

use flate2::{write::GzEncoder, Compression};
use futures::{channel::mpsc, StreamExt};
use object_store::ObjectStore;
use tracing::debug;

use crate::{bundle_aggregator::SlotBundle, performance::TimedExt};

pub async fn compress_ndjson(ndjson: String) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(ndjson.as_bytes())?;
    let bytes = encoder.finish()?;
    Ok(bytes)
}

pub struct BundleShipper<OS: ObjectStore> {
    bundle_rx: mpsc::Receiver<SlotBundle>,
    object_store: OS,
}

impl<OS: ObjectStore> BundleShipper<OS> {
    pub fn new(bundle_rx: mpsc::Receiver<SlotBundle>, object_store: OS) -> Self {
        Self {
            bundle_rx,
            object_store,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        while let Some(archive_bundle) = self.bundle_rx.next().await {
            debug!(slot = %archive_bundle.slot, "storing bundle");

            let path = archive_bundle.path();
            let ndjson = archive_bundle.to_ndjson()?;
            let bytes_ndjson_gz =
                tokio::spawn(async move { compress_ndjson(ndjson).await }).await??;

            self.object_store
                .put(&path, bytes_ndjson_gz.into())
                .timed("put-to-object-store")
                .await?;

            // Bundle has been successfully stored, ack the messages.
            archive_bundle.ack().await?;
        }

        Ok(())
    }
}
