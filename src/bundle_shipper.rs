use std::io::Write;

use anyhow::Result;

use flate2::{write::GzEncoder, Compression};
use futures::{channel::mpsc, StreamExt};
use object_store::ObjectStore;
use tokio::{select, sync::Notify};
use tracing::{debug, error, info, trace};

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

    async fn run_ship_bundles(&mut self) -> Result<()> {
        while let Some(archive_bundle) = self.bundle_rx.next().await {
            debug!(slot = %archive_bundle.slot, "storing bundle");

            let path = archive_bundle.path();

            let ndjson = archive_bundle.to_ndjson()?;
            let uncompressed_size_kb = ndjson.len() / 1000;

            let bytes_ndjson_gz =
                tokio::spawn(async move { compress_ndjson(ndjson).await }).await??;
            let compressed_size_kb = bytes_ndjson_gz.len() / 1000;

            trace!(
                slot = %archive_bundle.slot,
                uncompressed_size_kb,
                compressed_size_kb,
                compression_ratio = uncompressed_size_kb as f64 / compressed_size_kb as f64,
                "compressed bundle"
            );

            self.object_store
                .put(&path, bytes_ndjson_gz.into())
                .timed("put-to-object-store")
                .await?;

            debug!(slot = %archive_bundle.slot, "stored bundle");

            // Bundle has been successfully stored, ack the messages.
            archive_bundle.ack().await?;
            debug!(slot = %archive_bundle.slot, "acked messages used to construct bundle")
        }

        Ok(())
    }

    pub async fn run(&mut self, shutdown_notify: &Notify) {
        select! {
            result = self.run_ship_bundles() => {
                match result {
                    Ok(_) => error!("bundle shipper exited unexpectedly"),
                    Err(e) => {
                        error!(%e, "bundle shipper exited with error");
                        shutdown_notify.notify_waiters();
                    },
                }
            }
            _ = shutdown_notify.notified() => {
                info!("bundle shipper shutting down");
            }
        }
    }
}
