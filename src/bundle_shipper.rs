use std::sync::Arc;

use anyhow::{Error, Result};
use futures::{channel::mpsc, FutureExt, StreamExt};
use object_store::ObjectStore;
use tokio::{select, sync::Notify};
use tracing::{debug, error, info};

use crate::{bundle_aggregator::CompleteBundle, performance::TimedExt};

const COMPRESS_BUNDLE_CONCURRENCY_LIMIT: usize = 3;

pub struct BundleShipper<OS: ObjectStore> {
    bundle_rx: mpsc::Receiver<CompleteBundle>,
    object_store: Arc<OS>,
}

impl<OS: ObjectStore> BundleShipper<OS> {
    pub fn new(bundle_rx: mpsc::Receiver<CompleteBundle>, object_store: OS) -> Self {
        Self {
            bundle_rx,
            object_store: Arc::new(object_store),
        }
    }

    async fn run_ship_bundles(self) -> Result<()> {
        // Compression is slow, but can be done in parallel.
        self.bundle_rx.for_each_concurrent(COMPRESS_BUNDLE_CONCURRENCY_LIMIT, |complete_bundle| {
            let object_store = self.object_store.clone();
            async move {
                debug!(slot = %complete_bundle.slot, "storing bundle");

                let path = complete_bundle.path();

                debug!(slot = %complete_bundle.slot, "converting bundle to ndjson");
                let uncompressed_size_kb = complete_bundle.execution_payloads_ndjson.len() / 1000;

                debug!(slot = %complete_bundle.slot, "compressing bundle");
                let complete_bundle_arc = Arc::new(complete_bundle);
                let complete_bundle_arc_clone = complete_bundle_arc.clone();
                let bytes_ndjson_gz = complete_bundle_arc.compress_ndjson().timed("compress-bundle").await?;
                let compressed_size_kb = bytes_ndjson_gz.len() / 1000;

                debug!(
                    slot = %complete_bundle_arc_clone.slot,
                    uncompressed_size_kb,
                    compressed_size_kb,
                    compression_ratio = uncompressed_size_kb as f64 / compressed_size_kb as f64,
                    "compressed bundle"
                );

                object_store
                    .put(&path, bytes_ndjson_gz.into())
                    .timed("put-to-object-store")
                    .await?;
                info!(slot = %complete_bundle_arc_clone.slot, "stored bundle");

                // Bundle has been successfully stored, ack the messages.
                complete_bundle_arc_clone
                    .ack_all()
                    .timed("ack-messages-for-bundle")
                    .await?;
                debug!(slot = %complete_bundle_arc_clone.slot, "acked messages used to construct bundle");

                Ok::<_, Error>(complete_bundle_arc_clone.slot)
            }.map(|result| {
                match result {
                    Ok(_) => {},
                    Err(e) => error!(%e, "failed to store bundle"),
                }
            })
        }).await;

        Ok(())
    }

    pub async fn run(self, shutdown_notify: &Notify) {
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
