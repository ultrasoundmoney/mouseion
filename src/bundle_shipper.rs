use anyhow::Result;

use futures::{channel::mpsc, StreamExt};
use object_store::ObjectStore;
use tracing::debug;

use crate::{bundle_aggregator::SlotBundle, performance::TimedExt};

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

            self.object_store
                .put(
                    &archive_bundle.path(),
                    archive_bundle.to_ndjson_gz()?.into(),
                )
                .timed("put-to-object-store")
                .await?;

            // Bundle has been successfully stored, ack the messages.
            archive_bundle.ack().await?;
        }

        Ok(())
    }
}
