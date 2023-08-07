//! Bundle aggregator, aggregates execution payloads into bundles, and makes them available on
//! stream for storage.
//!
//! Interesting performance trade-off is to do csv serialization and compression work as messages
//! are received, or to buffer them by slot and do the work all at once, concurrently.
mod hashmap_size;

use std::{collections::HashMap, io::Write, sync::Arc};

use anyhow::{anyhow, Result};
use async_nats::jetstream::message::Acker;
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use flate2::{write::GzEncoder, Compression};
use futures::{
    channel::mpsc::{self, Receiver},
    future::try_join_all,
    FutureExt, SinkExt, StreamExt,
};
use object_store::path::Path;
use payload_archiver::units::Slot;
use tokio::{
    select,
    sync::{Mutex, Notify, RwLock},
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    message_consumer::AckablePayload,
    operation_constants::{BUNDLE_MIN_AGE, BUNDLE_SLOT_MAX_AGE, MAX_INCOMPLETE_BUNDLES},
};

const AGGREGATION_INTERVAL_DURATION: std::time::Duration = std::time::Duration::from_secs(1);

// Archive bundles, bundle together all execution payloads which share a slot. In addition, they
// store when we first and last, saw an execution payload for this slot.
pub struct SlotBundle {
    // Used to Ack all messages linked to the bundle.
    ackers: Vec<Acker>,
    // Earliest is when we first saw an execution payload for the slot these payloads are for.
    earliest: DateTime<Utc>,
    execution_payloads_ndjson: String,
    pub slot: Slot,
}

pub struct CompleteBundle {
    pub slot: Slot,
    pub ackers: Vec<Acker>,
    pub execution_payloads_ndjson: String,
}

impl CompleteBundle {
    pub fn path(&self) -> Path {
        let slot = self.slot;
        let slot_date_time = slot.date_time();
        let year = slot_date_time.year();
        let month = slot_date_time.month();
        let day = slot_date_time.day();
        let hour = slot_date_time.hour();
        let minute = slot_date_time.minute();
        let slot = slot.to_string();
        let path_string =
            format!("{year}/{month:02}/{day:02}/{hour:02}/{minute:02}/{slot}.ndjson.gz");
        Path::from(path_string)
    }

    pub async fn ack_all(&self) -> Result<()> {
        try_join_all(self.ackers.iter().map(|acker| acker.ack()))
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(())
    }

    pub async fn compress_ndjson(self: Arc<Self>) -> Result<Vec<u8>> {
        let self_clone = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(self_clone.execution_payloads_ndjson.as_bytes())?;
            let bytes = encoder.finish()?;
            Ok(bytes)
        })
        .await?
    }
}

impl From<SlotBundle> for CompleteBundle {
    fn from(slot_bundle: SlotBundle) -> Self {
        Self {
            slot: slot_bundle.slot,
            ackers: slot_bundle.ackers,
            execution_payloads_ndjson: slot_bundle.execution_payloads_ndjson,
        }
    }
}

pub struct BundleAggregator {
    slot_bundles: RwLock<HashMap<Slot, SlotBundle>>,
    bundle_tx: mpsc::Sender<CompleteBundle>,
    ackable_payload_rx: Mutex<Receiver<AckablePayload>>,
    shutdown_notify: Arc<Notify>,
}

impl BundleAggregator {
    pub fn new(
        ackable_payload_rx: mpsc::Receiver<AckablePayload>,
        bundle_tx: mpsc::Sender<CompleteBundle>,
        shutdown_notify: Arc<Notify>,
    ) -> Self {
        Self {
            ackable_payload_rx: Mutex::new(ackable_payload_rx),
            bundle_tx,
            shutdown_notify,
            slot_bundles: RwLock::new(HashMap::new()),
        }
    }

    async fn add_execution_payload(&self, (acker, archive_payload): AckablePayload) -> Result<()> {
        let mut slot_bundles = self.slot_bundles.write().await;

        let slot_bundle = slot_bundles
            .entry(archive_payload.slot)
            .or_insert_with(|| SlotBundle {
                ackers: Vec::new(),
                earliest: Utc::now(),
                execution_payloads_ndjson: String::new(),
                slot: archive_payload.slot,
            });

        slot_bundle.ackers.push(acker);

        slot_bundle
            .execution_payloads_ndjson
            .push_str(&archive_payload.payload.to_string());
        slot_bundle.execution_payloads_ndjson.push('\n');

        Ok(())
    }

    async fn find_complete_slots(&self) -> Vec<Slot> {
        let slot_bundles = self.slot_bundles.read().await;

        trace!(
            size_mb = tracing::field::display({
                let size = hashmap_size::size_of_hashmap(&slot_bundles);
                size / 1_000_000
            }),
            "scanning for complete slots in slot_bundles map"
        );

        slot_bundles
            .iter()
            .filter(|(slot, bundle)| {
                // During normal operation, the is_past_slot check should be enough. However, at times,
                // there may be a backlog. We wait at least this long from the first moment we see
                // a message for a slot, before we start archiving it, to make sure we've cleared any
                // backlog.
                let is_bundle_old_enough =
                    Utc::now() - bundle.earliest >= Duration::from_std(BUNDLE_MIN_AGE).unwrap();

                // We only want to archive slots which are at least one slot old. This is to avoid
                // archiving slots which are still being built. Normally the very first message for
                // a slot comes in at t-12. Proposer normally ask for bids around t+2. If things go
                // very wrong, perhaps t+6. At t+12 it becomes impossible to collect enough
                // attestations. Still, we may receive messages, so we allow for a very generous
                // buffer. Shipping our bundle at t + 12 + 8 = t+20.
                let is_past_slot = slot.date_time() + *BUNDLE_SLOT_MAX_AGE < Utc::now();

                is_bundle_old_enough && is_past_slot
            })
            .map(|(slot, _)| *slot)
            .collect()
    }

    async fn get_complete_bundles(&self) -> Result<Vec<(Slot, SlotBundle)>> {
        let complete_slots = self.find_complete_slots().await;
        let mut slot_bundles = self.slot_bundles.write().await;
        let mut complete_bundles = Vec::new();
        for slot in complete_slots {
            let bundle = slot_bundles.remove(&slot).unwrap();

            trace!(
                %slot,
                bundle_payload_count = bundle.execution_payloads_ndjson.len(),
                bundle_size_kb = bundle.execution_payloads_ndjson.len() / 1000,
                "completed new bundle"
            );

            complete_bundles.push((slot, bundle));
        }

        Ok(complete_bundles)
    }

    async fn get_oldest_bundle(&self) -> Option<SlotBundle> {
        let mut slot_bundles = self.slot_bundles.write().await;
        let oldest_slot = slot_bundles.keys().min().cloned();
        oldest_slot.map(|oldest_slot| slot_bundles.remove(&oldest_slot).unwrap())
    }

    async fn run_consume_ackable_payloads_inner(&self) -> Result<()> {
        let mut ackable_payload_rx = self.ackable_payload_rx.lock().await;
        while let Some(ackable_payload) = ackable_payload_rx.next().await {
            // Because many ackable payloads may be consumed from a server side backlog and bundles
            // have a minimum time to complete, we may run out of memory when trying to consume all
            // available payloads. We make the relatively safe assumption that no payloads will
            // arrive for the oldest bundle once MAX_INCOMPLETE_BUNDLES - 1 newer bundles are
            // already being formed.
            if !self
                .slot_bundles
                .read()
                .await
                .contains_key(&ackable_payload.1.slot)
                && self.slot_bundles.read().await.len() == MAX_INCOMPLETE_BUNDLES
            {
                let mut tx = self.bundle_tx.clone();

                let oldest_slot_bundle = self.get_oldest_bundle().await.expect("expect slot_bundles to have at least one slot bundle when at incomplete bundle limit");

                warn!(
                    MAX_INCOMPLETE_BUNDLES,
                    slot = %oldest_slot_bundle.slot,
                    "hit incomplete bundles limit, queueing oldest aggregated bundle for storage",
                );

                tx.send(oldest_slot_bundle.into()).await?;
            }

            self.add_execution_payload(ackable_payload).await?;
        }

        Ok(())
    }

    pub async fn run_consume_ackable_payloads(&self) {
        select! {
            result = self.run_consume_ackable_payloads_inner() => {
                match result {
                    Ok(_) => info!("bundle aggregator stopped consuming ackable payloads"),
                    Err(e) => {
                        error!(%e, "bundle aggregator exited with error");
                        self.shutdown_notify.notify_waiters();
                    },
                }
            }
            _ = self.shutdown_notify.notified() => {
                info!("bundle aggregator ackable payload consumption shutting down");
            }
        }
    }

    async fn run_complete_bundle_check_inner(&self) -> Result<()> {
        debug!(
            "starting bundle aggregator, on a {:?} interval",
            AGGREGATION_INTERVAL_DURATION
        );

        let mut interval = tokio::time::interval(AGGREGATION_INTERVAL_DURATION);

        let mut tx = self.bundle_tx.clone();

        loop {
            interval.tick().await;

            let complete_bundles = self.get_complete_bundles().await?;

            if complete_bundles.is_empty() {
                trace!(
                    "no slot bundles are old enough to archive, sleeping for {}s",
                    AGGREGATION_INTERVAL_DURATION.as_secs()
                );
            } else {
                info!(
                    count = complete_bundles.len(),
                    "bundles are old enough with slots far enough in the past to archive",
                );
                for (slot, bundle) in complete_bundles {
                    trace!(%slot, "queueing aggregated bundle for storage");
                    tx.send(bundle.into()).await?;
                }
            }
        }
    }

    pub async fn run_complete_bundle_check(&self) {
        select! {
            result = self.run_complete_bundle_check_inner() => {
                match result {
                    Ok(_) => info!("bundle aggregator bundle checking stopped"),
                    Err(e) => {
                        error!(%e, "bundle aggregator exited with error");
                        self.shutdown_notify.notify_waiters();
                    },
                }
            }
            _ = self.shutdown_notify.notified().fuse() => {
                info!("bundle aggregator complete bundle checking shutting down");
            }
        }
    }
}
