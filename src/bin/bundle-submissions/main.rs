//! # Bundle submissions
//! Takes individual block submissions and bundles them up by slot.
//!
//! ## Performance notes
//! Speeding up by fetching slots in parallel seems to not work well at all. We need to fetch all
//! submissions for a slot, which may be hundreds to low thousands. It appears slots in the
//! same vicinity hit the same bottleneck and limit our rate to some 100 payloads per second. To
//! speed up the process we have slot discovery discover slots from different hours in round robin
//! fashion.
mod bundle_slots;
mod compress_bundles;
mod delete_source_submissions;
mod discover_slots;
mod store_bundles;

use std::sync::Arc;

use ::object_store as object_store_lib;
use futures::{channel::mpsc::channel, try_join};
use lazy_static::lazy_static;
use mouseion::{env::ENV_CONFIG, log, object_store};
use tracing::info;

use crate::{
    bundle_slots::run_bundle_slots_thread, compress_bundles::run_compression_thread,
    delete_source_submissions::run_delete_source_submissions_thread,
    discover_slots::run_discover_slots_thread, store_bundles::run_store_bundles_thread,
};

type ObjectPath = object_store_lib::path::Path;

lazy_static! {
    static ref SOURCE_BUCKET: String = (*ENV_CONFIG.submissions_bucket).to_string();
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    log::init();

    info!("starting to bundle submissions");

    let submissions_store = Arc::new(object_store::build_submissions_store()?);

    let bundles_store = object_store::build_bundles_store()?;

    let (slots_tx, slots_rx) = channel(16);

    let (bundles_tx, bundles_rx) = channel(32);

    let (compressed_bundles_tx, compressed_bundles_rx) = channel(8);

    let (slots_to_delete_tx, slots_to_delete_rx) = channel(16);

    let from = std::env::args().nth(1);

    try_join!(
        run_discover_slots_thread(from, submissions_store.clone(), slots_tx),
        run_bundle_slots_thread(submissions_store.clone(), slots_rx, bundles_tx),
        run_compression_thread(bundles_rx, compressed_bundles_tx),
        run_store_bundles_thread(bundles_store, compressed_bundles_rx, slots_to_delete_tx),
        run_delete_source_submissions_thread(submissions_store, slots_to_delete_rx)
    )?;

    info!("finished bundling submissions");

    Ok(())
}
