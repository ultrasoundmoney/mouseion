use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use futures::{channel::mpsc::Sender, SinkExt, TryStreamExt};
use mouseion::units::Slot;
use object_store::{aws::AmazonS3, path::Path, ObjectStore};
use tokio::{spawn, task::JoinHandle};
use tracing::{debug, info, warn};

// Make sure not to bundle the current slot as we might create incomplete bundles.
const SLOT_LIMIT: i32 = 7562638;

const SLOT_MEMORY: usize = 32;

async fn discover_slots(
    from: Option<String>,
    object_store: &AmazonS3,
    mut slots_tx: Sender<Slot>,
) -> anyhow::Result<()> {
    let path = from.as_deref().unwrap_or("/2023");
    let mut block_submission_meta_stream =
        object_store.list(Some(&Path::from(path))).await.unwrap();

    let mut last_sent_slots: VecDeque<Slot> = VecDeque::with_capacity(SLOT_MEMORY);
    let mut last_sent_slots_set: HashSet<Slot> = HashSet::with_capacity(SLOT_MEMORY);

    while let Some(block_submission_meta) = block_submission_meta_stream.try_next().await.unwrap() {
        let path_str = block_submission_meta.location.to_string();
        let slot = path_str
            .split('/')
            .nth_back(1)
            .expect("failed to find second from last '/' delimited string")
            .parse::<Slot>()
            .expect("failed to parse string segment as slot");

        if slot.0 > SLOT_LIMIT {
            warn!(
                "slot {} is greater than SLOT_LIMIT {}, stopping slot discovery",
                slot, SLOT_LIMIT
            );
            break;
        }

        // Only send new slots.
        if !last_sent_slots_set.contains(&slot) {
            debug!(%slot, "discovered slot");
            slots_tx.send(slot).await?;
            last_sent_slots.push_back(slot);
            last_sent_slots_set.insert(slot);
            if last_sent_slots.len() > SLOT_MEMORY {
                // Forget the oldest slot.
                let oldest_slot = last_sent_slots.pop_front().unwrap();
                last_sent_slots_set.remove(&oldest_slot);
            }
        }
    }

    Ok(())
}

pub fn run_discover_slots_thread(
    from: Option<String>,
    object_store: Arc<AmazonS3>,
    slots_tx: Sender<Slot>,
) -> JoinHandle<()> {
    let object_store = object_store.clone();
    spawn(async move {
        match discover_slots(from, object_store.as_ref(), slots_tx).await {
            Ok(_) => info!("finished discovering slots"),
            Err(e) => panic!("discover_slots failed: {:?}", e),
        };
    })
}
