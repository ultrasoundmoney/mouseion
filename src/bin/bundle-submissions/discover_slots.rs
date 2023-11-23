use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use futures::{channel::mpsc::Sender, SinkExt, TryStreamExt};
use mouseion::units::Slot;
use object_store::{aws::AmazonS3, path::Path, ObjectStore};
use tokio::{spawn, task::JoinHandle, time::sleep};
use tracing::{debug, info, warn};

use crate::beacon_node::BeaconNode;

const SLOT_MEMORY: usize = 32;

async fn discover_slots_loop(
    beacon_node: &BeaconNode,
    object_store: &AmazonS3,
    mut slots_tx: Sender<Slot>,
) -> anyhow::Result<()> {
    loop {
        // Make sure not to bundle the current slot as we might create incomplete bundles.
        let max_slot = {
            let current_slot = beacon_node.current_slot().await?;
            current_slot - 1
        };

        let path = &Path::from("/2023");
        let mut block_submission_meta_stream = object_store.list(Some(path));

        let mut last_sent_slots: VecDeque<Slot> = VecDeque::with_capacity(SLOT_MEMORY);
        let mut last_sent_slots_set: HashSet<Slot> = HashSet::with_capacity(SLOT_MEMORY);

        while let Some(block_submission_meta) = block_submission_meta_stream.try_next().await? {
            let path_str = block_submission_meta.location.to_string();
            let slot = path_str
                .split('/')
                .nth_back(1)
                .expect("failed to find second from last '/' delimited string")
                .parse::<Slot>()
                .expect("failed to parse string segment as slot");

            if slot.0 > max_slot {
                warn!(
                    "slot {} is greater than max_slot {}, stopping discovery and checking current max_slot",
                    slot, max_slot
                );
                // We sleep 13s in case we've caught up with the head of the chain.
                sleep(Duration::from_secs(13)).await;
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
    }
}

fn check_if_error_is_connection_reset_by_peer(err: &anyhow::Error) -> bool {
    err.downcast_ref::<std::io::Error>()
        .map_or(false, |e| e.kind() == std::io::ErrorKind::ConnectionReset)
}

pub fn run_discover_slots_thread(
    beacon_node: BeaconNode,
    object_store: Arc<AmazonS3>,
    slots_tx: Sender<Slot>,
) -> JoinHandle<()> {
    spawn(async move {
        // OVH Object Storage resets the connection sometimes. If this is the case, we simply
        // retry.
        loop {
            match discover_slots_loop(&beacon_node, object_store.as_ref(), slots_tx.clone()).await {
                Ok(_) => {
                    info!("finished discovering slots");
                    break;
                }
                Err(e) => {
                    if check_if_error_is_connection_reset_by_peer(&e) {
                        warn!("failed to discover slots, {}, retrying", e);
                        continue;
                    } else {
                        panic!("failed to discover slots, {}", e);
                    }
                }
            }
        }
    })
}
