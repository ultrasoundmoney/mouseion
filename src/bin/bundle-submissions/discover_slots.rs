use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use futures::{channel::mpsc::Sender, SinkExt, StreamExt, TryStreamExt};
use mouseion::units::Slot;
use object_store::{aws::AmazonS3, path::Path, ObjectStore};
use tokio::{spawn, task::JoinHandle};
use tracing::{debug, info, warn};

// Make sure not to bundle the current slot as we might create incomplete bundles.
const SLOT_LIMIT: i32 = 7562638;

const SLOT_MEMORY: usize = 32;

async fn discover_next_day(from: Option<String>, object_store: &AmazonS3) -> anyhow::Result<i32> {
    let path = from.as_deref().unwrap_or("/2023");
    let mut block_submission_meta_stream =
        object_store.list(Some(&Path::from(path))).await.unwrap();
    let first = block_submission_meta_stream
        .next()
        .await
        .expect("non-empty stream")?;
    let path_str = first.location.to_string();
    let day = path_str
        .split('/')
        .nth(2)
        .expect("failed to find third '/' delimited string")
        .parse::<i32>()
        .expect("failed to parse string segment as day");

    Ok(day)
}

async fn discover_slots_for_day(
    day: i32,
    object_store: &AmazonS3,
    mut slots_tx: Sender<Slot>,
) -> anyhow::Result<()> {
    // Create 24 streams, one for each hour.
    let mut hour_streams = Vec::with_capacity(24);
    for hour in 0..24 {
        let path = format!("/2023/{:02}/{:02}", day, hour);
        let block_submission_meta_stream =
            object_store.list(Some(&Path::from(path))).await.unwrap();
        hour_streams.push(block_submission_meta_stream);
    }

    let mut last_sent_slots: VecDeque<Slot> = VecDeque::with_capacity(SLOT_MEMORY);
    let mut last_sent_slots_set: HashSet<Slot> = HashSet::with_capacity(SLOT_MEMORY);

    // Get a new slot from each stream in round robin fashion.
    let mut hour_index = 0;
    loop {
        let block_submission_meta_stream = &mut hour_streams[hour_index];
        hour_index = (hour_index + 1) % 24;

        let block_submission_meta = block_submission_meta_stream.try_next().await?;

        if block_submission_meta.is_none() {
            // We have reached the end of the stream for this hour, continue with the others.
            continue;
        }

        let block_submission_meta = block_submission_meta.expect("non-empty stream");

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
        // Decide which day to work on.
        let day = match discover_next_day(from, object_store.as_ref()).await {
            Ok(day) => day,
            Err(e) => panic!("discover_next_day failed: {:?}", e),
        };
        debug!(%day, "discovered day");
        match discover_slots_for_day(day, object_store.as_ref(), slots_tx).await {
            Ok(_) => info!("finished discovering slots"),
            Err(e) => panic!("discover_slots failed: {:?}", e),
        };
    })
}
