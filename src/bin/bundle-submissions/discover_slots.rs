use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    future::try_join_all,
    SinkExt, StreamExt, TryStreamExt,
};
use mouseion::units::Slot;
use object_store::{aws::AmazonS3, path::Path, ObjectStore};
use tokio::{spawn, task::JoinHandle};
use tracing::{debug, info, warn};

// Make sure not to bundle the current slot as we might create incomplete bundles.
// TODO: make this dynamically the current slot - 1
const SLOT_LIMIT: i32 = 7696479;

const SLOT_MEMORY: usize = 32;

async fn discover_slots_for_path(
    path: &Path,
    object_store: &AmazonS3,
    mut slots_tx: Sender<Slot>,
) -> anyhow::Result<()> {
    let mut block_submission_meta_stream = object_store.list(Some(path)).await.unwrap();

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

async fn discover_month_paths(
    from: Option<String>,
    object_store: &AmazonS3,
) -> anyhow::Result<Vec<Path>> {
    let path = {
        let path = from.as_deref().unwrap_or("/2023");
        Path::from(path)
    };
    let common_prefixes = object_store
        .list_with_delimiter(Some(&path))
        .await
        .map(|list_results| list_results.common_prefixes);
    let paths = common_prefixes.into_iter().flatten().collect();
    Ok(paths)
}

async fn discover_day_paths(months: &[Path], object_store: &AmazonS3) -> anyhow::Result<Vec<Path>> {
    let days_futs = months
        .into_iter()
        .map(|path| object_store.list_with_delimiter(Some(path)))
        .collect::<Vec<_>>();
    let day_list_results = try_join_all(days_futs).await?;
    let days = day_list_results
        .into_iter()
        .map(|list_results| list_results.common_prefixes)
        .flatten()
        .collect();
    Ok(days)
}

const DAY_CONCURRENCY: usize = 8;
const DAY_SLOT_BUFFER: usize = 8;

pub fn run_discover_slots_thread(
    from: Option<String>,
    object_store: Arc<AmazonS3>,
    mut slots_tx: Sender<Slot>,
) -> JoinHandle<()> {
    let object_store = object_store.clone();
    spawn(async move {
        let month_paths = discover_month_paths(from.clone(), &object_store)
            .await
            .unwrap();
        // These are the days we'd like to concurrently discover slots for.
        let day_paths = discover_day_paths(&month_paths, &object_store)
            .await
            .unwrap();

        // Vec of sender, receiver pair for each task.
        let mut individual_channels: Vec<(Sender<Slot>, Receiver<Slot>)> = Vec::new();

        // Create a channel for each day slot discovery task.
        for _ in 0..DAY_CONCURRENCY {
            let (send, recv) = channel(DAY_SLOT_BUFFER);
            individual_channels.push((send, recv));
        }

        // Start discovering days in a threads. For simplicity sake we create a task for each day,
        // even if we only take from the first DAY_CONCURRENCY.
        for (path, (task_tx, _)) in day_paths.into_iter().zip(individual_channels.iter()) {
            let object_store = object_store.clone();
            let task_tx = task_tx.clone();

            spawn(async move {
                match discover_slots_for_path(&path, &object_store, task_tx).await {
                    Ok(_) => {
                        info!(%path, "finished discovering slots for day");
                    }
                    Err(e) => {
                        panic!("failed to discover slots for day, {}", e);
                    }
                }
            });
        }

        // Take discovered slots round-robin.
        let mut curr_ind = 0;

        loop {
            let rx = &mut individual_channels[curr_ind].1;
            if let Some(slot) = rx.next().await {
                slots_tx.send(slot).await.unwrap();
            } else {
                // The channel is closed, means the task is done
                individual_channels.remove(curr_ind);
            }

            if individual_channels.is_empty() {
                // all tasks are done, break.
                info!("all day slot discovery tasks are done");
                break;
            }

            curr_ind = (curr_ind + 1) % DAY_CONCURRENCY % individual_channels.len();
        }
    })
}
