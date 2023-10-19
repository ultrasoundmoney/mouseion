use std::collections::{HashSet, VecDeque};
use std::io::Read;
use std::sync::Arc;

use ::object_store as object_store_lib;
use backoff::ExponentialBackoff;
use bytes::Bytes;
use chrono::{Datelike, Timelike};
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    try_join, SinkExt, StreamExt, TryStreamExt,
};
use lazy_static::lazy_static;
use mouseion::{env::ENV_CONFIG, log, object_store, units::Slot, BlockSubmission};
use object_store_lib::aws::AmazonS3;
use object_store_lib::{path::Path, ObjectStore};
use tokio::sync::Semaphore;
use tokio::task::spawn_blocking;
use tokio::{spawn, task::JoinHandle};
use tracing::{debug, error, info, instrument, trace, warn};

type ObjectPath = object_store_lib::path::Path;

lazy_static! {
    static ref SOURCE_BUCKET: String = (*ENV_CONFIG.submissions_bucket).to_string();
}

// Make sure not to bundle the current slot as we might create incomplete bundles.
const SLOT_LIMIT: i32 = 7562638;
const SLOT_MEMORY: usize = 32;

async fn discover_slots(object_store: &AmazonS3, mut slots_tx: Sender<Slot>) -> anyhow::Result<()> {
    let mut block_submission_meta_stream =
        object_store.list(Some(&Path::from("/2023"))).await.unwrap();

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

fn run_discover_slots_thread(
    object_store: Arc<AmazonS3>,
    slots_tx: Sender<Slot>,
) -> JoinHandle<()> {
    let object_store = object_store.clone();
    spawn(async move {
        match discover_slots(object_store.as_ref(), slots_tx).await {
            Ok(_) => info!("finished discovering slots"),
            Err(e) => panic!("discover_slots failed: {:?}", e),
        };
    })
}

/// Does three things given a slot.
/// 1. Fetch all block submissions for the slot.
/// 2. Decompress them.
/// 3. Bundle them together in a Vec.
#[instrument(skip(object_store), fields(%slot))]
async fn bundle_slot(
    object_store: Arc<AmazonS3>,
    slot: Slot,
) -> anyhow::Result<Vec<BlockSubmission>> {
    let path = slot.partial_s3_path();

    debug!(%path, "fetching all submissions for slot");

    let mut block_submission_meta_stream = object_store.list(Some(&path)).await?;

    let mut block_submissions = Vec::new();

    while let Some(block_submission_meta) = block_submission_meta_stream.try_next().await? {
        let bytes_gz = object_store
            .get(&block_submission_meta.location)
            .await?
            .bytes()
            .await?;

        let decoder = flate2::read::GzDecoder::new(&bytes_gz[..]);
        let block_submission: BlockSubmission = serde_json::from_reader(decoder)?;

        trace!(
            block_hash = block_submission.block_hash(),
            "fetched block submission"
        );

        block_submissions.push(block_submission);
    }

    debug!("fetched all bundles for slot");

    if block_submissions.is_empty() {
        warn!("no submissions found for slot");
    }

    Ok(block_submissions)
}

const FETCH_BUNDLE_CONCURRENCY: usize = 8;

fn run_bundle_slots_thread(
    object_store: Arc<AmazonS3>,
    slots_rx: Receiver<Slot>,
    bundles_tx: Sender<(Slot, Vec<BlockSubmission>)>,
) -> JoinHandle<()> {
    spawn(async move {
        slots_rx
            .map(Ok)
            .try_for_each_concurrent(FETCH_BUNDLE_CONCURRENCY, |slot| {
                let object_store = object_store.clone();
                let mut bundles_tx = bundles_tx.clone();
                async move {
                    let bundle = bundle_slot(object_store, slot).await?;
                    bundles_tx.send((slot, bundle)).await?;
                    Ok::<_, anyhow::Error>(())
                }
            })
            .await
            .unwrap();
    })
}

fn bundle_path_from_slot(slot: Slot) -> Path {
    let slot_date_time = slot.date_time();
    let year = slot_date_time.year();
    let month = slot_date_time.month();
    let day = slot_date_time.day();
    let hour = slot_date_time.hour();
    let minute = slot_date_time.minute();

    let path_string = format!("{year}/{month:02}/{day:02}/{hour:02}/{minute:02}/{slot}.ndjson.gz");
    Path::from(path_string)
}

#[instrument(skip(bundle), fields(%slot))]
async fn compress_bundle(bundle: Vec<BlockSubmission>, slot: Slot) -> anyhow::Result<Bytes> {
    // convert each block submission to a json string, then join them with newline
    let ndjson: String = bundle
        .into_iter()
        .map(|item| match serde_json::to_string(&item) {
            Ok(json_string) => Ok(json_string + "\n"),
            Err(_) => Err(anyhow::anyhow!("Failed to serialize item")),
        })
        .collect::<Result<Vec<String>, _>>()?
        .join("");

    let bytes_gz = spawn_blocking(move || {
        // get the bytes
        let bytes = ndjson.as_bytes();

        // start the encoder with the bytes
        let mut encoder = flate2::read::GzEncoder::new(bytes, flate2::Compression::default());
        let mut bytes_gz = vec![];

        // encode (compress) the bytes
        match encoder.read_to_end(&mut bytes_gz) {
            Ok(_) => Ok(bytes_gz.into()),
            Err(err) => Err(anyhow::anyhow!("Failed to compress data: {:?}", err)),
        }
    })
    .await??;

    debug!("compressed bundle");

    Ok(bytes_gz)
}

const COMPRESSION_CONCURRENCY: usize = 8;

fn run_compression_thread(
    mut bundles_rx: Receiver<(Slot, Vec<BlockSubmission>)>,
    compressed_bundles_tx: Sender<(Slot, ObjectPath, Bytes)>,
) -> JoinHandle<()> {
    let semaphore = Arc::new(Semaphore::new(COMPRESSION_CONCURRENCY));
    spawn(async move {
        while let Some((slot, bundle)) = bundles_rx.next().await {
            let mut compressed_bundles_tx = compressed_bundles_tx.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            spawn(async move {
                match compress_bundle(bundle, slot).await {
                    Ok(bytes_gz) => {
                        let path = bundle_path_from_slot(slot);

                        compressed_bundles_tx
                            .send((slot, path, bytes_gz))
                            .await
                            .unwrap();
                    }
                    Err(e) => {
                        panic!("compress_bundle failed: {:?}", e);
                    }
                }
                drop(permit);
            });
        }

        info!("finished compressing bundles");
    })
}

#[instrument(skip(bundle_gz, object_store), fields(path = path.to_string(), %slot))]
async fn store_bundle(
    bundle_gz: Bytes,
    object_store: &AmazonS3,
    path: ObjectPath,
    slot: Slot,
) -> anyhow::Result<()> {
    backoff::future::retry(ExponentialBackoff::default(), || async {
        object_store
            .put(&path, bundle_gz.clone())
            .await
            .map_err(|err| {
                if err.to_string().contains("409 Conflict") {
                    warn!("failed to execute OVH put operation: {}, retrying", err);
                    backoff::Error::Transient {
                        err,
                        retry_after: None,
                    }
                } else {
                    error!("failed to execute OVH put operation: {}", err);
                    backoff::Error::Permanent(err)
                }
            })
    })
    .await?;

    debug!("stored bundle");

    Ok(())
}

fn run_store_bundles_thread(
    bundles_stroe: AmazonS3,
    mut compressed_bundles_rx: Receiver<(Slot, ObjectPath, Bytes)>,
    mut slots_to_delete_tx: Sender<Slot>,
) -> JoinHandle<()> {
    spawn(async move {
        while let Some((slot, path, bundle_gz)) = compressed_bundles_rx.next().await {
            match store_bundle(bundle_gz, &bundles_stroe, path, slot).await {
                Ok(_) => slots_to_delete_tx.send(slot).await.unwrap(),
                Err(e) => {
                    panic!("store_bundle failed: {:?}", e);
                }
            };
        }
    })
}

#[instrument(skip(object_store), fields(%slot))]
async fn delete_slot(object_store: Arc<AmazonS3>, slot: Slot) -> anyhow::Result<()> {
    let path = slot.partial_s3_path();
    let mut block_submission_meta_stream = object_store.list(Some(&path)).await?;

    while let Some(object_meta) = block_submission_meta_stream.try_next().await? {
        trace!(location = %object_meta.location, "deleting submission");
        object_store.delete(&object_meta.location).await?;
    }

    debug!("deleted all submissions for slot");

    Ok(())
}

const DELETE_SLOT_CONCURRENCY: usize = 8;

fn run_clean_source_submissions_thread(
    object_store: Arc<AmazonS3>,
    slots_to_delete_rx: Receiver<Slot>,
) -> JoinHandle<()> {
    spawn(async move {
        slots_to_delete_rx
            .map(Ok)
            .try_for_each_concurrent(DELETE_SLOT_CONCURRENCY, |slot| {
                let object_store = object_store.clone();
                delete_slot(object_store, slot)
            })
            .await
            .unwrap();
    })
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    log::init();

    info!("starting to bundle submissions");

    let submissions_store = Arc::new(object_store::build_submissions_store()?);

    let bundles_store = object_store::build_bundles_store()?;

    let (slots_tx, slots_rx) = channel(8);

    let (bundles_tx, bundles_rx) = channel(8);

    let (compressed_bundles_tx, compressed_bundles_rx) = channel(8);

    let (slots_to_delete_tx, slots_to_delete_rx) = channel(8);

    try_join!(
        run_discover_slots_thread(submissions_store.clone(), slots_tx),
        run_bundle_slots_thread(submissions_store.clone(), slots_rx, bundles_tx),
        run_compression_thread(bundles_rx, compressed_bundles_tx),
        run_store_bundles_thread(bundles_store, compressed_bundles_rx, slots_to_delete_tx),
        run_clean_source_submissions_thread(submissions_store, slots_to_delete_rx)
    )?;

    info!("finished bundling submissions");

    Ok(())
}
