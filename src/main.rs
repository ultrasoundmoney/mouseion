//! # Payload Archiver
//! Takes the many execution payloads that the relay receives, over a message queue, bundles them
//! by slot, and stores them in cheap Object Storage.
//!
//! ## Architecture
//! The archiver is composed of three main components:
//! - A message consumer, which consumes messages from the message queue, and puts them on the
//!   ackable payload queue.
//! - A bundle aggregator, takes ackable payloads, and aggregates them into bundles by slot.
//!   Complete bundles are put on the bundle queue.
//! - An object storage, which takes bundles from the bundle queue, and stores them in object
//!   storage.
mod bundle_aggregator;
mod bundle_shipper;
mod health;
mod message_consumer;
mod object_stores;
mod operation_constants;
mod performance;
mod server;
mod trace_memory;

use std::{
    collections::HashSet,
    io::Write,
    iter::StepBy,
    ops::RangeInclusive,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Error, Result};
use async_nats::jetstream::{
    self,
    consumer::{self, pull, Consumer, DeliverPolicy},
    stream::{ConsumerError, ConsumerErrorKind, Stream},
    ErrorCode,
};
use bytes::Bytes;
use chrono::{Datelike, TimeZone, Timelike, Utc};
use flate2::{write::GzEncoder, Compression};
use futures::{
    channel::mpsc::{self},
    StreamExt, TryStreamExt,
};
use health::{MessageConsumerHealth, NatsHealth};
use lazy_static::lazy_static;
use nanoid::nanoid;
use object_store::{path::Path, ObjectStore};
use payload_archiver::{env, units::Slot};
use tokio::{select, sync::Notify, try_join};
use tracing::{debug, error, info, trace, Level};

use crate::{
    bundle_aggregator::BundleAggregator,
    bundle_shipper::BundleShipper,
    message_consumer::{MessageConsumer, STREAM_NAME},
    operation_constants::{MAX_ACK_PENDING, MAX_PAYLOADS_PER_BUNDLE},
    performance::TimedExt,
};

// Maximum number of bundles that can be waiting for shipping. Set to limit memory usage and provide
// backpressure.
const MAX_BUNDLE_QUEUE_SIZE: usize = 2;

// Maximum number of ackable payloads that can be waiting for aggregation. Set to limit memory
// usage and provide backpressure.
const MAX_ACKABLE_PAYLOAD_QUEUE_SIZE: usize = 512;

lazy_static! {
    static ref CONSUMER_ID: String = nanoid!(4);
    static ref CONSUMER_WQ_CONSUMER_NOT_UNIQUE: String =
        serde_json::to_string(&jetstream::ErrorCode::CONSUMER_WQ_CONSUMER_NOT_UNIQUE).unwrap();
}

struct AvailableSequenceProvider {
    stream: jetstream::stream::Stream,
}

impl AvailableSequenceProvider {
    pub fn new(stream: jetstream::stream::Stream) -> Self {
        Self { stream }
    }

    async fn update_stream_info(&mut self) -> Result<()> {
        self.stream.info().await?;
        Ok(())
    }

    async fn available_sequence(&mut self) -> Result<Option<StepBy<RangeInclusive<u64>>>> {
        self.update_stream_info().await?;
        let stream_info = self.stream.cached_info();

        if stream_info.state.messages == 0 {
            return Ok::<_, Error>(None);
        }

        let first_sequence = stream_info.state.first_sequence;
        let last_sequence = stream_info.state.last_sequence;
        // Getting every message to look which subjects are in there would include a lot of
        // overhead. As we can make a reasonable estimate of the number of messages per slot, we
        // can use that to determine the step size. In addition, missing a subject is fine,
        // eventually it'll be the first one on the queue.
        const MIN_BUNDLE_SIZE: usize = 100;
        let range = (first_sequence..=last_sequence).step_by(MIN_BUNDLE_SIZE);
        Ok(Some(range))
    }
}

struct AvailableSlotProvider {
    available_sequence_provider: AvailableSequenceProvider,
    kv_store: jetstream::kv::Store,
    stream: jetstream::stream::Stream,
}

const SLOT_LOCKS_BUCKET_NAME: &str = "slot-locks";

impl AvailableSlotProvider {
    const LOCK_SLOT_KEY_PREFIX: &str = "slot-lock";

    fn make_lock_key(slot: u32) -> String {
        format!("{}.{}", Self::LOCK_SLOT_KEY_PREFIX, slot)
    }

    pub async fn new(jetstream: &jetstream::Context, stream: Stream) -> Result<Self> {
        let available_sequence_provider = AvailableSequenceProvider::new(stream.clone());
        let kv_store = jetstream.get_key_value(SLOT_LOCKS_BUCKET_NAME).await?;

        Ok(Self {
            available_sequence_provider,
            kv_store,
            stream,
        })
    }

    pub async fn lock_next_available_slot(&mut self) -> Result<Option<u32>> {
        let available_sequence = {
            let available_sequence = self
                .available_sequence_provider
                .available_sequence()
                .await?;
            match available_sequence {
                Some(available_sequence) => available_sequence,
                None => return Ok(None),
            }
        };

        let mut tried_slots = HashSet::new();

        for sequence_number in available_sequence {
            let message = {
                let message_result = self
                    .stream
                    .get_raw_message(sequence_number)
                    .await
                    .map_err(|e| anyhow!(e));

                // As the message queue updates all the time, we may well discover a sequence number is
                // long gone. They dissappear and appear at a rate of thousands per second.
                match message_result {
                    Ok(message) => message,
                    Err(e) => {
                        if e.to_string().contains("10037") {
                            continue;
                        }
                        return Err(e);
                    }
                }
            };

            let subject = message.subject;
            let slot = subject
                .strip_prefix("slot.")
                .ok_or(anyhow!(
                    "expected subject of message on {STREAM_NAME} stream to start with 'slot.'"
                ))?
                .parse::<u32>()
                .context(format!(
                    "expected NUM, in slot.NUM, in message subject to be u32"
                ))?;
            if tried_slots.contains(&slot) {
                trace!(%slot, sequence_number, "already tried this slot, skipping");
                continue;
            }

            // Check if the slot is old enough to be considered available.
            // We don't want to lock a slot that is still being written to.
            let slot_date_time = Slot(slot).date_time();
            let now = Utc::now();
            let slot_age = now - slot_date_time;
            const SLOT_MIN_AGE: i64 = 32;
            if slot_age < chrono::Duration::seconds(SLOT_MIN_AGE) {
                trace!(%slot, sequence_number, "slot on {STREAM_NAME} stream is too young to be considered available");
                tried_slots.insert(slot);
                continue;
            }

            trace!(%slot, sequence_number, "found candidate message for available slot on {STREAM_NAME} stream");

            // Check if the slot is available.
            // The only real test for whether a slot is available is whether we can grab the lock.
            // Because kv_store will simply overwrite in the event two consumers try to grab the
            // same lock, we should check before, whether there is a lock, if there's not put ours,
            // but then still check after, it is our ID which made it in.
            let key = Self::make_lock_key(slot);
            trace!(%slot, sequence_number, "checking if slot on {STREAM_NAME} stream is locked");
            let existing_key = self.kv_store.get(&key).await?;
            if existing_key.is_some() {
                // There is a lock on this slot, we can't use it.
                trace!(%slot, sequence_number, "slot on {STREAM_NAME} stream is locked");
                continue;
            }

            // Slot is not locked yet, try to lock it.
            let id_bytes: Bytes = (*CONSUMER_ID).clone().into();
            self.kv_store.put(&key, id_bytes.clone()).await?;

            // Check we locked it and not some other instance.
            let stored_id = self.kv_store.get(&key).await?;
            let is_our_id = match stored_id {
                Some(stored_id) => stored_id == id_bytes,
                None => false,
            };

            if is_our_id {
                // We got the lock, we're done.
                debug!(%slot, sequence_number, "got lock on slot on {STREAM_NAME} stream");
                return Ok(Some(slot));
            }

            // Some other instance locked it.

            tried_slots.insert(slot);

            // We continue to the next sequence_number.
        }

        Ok(None)
    }

    pub async fn release_slot_lock(&self, slot: u32) -> Result<()> {
        let key = Self::make_lock_key(slot);
        self.kv_store.delete(&key).await?;
        Ok(())
    }
}

fn bundle_path(slot: Slot) -> Path {
    let slot_date_time = slot.date_time();
    let year = slot_date_time.year();
    let month = slot_date_time.month();
    let day = slot_date_time.day();
    let hour = slot_date_time.hour();
    let minute = slot_date_time.minute();
    let slot = slot.to_string();
    let path_string = format!("{year}/{month:02}/{day:02}/{hour:02}/{minute:02}/{slot}.ndjson.gz");
    Path::from(path_string)
}

// TODO: an archiver may crash for any reason, whilst holding a lock. We periodically clean up
// locks more than 4 min old.

#[derive(Debug, Clone)]
pub struct AppState {
    message_health: Arc<MessageConsumerHealth>,
    nats_health: NatsHealth,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("starting payload archiver");

    let shutdown_notify = Arc::new(Notify::new());

    if tracing::enabled!(Level::TRACE) {
        let handle = tokio::spawn(async move {
            trace_memory::report_memory_periodically().await;
        });
        let shutdown_notify = shutdown_notify.clone();
        tokio::spawn(async move {
            shutdown_notify.notified().await;
            handle.abort();
        });
    }

    let env_config = &env::ENV_CONFIG;

    let object_store = object_stores::build_env_based_store(env_config)?;

    pub const STREAM_NAME: &str = "payload-archive";

    debug!("connecting to NATS");
    let nats_uri = std::env::var("NATS_URI").unwrap_or_else(|_| "localhost:4222".to_string());
    let nats_client = async_nats::connect(nats_uri)
        .await
        .context("connecting to NATS")?;
    let jetstream = jetstream::new(nats_client.clone());
    let stream = jetstream.get_stream(STREAM_NAME).await?;

    let mut available_slot_provider =
        AvailableSlotProvider::new(&jetstream, stream.clone()).await?;

    let mut held_locks = HashSet::new();

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        info!("received shutdown signal");
        r.store(false, Ordering::Relaxed);
    })
    .expect("Error setting Ctrl-C handler");

    loop {
        if !running.load(Ordering::Relaxed) {
            break;
        }

        let slot = {
            let slot_option = available_slot_provider.lock_next_available_slot().await?;

            match slot_option {
                None => {
                    trace!("no available slots, waiting");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
                Some(slot) => slot,
            }
        };

        held_locks.insert(slot);

        debug!(slot, "got an available slot");

        // Store it!
        let bytes_gz = vec![];
        let mut writer = GzEncoder::new(bytes_gz, Compression::default());
        debug!(slot, "created writer");
        let consumer = stream
            .create_consumer(pull::Config {
                ack_wait: std::time::Duration::from_secs(60),
                filter_subject: format!("slot.{}", slot),
                max_ack_pending: MAX_PAYLOADS_PER_BUNDLE * 2,
                ..Default::default()
            })
            .await?;
        debug!(slot, "got consumer");
        // TODO: release lock in the event of an error.
        let mut messages = consumer.messages().await?;
        let mut ackers = vec![];
        loop {
            // Write messages to the writer as long as we see them show up given a 4s timeout.
            // As we only process slots which are SLOT_MIN_AGE seconds the past, this
            // should be plenty.
            const MAX_MESSAGE_WAIT: Duration = Duration::from_secs(4);
            let timeout = tokio::time::sleep(MAX_MESSAGE_WAIT);
            select! {
                _ = timeout => {
                    // We haven't seen any messages for 4s, we're done.
                    dbg!("hit 4s timeout");
                    break;
                }
                message = messages.try_next() => {
                    let message = message?;
                    match message {
                        None => {
                            // We've reached the end of the stream, we're done.
                            break;
                        }
                        Some(message) => {
                            let instant = Instant::now();
                            let (message, acker) = message.split();
                            writer.write_all(&message.payload)?;
                            writer.write_all(b"\n")?;
                            ackers.push(acker);
                            debug!(took_ms = instant.elapsed().as_millis(), "wrote message");
                        }
                    }
                }
            }
        }

        let bytes_ndjson_gz = writer.finish()?;
        let path = bundle_path(Slot(slot));
        object_store
            .put(&path, bytes_ndjson_gz.into())
            .timed("put-to-object-store")
            .await?;
        info!(%slot, "stored bundle");

        for acker in ackers {
            acker.ack().await.map_err(|e| anyhow!(e))?;
        }

        available_slot_provider.release_slot_lock(slot).await?;

        held_locks.remove(&slot);

        debug!(slot, "released slot lock");
    }

    for slot in held_locks {
        available_slot_provider.release_slot_lock(slot).await?;
    }

    // let consumer = stream
    //     .get_or_create_consumer(
    //         CONSUMER_NAME,
    //         pull::Config {
    //             durable_name: Some(CONSUMER_NAME.to_string()),
    //             max_ack_pending: MAX_ACK_PENDING,
    //             ack_wait: std::time::Duration::from_secs(60),
    //             ..Default::default()
    //         },
    //     )
    //     .await?;

    // let nats_health = NatsHealth::new(nats_client.clone());
    // let message_health = Arc::new(MessageConsumerHealth::new());

    // Ackable payload queue
    // let (ackable_payload_tx, ackable_payload_rx) = mpsc::channel(MAX_ACKABLE_PAYLOAD_QUEUE_SIZE);
    // let message_consumer = MessageConsumer::new(
    //     message_health.clone(),
    //     nats_client,
    //     ackable_payload_tx,
    //     shutdown_notify.clone(),
    // )
    // .await;

    // // Bundle queue
    // let (bundle_tx, bundle_rx) = mpsc::channel(MAX_BUNDLE_QUEUE_SIZE);
    // let bundle_aggregator = Arc::new(BundleAggregator::new(
    //     ackable_payload_rx,
    //     bundle_tx,
    //     shutdown_notify.clone(),
    // ));
    // let bundle_aggregator_clone = bundle_aggregator.clone();

    // let object_store = object_stores::build_env_based_store(env_config)?;
    // let bundle_shipper = BundleShipper::new(bundle_rx, object_store);

    // let message_consumer_thread = tokio::spawn(async move { message_consumer.run().await });

    // let bundle_aggregator_complete_bundle_check_thread = tokio::spawn(async move {
    //     bundle_aggregator.run_complete_bundle_check().await;
    // });

    // let bundle_aggregator_consume_ackable_payloads_thread = tokio::spawn(async move {
    //     bundle_aggregator_clone.run_consume_ackable_payloads().await;
    // });

    // let bundle_shipper_thread = tokio::spawn({
    //     let shutdown_notify = shutdown_notify.clone();
    //     async move {
    //         bundle_shipper.run(&shutdown_notify).await;
    //     }
    // });

    // let app_state = AppState {
    //     nats_health,
    //     message_health,
    // };

    // let server_thread = tokio::spawn({
    //     let shutdown_notify = shutdown_notify.clone();
    //     async move {
    //         server::serve(app_state, &shutdown_notify).await;
    //     }
    // });

    // try_join!(
    // bundle_aggregator_complete_bundle_check_thread,
    // bundle_aggregator_consume_ackable_payloads_thread,
    // bundle_shipper_thread,
    // message_consumer_thread,
    // server_thread,
    // )?;

    Ok(())
}
