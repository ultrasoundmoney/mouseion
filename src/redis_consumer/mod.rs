//! Pulls new block submissions from Redis.
//!
//! ## Technical Notes
//! - The consumer is built for Redis v7, it works with Redis v6 but encounters a subtle issue
//! where pending entries lists become bloated with deleted message IDs under heavy load. Further
//! details for how to implement v6 support can be found in the decoding module. Happy to support
//! and merge any implementation.
mod ack_submissions;
mod decoding;
mod new_submissions;
mod pending_submissions;

pub use ack_submissions::run_ack_submissions_thread;
pub use new_submissions::run_new_submissions_thread;
pub use pending_submissions::run_pending_submissions_thread;

use anyhow::{bail, Result};
use block_submission_archiver::{env::ENV_CONFIG, BlockSubmission, STREAM_NAME};
use fred::{
    pool::RedisPool,
    prelude::{RedisResult, StreamsInterface},
};
use lazy_static::lazy_static;
use nanoid::nanoid;
use tracing::{debug, info, instrument, trace};

use decoding::ConsumerInfo;

// Consumers claim messages from other consumers which have failed to acknowledge their messages
// for longer than this limit.
const MAX_MESSAGE_ACK_DURATION_MS: u64 = 60 * 1000;
// In order to avoid polling we use Redis' BLOCK option. Whenever no messages are available, this
// is the maximum time we'll want Redis to wait for new data before returning an empty response.
const BLOCK_DURATION_MS: u64 = 8000;
// After a consumer has been idle for 8 minutes, we consider it crashed and remove it.
const MAX_CONSUMER_IDLE_MS: u64 = 8 * 60 * 1000;
// Number of messages to pull from Redis at a time.
const MESSAGE_BATCH_SIZE: u64 = 8;
// How often to ack messages. We buffer them for efficiency
const ACK_SUBMISSIONS_DURATION_MS: u64 = 200;
// A consumer group ensures each message is only delivered to one consumer. We use a single
// consumer group for all consumers.
const GROUP_NAME: &str = "default-group";

lazy_static! {
    static ref CONSUMER_ID: String = ENV_CONFIG.pod_name.clone().unwrap_or(nanoid!(4));
    static ref PULL_PENDING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);
}

pub type IdBlockSubmission = (String, BlockSubmission);

// If no consumer group exists, create one, if it already exists, we'll get an error we can
// ignore.
#[instrument(skip(client))]
pub async fn ensure_consumer_group_exists(client: &RedisPool) -> Result<()> {
    let result: RedisResult<()> = client
        .xgroup_create(STREAM_NAME, GROUP_NAME, "0", false)
        .await;

    if let Err(e) = result {
        if e.to_string().contains("BUSYGROUP") {
            // The group already exists. This is fine.
            debug!("consumer group already exists");
        } else {
            // Propagate the original error
            bail!(e);
        }
    }
    Ok(())
}

// When a consumer gets terminated it leaves behind its consumer group entry. This fn cleans up
// dead consumers.
#[instrument(skip(client))]
pub async fn delete_dead_consumers(client: &RedisPool) -> Result<()> {
    let consumers: Vec<ConsumerInfo> = client.xinfo_consumers(STREAM_NAME, GROUP_NAME).await?;

    if consumers.is_empty() {
        // No consumers, nothing to do.
        debug!("no consumers, nothing to clean up");
        return Ok(());
    }

    debug!(count = consumers.len(), "found consumers");

    for consumer in consumers {
        if consumer.pending == 0 && consumer.idle > MAX_CONSUMER_IDLE_MS {
            info!(
                name = consumer.name,
                idle_seconds = consumer.idle / 1000,
                "removing long-idle, likely dead consumer"
            );
            client
                .xgroup_delconsumer(STREAM_NAME, GROUP_NAME, &consumer.name)
                .await?;
        } else {
            trace!(
                name = consumer.name,
                pending = consumer.pending,
                idle = consumer.idle,
                "consumer looks alive, leaving it"
            );
        }
    }

    Ok(())
}
