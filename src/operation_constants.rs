//! Operation Constants
//! This module specifies configuration constants which are closely related and should be considered
//! in unison.
//!
//! ## Message Queue
//! Our message queue delivers messages up to MAX_ACK_PENDING. We acknowledge messages only after
//! bundles have successfully shipped to ensure no data is lost. The average bundle contains about
//! 200 payloads, but some go as high as 1600 payloads. This means we may have
//! max_incomplete_bundles * max_messages_per_bundle messages waiting for acknowledgement.
//!
//! ## Bundle concurrency
//! During normal operation, we receive the first payloads up to twelve seconds in advance of the
//! slot, then allow for additional payloads during the slot, plus a buffer period of 8 seconds.
//! This means a bundle is considered complete after at most 12 + 12 + 8 = 32 seconds. Because
//! slots appear every twelve seconds, we expect to have at most three bundles concurrently from
//! normal operation. However, the service may start and find it has fallen behind, with a backlog
//! of outstanding messages on the queue. This limit is set elsewhere but expected to be in the
//! dozens of slots. Because we can't know how many payloads for a slot are still on-queue if
//! a slot is already in the past, we respect a MIN_BUNDLE_AGE. This means on start, we may consume
//! as many messages as we can within MIN_BUNDLE_AGE. This could easily be thousands. This message
//! count divided by min_bundle_size is the number of bundles we may have in parallel. Considering
//! bundles may be 300MiB in size, this is too many. To put a cap on this we stop payload
//! consumption for bundles at MAX_INCOMPLETE_BUNDLES and let our message_consumer <->
//! bundle_aggregator channel fill up to have it automatically back pressure and stop the
//! message_consumer.

use std::time::Duration;

/// The minimum time we allow for a bundle to come together.
pub const MIN_BUNDLE_AGE: Duration = Duration::from_secs(16);
/// The time we allow for a bundle to come together after a slot has finished.
pub const BUNDLE_MAX_AGE_BUFFER: Duration = Duration::from_secs(8);

const MAX_PAYLOADS_PER_BUNDLE: i64 = 1600;
pub const MAX_INCOMPLETE_BUNDLES: usize = 8;
pub const MAX_ACK_PENDING: i64 = 8 * MAX_PAYLOADS_PER_BUNDLE;
