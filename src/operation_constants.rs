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
//! bundles may be 300MiB in size, this is too many. To put a cap on this we make the relatively
//! safe assumption that there will be no more messages coming for the oldest bundle if
//! MAX_INCOMPLETE_BUNDLES - 1 newer bundles are already being formed.

use std::time::Duration;

use lazy_static::lazy_static;
use payload_archiver::units::Slot;

/// The minimum time we allow for a bundle to come together. Considering some bundles are several GiB in
/// size. We need a while to collect all payloads. Shipping the oldest when at the
/// MAX_INCOMPLETE_BUNDLES limit will keep things moving.
pub const BUNDLE_MIN_AGE: Duration = Duration::from_secs(32);
/// The time we allow for a bundle to come together after a slot has finished.
pub const BUNDLE_MAX_AGE_BUFFER: Duration = Duration::from_secs(8);
lazy_static! {
    /// Maximum age of a slot before we consider a bundle complete.
    pub static ref BUNDLE_SLOT_MAX_AGE: chrono::Duration =
        chrono::Duration::seconds(Slot::SECONDS_PER_SLOT.try_into().unwrap()) + chrono::Duration::from_std(BUNDLE_MAX_AGE_BUFFER).unwrap();
}

const MAX_PAYLOADS_PER_BUNDLE: i64 = 2100;
// Should be set high enough to allow it to be highly unlikely a payload for the oldest bundle
// arrives when having this many incomplete bundles.
pub const MAX_INCOMPLETE_BUNDLES: usize = 4;
pub const MAX_ACK_PENDING: i64 = MAX_INCOMPLETE_BUNDLES as i64 * MAX_PAYLOADS_PER_BUNDLE;
