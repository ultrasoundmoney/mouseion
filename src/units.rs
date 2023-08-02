use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

// Beacon chain slots are defined as 12 second periods starting from genesis. With u32 our program
// would overflow when the slot number passes 2_147_483_647. i32::MAX * 12 seconds = ~817 years.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Slot(pub i32);

lazy_static! {
    static ref GENESIS_TIMESTAMP: DateTime<Utc> = "2020-12-01T12:00:23Z".parse().unwrap();
    // We can't know whether a builder will submit a payload for a slot late. Although builders
    // should submit bids well before t+2 of a slot, proposers may be late. We allow a maximum t+12
    // seconds, _plus_ a buffer just to be extra sure we've received all payloads. The value below
    // is the duration of that buffer.
    static ref OLD_SLOT_BUFFER_DURATION: chrono::Duration = chrono::Duration::seconds(24);
}

impl Display for Slot {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&Slot> for DateTime<Utc> {
    fn from(slot: &Slot) -> Self {
        let seconds = slot.0 as i64 * Slot::SECONDS_PER_SLOT as i64;
        *GENESIS_TIMESTAMP + chrono::Duration::seconds(seconds)
    }
}

impl Slot {
    pub const GENESIS: Self = Self(0);
    pub const SECONDS_PER_SLOT: i32 = 12;

    pub fn date_time(&self) -> DateTime<Utc> {
        self.into()
    }
}
