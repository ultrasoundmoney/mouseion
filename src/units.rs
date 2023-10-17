use std::{
    fmt::{self, Display, Formatter},
    num::ParseIntError,
    str::FromStr,
};

use chrono::{DateTime, Datelike, Timelike, Utc};
use lazy_static::lazy_static;
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::env::{Network, ENV_CONFIG};

// Beacon chain slots are defined as 12 second periods starting from genesis. With u32 our program
// would overflow when the slot number passes 2_147_483_647. i32::MAX * 12 seconds = ~817 years.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialOrd, PartialEq, Serialize)]
pub struct Slot(pub i32);

lazy_static! {
    static ref GENESIS_TIMESTAMP: DateTime<Utc> = {
        match ENV_CONFIG.network {
            Network::Mainnet => "2020-12-01T12:00:23Z".parse().unwrap(),
            Network::Goerli => "2021-03-23T14:00:00Z".parse().unwrap()
        }
    };
    // We can't know whether a builder will submit a block submission for a slot late. Although builders
    // should submit bids well before t+2 of a slot, proposers may be late. We allow a maximum t+12
    // seconds, _plus_ a buffer just to be extra sure we've received all block submission. The value below
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

impl FromStr for Slot {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<i32>().map(Self)
    }
}

impl Slot {
    pub const GENESIS: Self = Self(0);
    pub const SECONDS_PER_SLOT: i32 = 12;

    pub fn date_time(&self) -> DateTime<Utc> {
        self.into()
    }

    pub fn partial_s3_path(&self) -> Path {
        let slot = self.0;
        let slot_date_time = self.date_time();
        let year = slot_date_time.year();
        let month = slot_date_time.month();
        let day = slot_date_time.day();
        let hour = slot_date_time.hour();
        let minute = slot_date_time.minute();

        let path = format!("{year}/{month:02}/{day:02}/{hour:02}/{minute:02}/{slot}");
        debug!(path, "built path from slot");

        Path::from(path)
    }
}
