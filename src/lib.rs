pub mod env;
pub mod units;

use serde::{Deserialize, Serialize};
use units::Slot;

type JsonValue = serde_json::Value;

// Archive messages carry a slot number, and a execution payload. The slot number is used to make
// storage simple. Bundle messages with the same slot number together. The execution payload is
// what we receive from builders. This also means we do not handle re-orgs, but store all data.
#[derive(Debug, Serialize, Deserialize)]
pub struct ArchivePayload {
    // To avoid unneccesary work we do not deserialize the payload.
    /// This is the execution payload we'd like to store.
    pub payload: JsonValue,
    pub slot: Slot,
}
