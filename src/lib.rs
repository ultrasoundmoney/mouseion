pub mod env;
pub mod units;

use std::{collections::HashMap, io::Write};

use anyhow::{Error, Result};
use bytes::Bytes;
use chrono::{Datelike, Timelike};
use flate2::{write::GzEncoder, Compression};
use fred::types::MultipleOrderedPairs;
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;
use tracing::debug;
use units::Slot;

pub type JsonValue = serde_json::Value;

pub const STREAM_NAME: &str = "block-submission-archive";

/// Block submission archive entries.
/// These are block submissions as they came in on the relay, plus some metadata.
#[derive(Debug, Deserialize, Serialize)]
pub struct ArchiveEntry {
    eligible_at: u64,
    payload: serde_json::Value,
    received_at: u64,
}

impl From<HashMap<String, String>> for ArchiveEntry {
    fn from(mut map: HashMap<String, String>) -> Self {
        let eligible_at: u64 = map.remove("eligible_at").unwrap().parse().unwrap();
        let payload: JsonValue = map.remove("payload").unwrap().parse().unwrap();
        let received_at: u64 = map.remove("received_at").unwrap().parse().unwrap();
        Self {
            eligible_at,
            payload,
            received_at,
        }
    }
}

impl From<ArchiveEntry> for MultipleOrderedPairs {
    fn from(entry: ArchiveEntry) -> Self {
        let pairs: Vec<(String, String)> = vec![
            ("eligible_at".into(), entry.eligible_at.to_string()),
            ("payload".into(), entry.payload.to_string()),
            ("received_at".into(), entry.received_at.to_string()),
        ];
        pairs.try_into().unwrap()
    }
}

impl ArchiveEntry {
    pub fn new(eligible_at: u64, payload: serde_json::Value, received_at: u64) -> Self {
        Self {
            eligible_at,
            payload,
            received_at,
        }
    }

    pub fn bundle_path(&self) -> Path {
        let state_root = self.state_root();

        let slot = self.slot();
        let slot_date_time = slot.date_time();
        let year = slot_date_time.year();
        let month = slot_date_time.month();
        let day = slot_date_time.day();
        let hour = slot_date_time.hour();
        let minute = slot_date_time.minute();

        let path_string =
            format!("{year}/{month:02}/{day:02}/{hour:02}/{minute:02}/{slot}/{state_root}.json.gz");
        Path::from(path_string)
    }

    pub async fn compress(&self) -> Result<Bytes> {
        let slot = self.slot();
        let state_root = self.state_root();

        let json_str = serde_json::to_string(&self)?;
        let json_size_kb = json_str.len() / 1000;

        let json_gz: Bytes = spawn_blocking(move || {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(json_str.as_bytes())?;
            let json_gz = encoder.finish()?.into();
            Ok::<_, Error>(json_gz)
        })
        .await??;
        let json_gz_size_kb = json_gz.len() / 1000;

        let compression_ratio_truncated =
            ((json_size_kb as f64 / json_gz_size_kb as f64) * 100.0).trunc() / 100.0;

        debug!(
            slot = slot.to_string().as_str(),
            state_root = state_root.as_str(),
            uncompressed_size_kb = json_size_kb,
            compressed_size_kb = json_gz_size_kb,
            compression_ratio = compression_ratio_truncated,
            "compressed block submission archive entry"
        );

        Ok(json_gz)
    }

    fn slot(&self) -> Slot {
        let slot_str = self.payload["message"]["slot"].as_str().unwrap();
        slot_str.parse::<Slot>().unwrap()
    }

    pub fn state_root(&self) -> String {
        self.payload["execution_payload"]["state_root"]
            .as_str()
            .unwrap()
            .to_string()
    }
}
