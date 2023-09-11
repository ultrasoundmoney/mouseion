use std::{collections::HashMap, io::Write};

use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::{Datelike, Timelike};
use flate2::{write::GzEncoder, Compression};
use fred::{
    prelude::{RedisError, RedisErrorKind},
    types::{FromRedis, MultipleOrderedPairs, RedisValue},
};
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;
use tracing::{debug, instrument};

use crate::units::Slot;

/// Block submission archive entries.
/// These are block submissions as they came in on the relay, plus some metadata.
#[derive(Deserialize, Serialize)]
pub struct BlockSubmission {
    eligible_at: i64,
    payload: serde_json::Value,
    received_at: u64,
    status_code: Option<u16>,
}

impl std::fmt::Debug for BlockSubmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state_root = self.state_root();
        f.debug_struct("BlockSubmission")
            .field("eligible_at", &self.eligible_at)
            .field("payload", &format!("<PAYLOAD_JSON:{state_root}>"))
            .field("received_at", &self.received_at)
            .finish()
    }
}

impl From<BlockSubmission> for MultipleOrderedPairs {
    fn from(entry: BlockSubmission) -> Self {
        let pairs: Vec<(String, String)> = vec![
            ("eligible_at".into(), entry.eligible_at.to_string()),
            ("payload".into(), entry.payload.to_string()),
            ("received_at".into(), entry.received_at.to_string()),
        ];
        pairs.try_into().unwrap()
    }
}

impl FromRedis for BlockSubmission {
    fn from_value(value: RedisValue) -> Result<Self, RedisError> {
        let mut map: HashMap<String, Bytes> = value.convert()?;
        let eligible_at = {
            let bytes = map
                .remove("eligible_at")
                .expect("expect eligible_at in block submission")
                .to_vec();
            let str = String::from_utf8(bytes)?;
            str.parse::<i64>()?
        };
        let received_at = {
            let bytes = map
                .remove("received_at")
                .expect("expect received_at in block submission")
                .to_vec();
            let str = String::from_utf8(bytes)?;
            str.parse::<u64>()?
        };
        let payload = {
            let bytes = map
                .remove("payload")
                .expect("expect payload in block submission")
                .to_vec();
            // We could implement custom Deserialize for this to avoid parsing the JSON here, we
            // don't do anything with it besides Serialize it later.
            serde_json::from_slice(&bytes)
                .context("failed to parse block submission payload as JSON")
                .map_err(|err| RedisError::new(RedisErrorKind::Parse, err.to_string()))?
        };
        let status_code = {
            let status_code_bytes = map.remove("status_code");
            // Until the builder-api is updated to match we allow this to be missing.
            match status_code_bytes {
                None => None,
                Some(bytes) => {
                    let str = String::from_utf8(bytes.to_vec())?;
                    let status_code = str.parse::<u16>()?;
                    Some(status_code)
                }
            }
        };
        Ok(Self {
            eligible_at,
            payload,
            received_at,
            status_code,
        })
    }
}

impl BlockSubmission {
    pub fn new(
        eligible_at: i64,
        payload: serde_json::Value,
        received_at: u64,
        status_code: u16,
    ) -> Self {
        Self {
            eligible_at,
            payload,
            received_at,
            status_code: Some(status_code),
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

    #[instrument(skip(self), fields(slot = %self.slot(), state_root = %self.state_root()))]
    pub async fn compress(&self) -> Result<Bytes> {
        let slot = self.slot();
        let state_root = self.state_root();

        let json_str = serde_json::to_string(&self)?;
        let json_size_kb = json_str.len() / 1000;

        let json_gz: Bytes = spawn_blocking(move || {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(json_str.as_bytes())?;
            let json_gz = encoder.finish()?.into();
            anyhow::Ok(json_gz)
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
            "compressed block submission"
        );

        Ok(json_gz)
    }

    pub fn slot(&self) -> Slot {
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use fred::types::{RedisMap, RedisValue};
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn create_block_submission() {
        let payload =
            json!({"message": {"slot": "42"}, "execution_payload": {"state_root": "some_root"}});
        let submission = BlockSubmission::new(100, payload.clone(), 200, 400);

        assert_eq!(submission.eligible_at, 100);
        assert_eq!(submission.payload, payload);
        assert_eq!(submission.received_at, 200);
        assert_eq!(submission.status_code, Some(400));
    }

    // This test depends on the environment, some tests break the environment, if it runs at an
    // unlucky time it will fail.
    #[test]
    fn test_bundle_path_generation() {
        // This test unfortunately depends on the ENV, which means we need a fully formed ENV
        // available.
        if std::env::var("REDIS_URI").is_err() {
            std::env::set_var("REDIS_URI", "dummy_redis_uri")
        }
        let payload =
            json!({"message": {"slot": "42"}, "execution_payload": {"state_root": "some_root"}});
        let submission = BlockSubmission::new(100, payload, 200, 400);

        let path = submission.bundle_path();
        assert_eq!(path.to_string(), "2020/12/01/12/08/42/some_root.json.gz");
    }

    #[tokio::test]
    async fn test_block_submission_compression() {
        let payload =
            json!({"message": {"slot": "42"}, "execution_payload": {"state_root": "some_root"}});
        let submission = BlockSubmission::new(100, payload, 200, 400);

        let compressed_result = submission.compress().await;
        assert!(compressed_result.is_ok());
    }

    #[test]
    fn test_block_submission_from_redis() {
        let mut map = HashMap::new();
        map.insert("eligible_at".to_string(), Bytes::from("100"));
        map.insert("payload".to_string(), Bytes::from("{\"message\": {\"slot\": \"42\"}, \"execution_payload\": {\"state_root\": \"some_root\"}}"));
        map.insert("received_at".to_string(), Bytes::from("200"));
        map.insert("status_code".to_string(), Bytes::from("400"));

        let mut redis_map = RedisMap::new();
        redis_map.insert("eligible_at".into(), RedisValue::String("100".into()));
        redis_map.insert(
            "payload".into(),
            RedisValue::String(
                "{\"message\": {\"slot\": \"42\"}, \"execution_payload\": {\"state_root\": \"some_root\"}}"
                    .into(),
            ),
        );
        redis_map.insert("received_at".into(), RedisValue::String("200".into()));
        redis_map.insert("status_code".into(), RedisValue::String("400".into()));
        let value: RedisValue = RedisValue::Map(redis_map);
        let result = BlockSubmission::from_value(value);

        assert!(result.is_ok());
        let submission = result.unwrap();
        assert_eq!(submission.eligible_at, 100);
        assert_eq!(submission.received_at, 200);
        assert_eq!(submission.status_code, Some(400));
    }
}
