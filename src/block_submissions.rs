use std::{collections::HashMap, io::Write};

use anyhow::Result;
use bytes::Bytes;
use chrono::{Datelike, Timelike};
use flate2::{write::GzEncoder, Compression};
use fred::{
    prelude::{RedisError, RedisErrorKind},
    types::{FromRedis, MultipleOrderedPairs, RedisKey, RedisMap, RedisValue},
};
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;
use tracing::{debug, instrument};

use crate::units::Slot;

fn deserialize_eligible_at<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let eligible_at: Option<i64> = Option::deserialize(deserializer)?;

    // eligible_at used to be sent as a negative number instead of nil when not available.
    // Until we update our example submissions we handle this special bug case.
    let eligible_at = eligible_at.and_then(|eligible_at| {
        if eligible_at < 0 {
            None
        } else {
            Some(eligible_at as u64)
        }
    });

    Ok(eligible_at)
}

/// Block submission archive entries.
/// These are block submissions as they came in on the relay, plus some metadata.
#[derive(Deserialize, Serialize)]
pub struct BlockSubmission {
    // Not every block submission becomes eligible, so this field is optional.
    #[serde(deserialize_with = "deserialize_eligible_at")]
    eligible_at: Option<u64>,
    pub payload: serde_json::Value,
    received_at: u64,
    // Optional until builder-api is updated to send it.
    safe_to_propose: Option<bool>,
    // Optional until builder-api is updated to send it.
    sim_optimistic: Option<bool>,
    // Optional until builder-api is updated to send it.
    sim_request_error: Option<String>,
    sim_validation_error: Option<String>,
    // Optional until builder-api is updated to send it.
    sim_was_simulated: Option<bool>,
    // A status code is not always available. Both historically, and because builder-api doesn't
    // always provide one.
    status_code: Option<u16>,
}

impl std::fmt::Debug for BlockSubmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state_root = self.state_root();
        f.debug_struct("BlockSubmission")
            .field("eligible_at", &self.eligible_at)
            .field("sim_optimistic", &self.sim_optimistic)
            .field("payload", &format!("<PAYLOAD_JSON:{state_root}>"))
            .field("received_at", &self.received_at)
            .field("safe_to_propose", &self.safe_to_propose)
            .field("sim_request_error", &self.sim_request_error)
            .field("sim_validation_error", &self.sim_validation_error)
            .field("sim_was_simulated", &self.sim_was_simulated)
            .field("status_code", &self.status_code)
            .finish()
    }
}

impl From<BlockSubmission> for MultipleOrderedPairs {
    fn from(entry: BlockSubmission) -> Self {
        let mut pairs: Vec<(RedisKey, RedisValue)> = vec![
            (
                "payload".into(),
                RedisValue::String(serde_json::to_string(&entry.payload).unwrap().into()),
            ),
            (
                "received_at".into(),
                RedisValue::Integer(entry.received_at.try_into().unwrap()),
            ),
        ];

        if let Some(eligible_at) = entry.eligible_at {
            pairs.push((
                "eligible_at".into(),
                RedisValue::Integer(eligible_at as i64),
            ))
        }

        if let Some(status_code) = entry.status_code {
            pairs.push((
                "status_code".into(),
                RedisValue::Integer(status_code.into()),
            ))
        }

        if let Some(sim_optimistic) = entry.sim_optimistic {
            pairs.push(("sim_optimistic".into(), RedisValue::Boolean(sim_optimistic)))
        }

        if let Some(sim_request_error) = entry.sim_request_error {
            pairs.push(("sim_request_error".into(), sim_request_error.into()))
        }

        if let Some(sim_validation_error) = entry.sim_validation_error {
            pairs.push(("sim_validation_error".into(), sim_validation_error.into()))
        }

        if let Some(sim_was_simulated) = entry.sim_was_simulated {
            pairs.push(("sim_was_simulated".into(), sim_was_simulated.into()))
        }

        if let Some(safe_to_propose) = entry.safe_to_propose {
            pairs.push(("safe_to_propose".into(), safe_to_propose.into()))
        }

        pairs.try_into().unwrap()
    }
}

fn into_redis_parse_err(err: impl std::fmt::Display) -> RedisError {
    RedisError::new(RedisErrorKind::Parse, err.to_string())
}

impl FromRedis for BlockSubmission {
    fn from_value(value: RedisValue) -> Result<Self, RedisError> {
        let mut map: HashMap<String, RedisValue> = value.convert()?;

        let eligible_at = map
            .remove("eligible_at")
            .map(|rv| {
                rv.as_u64().ok_or_else(|| {
                    into_redis_parse_err(format!(
                        "failed to parse eligible_at as u64, str: {}",
                        rv.as_bytes_str().unwrap()
                    ))
                })
            })
            .transpose()?;

        let received_at = {
            let rv = map
                .remove("received_at")
                .ok_or_else(|| into_redis_parse_err("expected received_at in block submission"))?;

            rv.as_u64().ok_or_else(|| {
                into_redis_parse_err(format!(
                    "failed to parse received_at as u64, str: {}",
                    rv.as_bytes_str().unwrap()
                ))
            })?
        };

        let payload = {
            let str = map
                .remove("payload")
                .ok_or_else(|| into_redis_parse_err("expected payload in block submission"))?
                .as_bytes_str()
                .ok_or_else(|| into_redis_parse_err("failed to parse payload as bytes str"))?;
            // We could implement custom Deserialize for this to avoid parsing the JSON here, we
            // don't do anything with it besides Serialize it later.
            serde_json::from_str(&str).map_err(|e| {
                into_redis_parse_err(format!(
                    "failed to parse payload bytes as serde_json Value, {}",
                    e
                ))
            })?
        };

        let status_code = map
            .remove("status_code")
            .map(|rv| {
                rv.as_u64()
                    .ok_or_else(|| {
                        into_redis_parse_err(format!(
                            "failed to parse status_code as u64, str: {}",
                            rv.as_bytes_str().unwrap()
                        ))
                    })
                    .map(|u| u as u16)
            })
            .transpose()?;

        let sim_optimistic = map
            .remove("sim_optimistic")
            .map(|rv| {
                rv.as_bool().ok_or_else(|| {
                    into_redis_parse_err(format!(
                        "failed to parse sim_optimistic as bool, str: {}",
                        rv.as_bytes_str().unwrap()
                    ))
                })
            })
            .transpose()?;

        let sim_request_error = map
            .remove("sim_request_error")
            .map(|rv| {
                rv.as_string()
                    .ok_or_else(|| {
                        into_redis_parse_err(format!(
                            "failed to parse sim_request_error as string, str: {}",
                            rv.as_bytes_str().unwrap()
                        ))
                    })
                    .map(|s| s.to_string())
            })
            .transpose()?;

        let sim_validation_error = map
            .remove("sim_validation_error")
            .map(|rv| {
                rv.as_string()
                    .ok_or_else(|| {
                        into_redis_parse_err(format!(
                            "failed to parse sim_validation_error as string, str: {}",
                            rv.as_bytes_str().unwrap()
                        ))
                    })
                    .map(|s| s.to_string())
            })
            .transpose()?;

        let sim_was_simulated = map
            .remove("sim_was_simulated")
            .map(|rv| {
                rv.as_bool().ok_or_else(|| {
                    into_redis_parse_err(format!(
                        "failed to parse sim_was_simulated as bool, str: {}",
                        rv.as_bytes_str().unwrap()
                    ))
                })
            })
            .transpose()?;

        let safe_to_propose = map
            .remove("safe_to_propose")
            .map(|rv| {
                rv.as_bool().ok_or_else(|| {
                    into_redis_parse_err(format!(
                        "failed to parse safe_to_propose as bool, str: {}",
                        rv.as_bytes_str().unwrap()
                    ))
                })
            })
            .transpose()?;

        Ok(Self {
            eligible_at,
            sim_optimistic,
            payload,
            received_at,
            safe_to_propose,
            sim_request_error,
            sim_validation_error,
            sim_was_simulated,
            status_code,
        })
    }
}

impl From<BlockSubmission> for RedisValue {
    fn from(entry: BlockSubmission) -> Self {
        #![allow(clippy::mutable_key_type)]
        let mut map: HashMap<RedisKey, RedisValue> = HashMap::new();
        if let Some(eligible_at) = entry.eligible_at {
            map.insert(
                "eligible_at".into(),
                RedisValue::Integer(eligible_at as i64),
            );
        }
        map.insert(
            "payload".into(),
            RedisValue::String(entry.payload.to_string().into()),
        );
        map.insert(
            "received_at".into(),
            RedisValue::Integer(entry.received_at as i64),
        );
        if let Some(status_code) = entry.status_code {
            map.insert(
                "status_code".into(),
                RedisValue::Integer(status_code.into()),
            );
        }
        if let Some(sim_optimistic) = entry.sim_optimistic {
            map.insert("sim_optimistic".into(), RedisValue::Boolean(sim_optimistic));
        }
        if let Some(sim_request_error) = entry.sim_request_error {
            map.insert("sim_request_error".into(), sim_request_error.into());
        }
        if let Some(sim_validation_error) = entry.sim_validation_error {
            map.insert("sim_validation_error".into(), sim_validation_error.into());
        }
        if let Some(sim_was_simulated) = entry.sim_was_simulated {
            map.insert("sim_was_simulated".into(), sim_was_simulated.into());
        }
        if let Some(safe_to_propose) = entry.safe_to_propose {
            map.insert("safe_to_propose".into(), safe_to_propose.into());
        }
        RedisMap::try_from(map).map(RedisValue::Map).unwrap()
    }
}

impl Default for BlockSubmission {
    fn default() -> Self {
        Self {
            eligible_at: None,
            sim_optimistic: None,
            payload: serde_json::Value::Null,
            received_at: 0,
            safe_to_propose: None,
            sim_request_error: None,
            sim_validation_error: None,
            sim_was_simulated: None,
            status_code: None,
        }
    }
}

impl BlockSubmission {
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
            Ok::<_, RedisError>(json_gz)
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
        let mut submission = BlockSubmission::default();
        let payload =
            json!({"message": {"slot": "42"}, "execution_payload": {"state_root": "some_root"}});
        submission.eligible_at = Some(100);
        submission.payload = payload.clone();
        submission.received_at = 200;
        submission.status_code = Some(400);

        assert_eq!(submission.eligible_at, Some(100));
        assert_eq!(submission.payload, payload);
        assert_eq!(submission.received_at, 200);
        assert_eq!(submission.status_code, Some(400));
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
        assert_eq!(submission.eligible_at, Some(100));
        assert_eq!(submission.received_at, 200);
        assert_eq!(submission.status_code, Some(400));
    }
}
