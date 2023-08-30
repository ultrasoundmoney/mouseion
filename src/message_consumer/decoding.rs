use fred::{
    prelude::{RedisError, RedisErrorKind, RedisResult},
    types::{FromRedis, RedisKey, RedisValue},
};
use payload_archiver::{ArchiveEntry, STREAM_NAME};
use tracing::{debug, trace};

use super::IdArchiveEntryPair;

pub struct XReadGroupResponse(pub Option<Vec<IdArchiveEntryPair>>);

impl FromRedis for XReadGroupResponse {
    fn from_value(value: RedisValue) -> RedisResult<Self> {
        trace!(x_read_group_response_raw = ?value);
        match value {
            RedisValue::Array(streams) => {
                let stream = streams
                    .into_iter()
                    .next()
                    .expect("expect non-empty array of streams in non-nil XREADGROUP response")
                    .into_array();

                let mut iter = stream.into_iter();

                // Index 0 is stream name, index 1 is messages.
                let stream_name = iter.next().expect("expect stream name at index 0");
                assert_eq!(stream_name, RedisValue::String(STREAM_NAME.into()));

                let messages = iter
                    .next()
                    .expect("expect messages at index 1")
                    .into_array();
                let mut id_archive_entry_pairs = Vec::with_capacity(messages.len());

                for value in messages {
                    match value {
                        RedisValue::Array(mut array) => {
                            let id: String = array.remove(0).convert().unwrap();
                            let entry: ArchiveEntry = array.remove(0).convert().unwrap();
                            let id_archive_entry_pair = IdArchiveEntryPair { id, entry };
                            id_archive_entry_pairs.push(id_archive_entry_pair);
                        }
                        _ => Err(RedisError::new(
                            RedisErrorKind::Parse,
                            "expected RedisValue::Array with id and message",
                        ))?,
                    }
                }

                Ok(Self(Some(id_archive_entry_pairs)))
            }
            RedisValue::Null => Ok(Self(None)),
            _ => Err(RedisError::new(
                RedisErrorKind::Parse,
                "expected RedisValue::Array containing streams",
            )),
        }
    }
}

pub struct XAutoClaimResponse(pub String, pub Vec<IdArchiveEntryPair>);

impl FromRedis for XAutoClaimResponse {
    fn from_value(value: RedisValue) -> RedisResult<Self> {
        trace!(x_auto_claim_response_raw = ?value);
        let mut iter = value.into_array().into_iter();
        let next_autoclaim_id = iter
            .next()
            .expect("expect autoclaim_id at index 0")
            .convert()?;
        let messages = iter
            .next()
            .expect("expected message at index 1")
            .into_array();
        let mut id_archive_entry_pairs = Vec::with_capacity(messages.len());
        for message in messages {
            let id_archive_pair = {
                let mut iter = message.into_array().into_iter();
                let id: String = iter.next().expect("expected id at index 0").convert()?;
                let entry: ArchiveEntry =
                    iter.next().expect("expected entry at index 1").convert()?;
                IdArchiveEntryPair { id, entry }
            };
            id_archive_entry_pairs.push(id_archive_pair);
        }
        Ok(Self(next_autoclaim_id, id_archive_entry_pairs))
    }
}

#[derive(Debug)]
pub struct ConsumerInfo {
    pub idle: u64,
    pub name: String,
    pub pending: u64,
}

impl FromRedis for ConsumerInfo {
    fn from_value(value: RedisValue) -> RedisResult<Self> {
        let info = value.into_map()?;

        let idle = info
            .get(&RedisKey::from_static_str("idle"))
            .ok_or_else(|| RedisError::new(RedisErrorKind::Parse, "expected idle field to exist"))?
            .as_u64()
            .ok_or_else(|| {
                RedisError::new(RedisErrorKind::Parse, "expected idle field to be a u64")
            })?;

        let name = info
            .get(&RedisKey::from_static_str("name"))
            .ok_or_else(|| RedisError::new(RedisErrorKind::Parse, "expected name field to exist"))?
            .as_string()
            .ok_or_else(|| {
                RedisError::new(RedisErrorKind::Parse, "expected name field to be a string")
            })?;

        let pending = info
            .get(&RedisKey::from_static_str("pending"))
            .expect("expect pending field")
            .as_u64()
            .ok_or_else(|| {
                RedisError::new(RedisErrorKind::Parse, "expected pending field to be a u64")
            })?;

        Ok(Self {
            idle,
            name,
            pending,
        })
    }
}
