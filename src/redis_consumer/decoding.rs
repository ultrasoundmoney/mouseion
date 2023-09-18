//! Decodes Redis bytes into Rust types.
use block_submission_archiver::{BlockSubmission, STREAM_NAME};
use fred::{
    prelude::{RedisError, RedisErrorKind, RedisResult},
    types::{FromRedis, RedisKey, RedisValue},
};
use tracing::{debug, trace};

pub struct XReadGroupResponse(pub Option<Vec<(String, BlockSubmission)>>);

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

                let mut id_block_submissions = Vec::with_capacity(messages.len());

                for message in messages {
                    let mut iter = message.into_array().into_iter();
                    let id: String = iter.next().expect("expected index 0 to be id").convert()?;
                    let entry: BlockSubmission = iter
                        .next()
                        .expect("expected index 1 to be block submission")
                        .convert()?;
                    id_block_submissions.push((id, entry));
                }

                Ok(Self(Some(id_block_submissions)))
            }
            RedisValue::Null => Ok(Self(None)),
            _ => Err(RedisError::new(
                RedisErrorKind::Parse,
                "expected array containing streams",
            )),
        }
    }
}

pub struct XAutoClaimResponse(pub String, pub Vec<(String, BlockSubmission)>);

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

        // Messages which are consumed move into the consumer's pending entries list. In rare cases
        // a message on this list is never acknowledged. The consumer may be terminated or crash
        // before it can. Normally this message would be re-delivered to another consumer, in even
        // more rare cases, the message gets deleted before any consumer can.
        //
        // In Redis v6, these messages end up being returned as nil. We skip them, but this
        // means they forever rotate between consumers that fail to acknowledge them. To fix this,
        // Redis v6 would need a more elaborate flow where we use XPENDING to figure out what IDs
        // we'd like to claim, and then use XCLAIM to claim them, then ack any which come back nil.
        // See also: https://github.com/redis/redis/pull/10227. One can use XPENDING to find out
        // which IDs to claim, try to claim them with XCLAIM, and when discovering they are nil,
        // ack them.
        //
        // In Redis v7, these messages are automatically deleted from the pending entries list and
        // their IDs are returned as a third value.
        let mut id_block_submissions = Vec::with_capacity(messages.len());
        for message in messages {
            match message {
                RedisValue::Null => {
                    debug!("received nil message in XAUTOCLAIM response, skipping");
                    continue;
                }
                message => {
                    let id_block_submission = {
                        let mut iter = message.into_array().into_iter();
                        let id: String = iter.next().expect("expected id at index 0").convert()?;
                        let entry: BlockSubmission =
                            iter.next().expect("expected entry at index 1").convert()?;
                        (id, entry)
                    };
                    id_block_submissions.push(id_block_submission);
                }
            }
        }

        Ok(Self(next_autoclaim_id, id_block_submissions))
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
