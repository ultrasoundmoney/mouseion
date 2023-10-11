mod block_submissions;
pub mod compression;
pub mod env;
pub mod health;
pub mod log;
pub mod object_store;
pub mod performance;
pub mod redis_consumer;
pub mod redis_decoding;
pub mod run;
pub mod server;
pub mod units;

pub use block_submissions::BlockSubmission;

pub type JsonValue = serde_json::Value;

pub const STREAM_NAME: &str = "block-submission-archive";
