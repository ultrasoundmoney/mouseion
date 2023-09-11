mod block_submissions;
pub mod env;
pub mod log;
pub mod units;

pub use block_submissions::BlockSubmission;

pub type JsonValue = serde_json::Value;

pub const STREAM_NAME: &str = "block-submission-archive";
