mod block_submission;
pub mod env;
pub mod log;
pub mod units;

pub use block_submission::BlockSubmission;

pub type JsonValue = serde_json::Value;

pub const STREAM_NAME: &str = "block-submission-archive";
