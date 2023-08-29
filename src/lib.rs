mod archive_entries;
pub mod env;
pub mod log;
pub mod units;

pub use archive_entries::ArchiveEntry;

pub type JsonValue = serde_json::Value;

pub const STREAM_NAME: &str = "block-submission-archive";
