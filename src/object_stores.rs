use anyhow::Result;
use block_submission_archiver::env::ENV_CONFIG;
use object_store::{aws::AmazonS3Builder, local::LocalFileSystem, ObjectStore};
use tracing::info;

use crate::env::EnvConfig;

fn build_local_file_store() -> Result<LocalFileSystem> {
    let object_store = LocalFileSystem::new_with_prefix("/tmp/")?;
    Ok(object_store)
}

pub fn build_env_based_store(env_config: &EnvConfig) -> Result<Box<dyn ObjectStore>> {
    if env_config.use_local_store {
        info!("using local file store");
        return Ok(Box::new(build_local_file_store()?));
    }

    let s3_bucket = &ENV_CONFIG.s3_bucket;
    let s3_store = AmazonS3Builder::from_env()
        .with_bucket_name(s3_bucket)
        .build()?;

    // We can't read directly from the s3_store so mimic what it does. No need to blow up if we
    // fail.
    let endpoint = std::env::var("AWS_ENDPOINT").unwrap_or("UNKNOWN".to_string());
    info!(endpoint, s3_bucket, "using S3 store");

    Ok(Box::new(s3_store))
}
