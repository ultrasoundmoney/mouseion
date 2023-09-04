use anyhow::Result;
use block_submission_archiver::env::ENV_CONFIG;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    local::LocalFileSystem,
    ObjectStore,
};
use tracing::info;

fn build_local_file_store() -> Result<LocalFileSystem> {
    let object_store = LocalFileSystem::new_with_prefix("/tmp/")?;
    Ok(object_store)
}

fn build_s3_store() -> Result<AmazonS3> {
    let s3_bucket = &ENV_CONFIG.s3_bucket;
    let s3_store = AmazonS3Builder::from_env()
        .with_bucket_name(s3_bucket)
        .build()?;
    Ok(s3_store)
}

pub fn build_env_based_store() -> Result<Box<dyn ObjectStore>> {
    if ENV_CONFIG.use_local_store {
        info!("using local file store");
        let store = build_local_file_store()?;
        Ok(Box::new(store))
    } else {
        // We can't read directly from the s3_store so mimic what it does. No need to blow up if we
        // fail.
        let endpoint = std::env::var("AWS_ENDPOINT").unwrap_or("UNKNOWN".to_string());
        let s3_bucket = &ENV_CONFIG.s3_bucket;
        info!(endpoint, s3_bucket, "using S3 store");
        let store = build_s3_store()?;
        Ok(Box::new(store))
    }
}
