use anyhow::Result;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    local::LocalFileSystem,
    ObjectStore,
};
use tracing::info;

use crate::env::{self, Env, EnvConfig};

fn build_ovh_store(bucket_name: &str) -> Result<AmazonS3> {
    let object_store = AmazonS3Builder::new()
        .with_endpoint("https://s3.rbx.io.cloud.ovh.net/")
        .with_region("rbx")
        .with_bucket_name(bucket_name)
        .with_secret_access_key(env::get_env_var_unsafe("S3_SECRET_ACCESS_KEY"))
        .with_access_key_id("3a7f56c872164eeb9ea200823ad7b403")
        .build()?;

    Ok(object_store)
}

fn build_local_file_store() -> Result<LocalFileSystem> {
    let object_store = LocalFileSystem::new_with_prefix("/tmp/")?;

    Ok(object_store)
}

pub fn build_env_based_store(env_config: &EnvConfig) -> Result<Box<dyn ObjectStore>> {
    if env_config.use_local_store {
        info!("using local file store");
        return Ok(Box::new(build_local_file_store()?));
    }

    let bucket_name = {
        match env_config.env {
            Env::Prod => "execution-payload-archive-prod",
            Env::Stag => "execution-payload-archive-stag",
            Env::Dev => "execution-payload-archive-dev",
        }
    };

    Ok(Box::new(build_ovh_store(bucket_name)?))
}
