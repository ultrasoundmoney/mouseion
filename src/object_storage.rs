use anyhow::Result;

use object_store::aws::{AmazonS3, AmazonS3Builder};

use crate::env::{self, Env};

pub fn build_ovh_object_store() -> Result<AmazonS3> {
    let bucket_name = {
        let env = env::get_env();
        if env == Env::Prod {
            "execution-payload-archive"
        } else {
            "execution-payload-archive-dev"
        }
    };

    let object_store = AmazonS3Builder::new()
        .with_endpoint("https://s3.rbx.io.cloud.ovh.net/")
        .with_region("rbx")
        .with_bucket_name(bucket_name)
        .with_secret_access_key(env::get_env_var_unsafe("S3_SECRET_ACCESS_KEY"))
        .with_access_key_id("3a7f56c872164eeb9ea200823ad7b403")
        .build()?;

    Ok(object_store)
}
