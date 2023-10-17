use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::{
    channel::mpsc::{Receiver, Sender},
    SinkExt, StreamExt, TryStreamExt,
};
use object_store as object_store_lib;
use object_store_lib::{
    aws::{AmazonS3, AmazonS3Builder},
    ObjectStore, RetryConfig,
};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{debug, error, info, instrument};

use crate::{
    compression::IdBlockSubmissionCompressed, env::ENV_CONFIG, performance::BlockCounter,
    BlockSubmission,
};

const MAX_CONCURRENCY: usize = 8;

fn build_s3_store(bucket: &str, endpoint: &str) -> Result<AmazonS3> {
    let s3_store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_endpoint(endpoint)
        .with_retry(RetryConfig {
            retry_timeout: std::time::Duration::from_secs(16),
            ..RetryConfig::default()
        })
        .build()?;
    Ok(s3_store)
}

pub fn build_submissions_store() -> Result<AmazonS3> {
    let endpoint = std::env::var("AWS_ENDPOINT").unwrap();
    let bucket = &ENV_CONFIG.submissions_bucket;
    info!(endpoint, bucket, "using S3 store");
    let store = build_s3_store(bucket, &endpoint)?;
    Ok(store)
}

pub fn build_bundles_store() -> Result<AmazonS3> {
    let endpoint = std::env::var("AWS_BUNDLES_ENDPOINT").unwrap();
    let bucket = &ENV_CONFIG.bundles_bucket;
    info!(endpoint, bucket, "using S3 store");
    let store = build_s3_store(bucket, &endpoint)?;
    Ok(store)
}

#[instrument(skip_all, fields(slot = %block_submission.slot(), block_hash = %block_submission.block_hash()))]
async fn store_submission(
    object_store: &dyn ObjectStore,
    block_submission: &BlockSubmission,
    json_gz_bytes: Bytes,
) -> Result<()> {
    let result = object_store
        .put(&block_submission.bucket_path(), json_gz_bytes)
        .await
        .context("failed to store block submission");

    // Sometimes archivers manage to grab and attempt to store the same message. As
    // the operations are idempotent, we simply skip 409s.
    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            if format!("{:?}", &e).contains("OperationAborted") {
                debug!(
                    block_hash = block_submission.block_hash(),
                    "hit 409 - conflict when storing bundle, ignoring"
                );
                Ok(())
            } else {
                Err(e)
            }
        }
    }
}

async fn store_submissions(
    block_counter: &BlockCounter,
    compressed_submissions_rx: Receiver<IdBlockSubmissionCompressed>,
    stored_submissions_tx: Sender<String>,
) -> Result<()> {
    let object_store = &build_submissions_store()?;

    compressed_submissions_rx
        .map(Ok)
        .try_for_each_concurrent(MAX_CONCURRENCY, |(id, block_submission, json_gz_bytes)| {
            let mut stored_submissions_tx = stored_submissions_tx.clone();
            async move {
                let result = store_submission(object_store, &block_submission, json_gz_bytes).await;

                match result {
                    Ok(_) => {
                        debug!(
                            block_hash = block_submission.block_hash(),
                            "stored block submission"
                        );
                        block_counter.increment();
                        stored_submissions_tx
                            .send(id)
                            .await
                            .context("failed to send stored submission id")
                    }
                    Err(e) => Err(e),
                }
            }
        })
        .await
}

pub fn run_store_submissions_thread(
    block_counter: Arc<BlockCounter>,
    compressed_submissions_rx: Receiver<IdBlockSubmissionCompressed>,
    stored_submissions_tx: Sender<String>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        match store_submissions(
            &block_counter,
            compressed_submissions_rx,
            stored_submissions_tx,
        )
        .await
        {
            Ok(_) => info!("no more submissions to store, thread exiting"),
            Err(e) => {
                error!("failed to store submissions: {:?}", e);
                shutdown_notify.notify_waiters();
            }
        }
    })
}
