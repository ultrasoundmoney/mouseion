use std::sync::Arc;

use anyhow::{Context, Result};
use block_submission_archiver::env::ENV_CONFIG;
use futures::{
    channel::mpsc::{Receiver, Sender},
    SinkExt, StreamExt, TryStreamExt,
};
use object_store as object_store_lib;
use object_store_lib::{
    aws::{AmazonS3, AmazonS3Builder},
    local::LocalFileSystem,
    ObjectStore,
};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{debug, error, info};

use crate::compression::IdBlockSubmissionCompressed;

const MAX_CONCURRENCY: usize = 8;

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

async fn store_submissions(
    compressed_submissions_rx: Receiver<IdBlockSubmissionCompressed>,
    stored_submissions_tx: Sender<String>,
) -> Result<()> {
    let object_store = build_env_based_store()?;

    compressed_submissions_rx
        .map(Ok)
        .try_for_each_concurrent(MAX_CONCURRENCY, |(id, block_submission, json_gz_bytes)| {
            let object_store = &object_store;
            let mut stored_submissions_tx = stored_submissions_tx.clone();

            async move {
                let store_result = {
                    let result = object_store
                        .put(&block_submission.bundle_path(), json_gz_bytes)
                        .await
                        .context("failed to store block submission");

                    // Sometimes archivers manage to grab and attempt to store the same message. As
                    // the operations are idempotent, we simply skip 409s.
                    match result {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            if format!("{:?}", &e).contains("OperationAborted") {
                                debug!(
                                    state_root = block_submission.state_root(),
                                    "hit 409 - conflict when storing bundle, ignoring"
                                );
                                Ok(())
                            } else {
                                Err(e)
                            }
                        }
                    }
                };

                if store_result.is_ok() {
                    debug!(
                        state_root = block_submission.state_root(),
                        "stored block submission"
                    );
                    stored_submissions_tx.send(id).await?;
                }

                store_result
            }
        })
        .await
}

pub fn run_store_submissions_thread(
    compressed_submissions_rx: Receiver<IdBlockSubmissionCompressed>,
    stored_submissions_tx: Sender<String>,
    shutdown_notify: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        match store_submissions(compressed_submissions_rx, stored_submissions_tx).await {
            Ok(_) => info!("no more submissions to store, thread exiting"),
            Err(e) => {
                error!("failed to store submissions: {:?}", e);
                shutdown_notify.notify_waiters();
            }
        }
    })
}