use std::{sync::Arc, time::Duration};

use block_submission_archiver::STREAM_NAME;
use fred::{
    pool::RedisPool,
    prelude::{RedisResult, StreamsInterface},
};
use futures::channel::mpsc::Receiver;
use tokio::{sync::Notify, task::JoinHandle, time::sleep};
use tracing::{debug, error, info, trace};

use crate::redis_consumer::{ACK_SUBMISSIONS_DURATION_MS, GROUP_NAME};

pub fn run_ack_submissions_thread(
    client: RedisPool,
    shutdown_notify: Arc<Notify>,
    mut stored_submissions_rx: Receiver<String>,
) -> JoinHandle<()> {
    tokio::spawn({
        async move {
            // In order to acknowledge efficiently, we take all pending messages from the
            // channel. This means we can't use a `while let Some(msg)` loop to automatically
            // shut down when the channel closes. We also won't receive wake-ups but instead
            // poll the channel every 200ms.
            loop {
                let mut ids = Vec::new();

                while let Ok(id) = stored_submissions_rx.try_next() {
                    match id {
                        Some(id) => {
                            ids.push(id);
                        }
                        None => {
                            info!("no more block submissions to ack, thread exiting");
                            return;
                        }
                    }
                }

                if !ids.is_empty() {
                    let ids_len = ids.len();
                    let result: RedisResult<()> = client.xack(STREAM_NAME, GROUP_NAME, ids).await;

                    match result {
                        Ok(()) => {
                            debug!(count = ids_len, "acked block submissions");
                        }
                        Err(e) => {
                            error!("failed to ack block submission: {:?}", e);
                            shutdown_notify.notify_waiters();
                            break;
                        }
                    }
                } else {
                    trace!("no block submissions to ack");
                }

                trace!(
                    duration_ms = ACK_SUBMISSIONS_DURATION_MS,
                    "completed ack cycle, sleeping"
                );
                sleep(Duration::from_millis(ACK_SUBMISSIONS_DURATION_MS)).await;
            }
        }
    })
}
