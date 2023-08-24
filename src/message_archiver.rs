use std::sync::Arc;

use anyhow::Result;
use fred::prelude::RedisClient;
use object_store::ObjectStore;
use tokio::{
    select,
    sync::{mpsc, Notify},
};
use tracing::{debug, error, info};

use crate::{health::MessageConsumerHealth, message_consumer::IdArchiveEntryPair};

pub struct MessageArchiver<OS: ObjectStore> {
    client: RedisClient,
    message_rx: mpsc::Receiver<IdArchiveEntryPair>,
    message_health: Arc<MessageConsumerHealth>,
    object_store: OS,
    shutdown_notify: Arc<Notify>,
}

impl<OS: ObjectStore> MessageArchiver<OS> {
    pub fn new(
        client: RedisClient,
        message_rx: mpsc::Receiver<IdArchiveEntryPair>,
        message_health: Arc<MessageConsumerHealth>,
        object_store: OS,
        shutdown_notify: Arc<Notify>,
    ) -> Self {
        Self {
            client,
            message_rx,
            message_health,
            object_store,
            shutdown_notify,
        }
    }

    async fn archive_messages(&mut self) -> Result<()> {
        while let Some(id_archive_entry_pair) = self.message_rx.recv().await {
            let json_gz_bytes = id_archive_entry_pair.entry.compress().await?;

            self.object_store
                .put(&id_archive_entry_pair.entry.bundle_path(), json_gz_bytes)
                .await?;

            id_archive_entry_pair.ack(&self.client).await?;

            self.message_health.set_last_message_received_now();
        }

        Ok(())
    }

    pub async fn run_archive_messages(&mut self) {
        debug!("processing messages");
        let shutdown_notify = self.shutdown_notify.clone();
        select! {
            _ = shutdown_notify.notified() => {
                debug!("shutting down process messages thread");
            }
            result = self.archive_messages() => {
                match result {
                    Ok(_) => info!("stopped processing messages"),
                    Err(e) => {
                        error!("error while processing messages: {:?}", e);
                        self.shutdown_notify.notify_waiters();
                    }
                }
            }
        }
    }
}
