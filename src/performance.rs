use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::info;

// Count the number of blocks archived.
#[derive(Debug)]
pub struct BlockCounter {
    count: AtomicU32,
    started_on: Instant,
}

impl Default for BlockCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockCounter {
    pub fn new() -> Self {
        Self {
            count: AtomicU32::new(0),
            started_on: Instant::now(),
        }
    }

    pub fn increment(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    // Format a pretty message showing our block submissions archived per second.
    fn per_second(&self) -> f64 {
        let elapsed = self.started_on.elapsed();
        let seconds = elapsed.as_secs_f64();
        let count = self.count.load(Ordering::Relaxed);
        count as f64 / seconds
    }

    pub fn log(&self) {
        let count = self.count.load(Ordering::Relaxed);
        let per_second = self.per_second();
        info!(count, per_second, "block submission archive rate");
    }
}

pub async fn report_archive_rate_periodically(block_counter: &BlockCounter) {
    let mut interval = interval(Duration::from_secs(8));
    loop {
        interval.tick().await;
        block_counter.log();
    }
}
