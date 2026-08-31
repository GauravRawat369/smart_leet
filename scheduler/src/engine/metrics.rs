use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    claimed: AtomicU64,
    finished: AtomicU64,
    retried: AtomicU64,
    failed: AtomicU64,
    recovered: AtomicU64,
    store_errors: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub claimed: u64,
    pub finished: u64,
    pub retried: u64,
    pub failed: u64,
    pub recovered: u64,
    pub store_errors: u64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_claimed(&self, count: u64) {
        self.claimed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_finished(&self) {
        self.finished.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retried(&self) {
        self.retried.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_recovered(&self, count: u64) {
        self.recovered.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_store_error(&self) {
        self.store_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            claimed: self.claimed.load(Ordering::Relaxed),
            finished: self.finished.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            recovered: self.recovered.load(Ordering::Relaxed),
            store_errors: self.store_errors.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate_into_snapshot() {
        let metrics = Metrics::new();
        metrics.record_claimed(3);
        metrics.record_finished();
        metrics.record_retried();
        metrics.record_retried();
        metrics.record_failed();
        metrics.record_recovered(2);
        metrics.record_store_error();

        assert_eq!(
            metrics.snapshot(),
            MetricsSnapshot {
                claimed: 3,
                finished: 1,
                retried: 2,
                failed: 1,
                recovered: 2,
                store_errors: 1,
            }
        );
    }
}
