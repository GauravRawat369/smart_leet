use time::Duration;
use uuid::Uuid;

use crate::backoff::BackoffConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub worker_id: String,
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub stalled_after: Duration,
    pub stalled_check_interval: Duration,
    pub backoff: BackoffConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            worker_id: generate_worker_id(),
            poll_interval: Duration::seconds(1),
            batch_size: 10,
            stalled_after: Duration::minutes(5),
            stalled_check_interval: Duration::minutes(1),
            backoff: BackoffConfig::default(),
        }
    }
}

impl Config {
    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = worker_id.into();
        self
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_stalled_after(mut self, stalled_after: Duration) -> Self {
        self.stalled_after = stalled_after;
        self
    }

    pub fn with_stalled_check_interval(mut self, stalled_check_interval: Duration) -> Self {
        self.stalled_check_interval = stalled_check_interval;
        self
    }

    pub fn with_backoff(mut self, backoff: BackoffConfig) -> Self {
        self.backoff = backoff;
        self
    }
}

pub fn generate_worker_id() -> String {
    Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_unique_worker_id() {
        assert_ne!(Config::default().worker_id, Config::default().worker_id);
    }

    #[test]
    fn builder_methods_override_defaults() {
        let config = Config::default()
            .with_worker_id("w1")
            .with_poll_interval(Duration::milliseconds(50))
            .with_batch_size(3)
            .with_stalled_after(Duration::seconds(30))
            .with_stalled_check_interval(Duration::seconds(5))
            .with_backoff(BackoffConfig::new(Duration::ZERO, Vec::new()));

        assert_eq!(config.worker_id, "w1");
        assert_eq!(config.poll_interval, Duration::milliseconds(50));
        assert_eq!(config.batch_size, 3);
        assert_eq!(config.stalled_after, Duration::seconds(30));
        assert_eq!(config.stalled_check_interval, Duration::seconds(5));
        assert_eq!(config.backoff.max_retries(), 0);
    }
}
