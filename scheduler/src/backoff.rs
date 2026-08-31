use time::{Duration, OffsetDateTime};

use crate::constants::backoff_defaults;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryWindow {
    pub interval: Duration,
    pub count: u32,
}

impl RetryWindow {
    pub const fn new(interval: Duration, count: u32) -> Self {
        Self { interval, count }
    }

    pub const fn seconds(interval_seconds: i64, count: u32) -> Self {
        Self::new(Duration::seconds(interval_seconds), count)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackoffConfig {
    pub start_after: Duration,
    pub windows: Vec<RetryWindow>,
}

impl BackoffConfig {
    pub fn new(start_after: Duration, windows: Vec<RetryWindow>) -> Self {
        Self {
            start_after,
            windows,
        }
    }

    pub fn delay(&self, retry_count: i32) -> Option<Duration> {
        let Ok(mut remaining) = u32::try_from(retry_count) else {
            return Some(self.start_after);
        };
        if remaining == 0 {
            return Some(self.start_after);
        }
        for window in &self.windows {
            if remaining <= window.count {
                return Some(window.interval);
            }
            remaining -= window.count;
        }
        None
    }

    pub fn next_run_at(&self, now: OffsetDateTime, retry_count: i32) -> Option<OffsetDateTime> {
        self.delay(retry_count).map(|delay| now + delay)
    }

    pub fn max_retries(&self) -> u32 {
        self.windows
            .iter()
            .fold(0, |total, window| total.saturating_add(window.count))
    }
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self::new(
            Duration::seconds(backoff_defaults::START_AFTER_SECONDS),
            backoff_defaults::WINDOWS
                .iter()
                .map(|(interval_seconds, count)| RetryWindow::seconds(*interval_seconds, *count))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn two_window_config() -> BackoffConfig {
        BackoffConfig::new(
            Duration::seconds(10),
            vec![RetryWindow::seconds(30, 3), RetryWindow::seconds(120, 2)],
        )
    }

    #[test]
    fn zero_retries_uses_start_after() {
        assert_eq!(two_window_config().delay(0), Some(Duration::seconds(10)));
    }

    #[test]
    fn negative_retry_count_is_treated_as_zero() {
        assert_eq!(two_window_config().delay(-3), Some(Duration::seconds(10)));
    }

    #[test]
    fn retries_within_first_window_use_first_interval() {
        let config = two_window_config();
        for retry_count in 1..=3 {
            assert_eq!(config.delay(retry_count), Some(Duration::seconds(30)));
        }
    }

    #[test]
    fn retries_walk_cumulatively_into_later_windows() {
        let config = two_window_config();
        assert_eq!(config.delay(4), Some(Duration::seconds(120)));
        assert_eq!(config.delay(5), Some(Duration::seconds(120)));
    }

    #[test]
    fn retries_beyond_all_windows_are_exhausted() {
        let config = two_window_config();
        assert_eq!(config.delay(6), None);
        assert_eq!(config.delay(100), None);
    }

    #[test]
    fn empty_windows_exhaust_on_first_retry() {
        let config = BackoffConfig::new(Duration::seconds(5), Vec::new());
        assert_eq!(config.delay(0), Some(Duration::seconds(5)));
        assert_eq!(config.delay(1), None);
    }

    #[test]
    fn zero_count_windows_are_skipped() {
        let config = BackoffConfig::new(
            Duration::ZERO,
            vec![RetryWindow::seconds(1, 0), RetryWindow::seconds(7, 1)],
        );
        assert_eq!(config.delay(1), Some(Duration::seconds(7)));
        assert_eq!(config.delay(2), None);
    }

    #[test]
    fn max_retries_sums_window_counts() {
        assert_eq!(two_window_config().max_retries(), 5);
        assert_eq!(BackoffConfig::default().max_retries(), 15);
    }

    #[test]
    fn next_run_at_offsets_now_by_delay() {
        let config = two_window_config();
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        assert_eq!(
            config.next_run_at(now, 1),
            Some(now + Duration::seconds(30))
        );
        assert_eq!(config.next_run_at(now, 6), None);
    }
}
