use time::OffsetDateTime;

use crate::backoff::BackoffConfig;
use crate::engine::metrics::Metrics;
use crate::outcome::Outcome;
use crate::store::SchedulerStore;
use crate::task::{Task, business_status};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transition {
    Finish {
        business_status: String,
    },
    Reschedule {
        next_run: OffsetDateTime,
        retry_count: i32,
    },
}

pub fn plan_transition(
    outcome: &Outcome,
    current_retry_count: i32,
    backoff: &BackoffConfig,
    now: OffsetDateTime,
) -> Transition {
    match outcome {
        Outcome::Done(status) | Outcome::GiveUp(status) => Transition::Finish {
            business_status: status.clone(),
        },
        Outcome::Retry => plan_retry(current_retry_count, backoff, now),
        Outcome::RetryAt(next_run) => Transition::Reschedule {
            next_run: *next_run,
            retry_count: 0,
        },
    }
}

fn plan_retry(
    current_retry_count: i32,
    backoff: &BackoffConfig,
    now: OffsetDateTime,
) -> Transition {
    let retry_count = current_retry_count.saturating_add(1);
    match backoff.delay(retry_count) {
        Some(delay) => Transition::Reschedule {
            next_run: now + delay,
            retry_count,
        },
        None => Transition::Finish {
            business_status: business_status::RETRIES_EXCEEDED.to_owned(),
        },
    }
}

pub async fn apply_transition<S: SchedulerStore>(
    store: &S,
    task_id: &str,
    transition: &Transition,
) -> Result<(), S::Error> {
    match transition {
        Transition::Finish { business_status } => store.finish(task_id, business_status).await,
        Transition::Reschedule {
            next_run,
            retry_count,
        } => store.reschedule(task_id, *next_run, *retry_count).await,
    }
}

pub async fn apply_outcome<S: SchedulerStore>(
    store: &S,
    task: &Task,
    outcome: &Outcome,
    backoff: &BackoffConfig,
    metrics: &Metrics,
    now: OffsetDateTime,
) -> Result<Transition, S::Error> {
    let transition = plan_transition(outcome, task.retry_count, backoff, now);
    apply_transition(store, &task.id, &transition).await?;
    record_outcome_metrics(metrics, outcome, &transition);
    Ok(transition)
}

fn record_outcome_metrics(metrics: &Metrics, outcome: &Outcome, transition: &Transition) {
    match (outcome, transition) {
        (Outcome::Done(_), _) => metrics.record_finished(),
        (Outcome::GiveUp(_), _) => metrics.record_failed(),
        (_, Transition::Reschedule { .. }) => metrics.record_retried(),
        (_, Transition::Finish { .. }) => metrics.record_failed(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backoff::RetryWindow;
    use time::Duration;

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp")
    }

    fn backoff() -> BackoffConfig {
        BackoffConfig::new(Duration::ZERO, vec![RetryWindow::seconds(30, 2)])
    }

    #[test]
    fn done_finishes_with_given_business_status() {
        assert_eq!(
            plan_transition(&Outcome::done("COMPLETED"), 0, &backoff(), fixed_now()),
            Transition::Finish {
                business_status: "COMPLETED".to_owned()
            }
        );
    }

    #[test]
    fn give_up_finishes_with_given_business_status() {
        assert_eq!(
            plan_transition(&Outcome::give_up("INVALID"), 4, &backoff(), fixed_now()),
            Transition::Finish {
                business_status: "INVALID".to_owned()
            }
        );
    }

    #[test]
    fn retry_increments_retry_count_and_applies_backoff_delay() {
        let now = fixed_now();
        assert_eq!(
            plan_transition(&Outcome::Retry, 0, &backoff(), now),
            Transition::Reschedule {
                next_run: now + Duration::seconds(30),
                retry_count: 1
            }
        );
        assert_eq!(
            plan_transition(&Outcome::Retry, 1, &backoff(), now),
            Transition::Reschedule {
                next_run: now + Duration::seconds(30),
                retry_count: 2
            }
        );
    }

    #[test]
    fn retry_past_backoff_windows_finishes_with_retries_exceeded() {
        assert_eq!(
            plan_transition(&Outcome::Retry, 2, &backoff(), fixed_now()),
            Transition::Finish {
                business_status: business_status::RETRIES_EXCEEDED.to_owned()
            }
        );
    }

    #[test]
    fn retry_at_reschedules_to_exact_time_and_resets_retry_count() {
        let later = fixed_now() + Duration::hours(6);
        assert_eq!(
            plan_transition(&Outcome::RetryAt(later), 7, &backoff(), fixed_now()),
            Transition::Reschedule {
                next_run: later,
                retry_count: 0
            }
        );
    }

    #[test]
    fn retry_count_increment_saturates_at_max() {
        let generous = BackoffConfig::new(Duration::ZERO, vec![RetryWindow::seconds(1, u32::MAX)]);
        assert_eq!(
            plan_transition(&Outcome::Retry, i32::MAX, &generous, fixed_now()),
            Transition::Reschedule {
                next_run: fixed_now() + Duration::seconds(1),
                retry_count: i32::MAX
            }
        );
    }
}
