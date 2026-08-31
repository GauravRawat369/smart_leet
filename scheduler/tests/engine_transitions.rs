mod common;

use common::fixtures;
use scheduler::engine::{Metrics, MetricsSnapshot, Transition, apply_outcome, apply_transition};
use scheduler::memory::MemoryStore;
use scheduler::{BackoffConfig, Outcome, RetryWindow, SchedulerStore, Status, business_status};
use time::Duration;

fn backoff() -> BackoffConfig {
    BackoffConfig::new(Duration::ZERO, vec![RetryWindow::seconds(30, 1)])
}

async fn claimed_store() -> (MemoryStore, scheduler::Task) {
    let store = MemoryStore::new();
    let now = fixtures::now();
    store.insert(fixtures::new_task("t1", now)).await.unwrap();
    let task = store
        .claim_due(now, 1, fixtures::WORKER)
        .await
        .unwrap()
        .remove(0);
    (store, task)
}

#[tokio::test]
async fn apply_transition_finish_marks_task_terminal() {
    let (store, task) = claimed_store().await;
    apply_transition(
        &store,
        &task.id,
        &Transition::Finish {
            business_status: business_status::COMPLETED.to_owned(),
        },
    )
    .await
    .unwrap();

    let stored = store.find("t1").await.unwrap().unwrap();
    assert_eq!(stored.status, Status::Finish);
    assert_eq!(stored.business_status, business_status::COMPLETED);
}

#[tokio::test]
async fn apply_transition_reschedule_returns_task_to_pending() {
    let (store, task) = claimed_store().await;
    let next_run = fixtures::now() + Duration::minutes(1);
    apply_transition(
        &store,
        &task.id,
        &Transition::Reschedule {
            next_run,
            retry_count: 2,
        },
    )
    .await
    .unwrap();

    let stored = store.find("t1").await.unwrap().unwrap();
    assert_eq!(stored.status, Status::Pending);
    assert_eq!(stored.schedule_time, next_run);
    assert_eq!(stored.retry_count, 2);
}

#[tokio::test]
async fn apply_outcome_done_finishes_and_counts_finished() {
    let (store, task) = claimed_store().await;
    let metrics = Metrics::new();
    apply_outcome(
        &store,
        &task,
        &Outcome::done(business_status::COMPLETED),
        &backoff(),
        &metrics,
        fixtures::now(),
    )
    .await
    .unwrap();

    assert_eq!(
        store.find("t1").await.unwrap().unwrap().status,
        Status::Finish
    );
    assert_eq!(
        metrics.snapshot(),
        MetricsSnapshot {
            finished: 1,
            ..MetricsSnapshot::default()
        }
    );
}

#[tokio::test]
async fn apply_outcome_give_up_finishes_and_counts_failed() {
    let (store, task) = claimed_store().await;
    let metrics = Metrics::new();
    apply_outcome(
        &store,
        &task,
        &Outcome::give_up("INVALID"),
        &backoff(),
        &metrics,
        fixtures::now(),
    )
    .await
    .unwrap();

    let stored = store.find("t1").await.unwrap().unwrap();
    assert_eq!(stored.status, Status::Finish);
    assert_eq!(stored.business_status, "INVALID");
    assert_eq!(metrics.snapshot().failed, 1);
}

#[tokio::test]
async fn apply_outcome_retry_reschedules_with_backoff_and_counts_retried() {
    let (store, task) = claimed_store().await;
    let metrics = Metrics::new();
    let now = fixtures::now();
    apply_outcome(&store, &task, &Outcome::Retry, &backoff(), &metrics, now)
        .await
        .unwrap();

    let stored = store.find("t1").await.unwrap().unwrap();
    assert_eq!(stored.status, Status::Pending);
    assert_eq!(stored.retry_count, 1);
    assert_eq!(stored.schedule_time, now + Duration::seconds(30));
    assert_eq!(metrics.snapshot().retried, 1);
}

#[tokio::test]
async fn apply_outcome_retry_when_exhausted_finishes_with_retries_exceeded() {
    let store = MemoryStore::new();
    let now = fixtures::now();
    store.insert(fixtures::new_task("t1", now)).await.unwrap();
    store.claim_due(now, 1, fixtures::WORKER).await.unwrap();
    store.reschedule("t1", now, 1).await.unwrap();
    let task = store
        .claim_due(now, 1, fixtures::WORKER)
        .await
        .unwrap()
        .remove(0);
    let metrics = Metrics::new();

    let transition = apply_outcome(&store, &task, &Outcome::Retry, &backoff(), &metrics, now)
        .await
        .unwrap();

    assert_eq!(
        transition,
        Transition::Finish {
            business_status: business_status::RETRIES_EXCEEDED.to_owned()
        }
    );
    let stored = store.find("t1").await.unwrap().unwrap();
    assert_eq!(stored.status, Status::Finish);
    assert_eq!(stored.business_status, business_status::RETRIES_EXCEEDED);
    assert_eq!(metrics.snapshot().failed, 1);
    assert_eq!(metrics.snapshot().retried, 0);
}

#[tokio::test]
async fn apply_outcome_retry_at_reschedules_exactly_and_resets_retry_count() {
    let (store, task) = claimed_store().await;
    let metrics = Metrics::new();
    let later = fixtures::now() + Duration::hours(6);
    apply_outcome(
        &store,
        &task,
        &Outcome::RetryAt(later),
        &backoff(),
        &metrics,
        fixtures::now(),
    )
    .await
    .unwrap();

    let stored = store.find("t1").await.unwrap().unwrap();
    assert_eq!(stored.status, Status::Pending);
    assert_eq!(stored.schedule_time, later);
    assert_eq!(stored.retry_count, 0);
    assert_eq!(metrics.snapshot().retried, 1);
}

#[tokio::test]
async fn apply_outcome_surfaces_store_errors() {
    let store = MemoryStore::new();
    let now = fixtures::now();
    let ghost = fixtures::new_task("ghost", now).into_task(now);
    let metrics = Metrics::new();

    let result = apply_outcome(
        &store,
        &ghost,
        &Outcome::done(business_status::COMPLETED),
        &backoff(),
        &metrics,
        now,
    )
    .await;

    assert!(result.is_err());
    assert_eq!(metrics.snapshot(), MetricsSnapshot::default());
}
