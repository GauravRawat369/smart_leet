mod common;

use std::collections::HashSet;

use common::engine_harness::{
    Execution, NoWorkflowDispatcher, RunningEngine, ScriptedWorkflow, SingleWorkflowDispatcher,
    fast_config, settle, wait_until,
};
use common::fixtures;
use scheduler::memory::MemoryStore;
use scheduler::{BackoffConfig, Outcome, RetryWindow, SchedulerStore, Status, business_status};
use time::Duration;

fn immediate_retries(count: u32) -> BackoffConfig {
    BackoffConfig::new(Duration::ZERO, vec![RetryWindow::seconds(0, count)])
}

async fn task_status(store: &MemoryStore, id: &str) -> Status {
    store.find(id).await.unwrap().unwrap().status
}

async fn task_is_finished(store: &MemoryStore, id: &str) -> bool {
    task_status(store, id).await == Status::Finish
}

#[tokio::test]
async fn task_retries_once_then_finishes() {
    let store = MemoryStore::new();
    store
        .insert(fixtures::new_task("t1", fixtures::now()))
        .await
        .unwrap();
    let workflow = ScriptedWorkflow::new(|_, attempt| match attempt {
        1 => Outcome::Retry,
        _ => Outcome::done(business_status::COMPLETED),
    });
    let engine = RunningEngine::start(
        store.clone(),
        SingleWorkflowDispatcher(workflow.clone()),
        fast_config("w1").with_backoff(immediate_retries(3)),
    );

    wait_until(|| task_is_finished(&store, "t1")).await;
    let metrics = engine.stop().await.snapshot();

    let task = store.find("t1").await.unwrap().unwrap();
    assert_eq!(task.business_status, business_status::COMPLETED);
    assert_eq!(task.retry_count, 1);
    assert_eq!(
        workflow.executions().await,
        vec![
            Execution {
                id: "t1".to_owned(),
                retry_count: 0
            },
            Execution {
                id: "t1".to_owned(),
                retry_count: 1
            },
        ]
    );
    assert_eq!(metrics.claimed, 2);
    assert_eq!(metrics.retried, 1);
    assert_eq!(metrics.finished, 1);
    assert_eq!(metrics.failed, 0);
}

#[tokio::test]
async fn task_that_keeps_failing_ends_with_retries_exceeded() {
    let store = MemoryStore::new();
    store
        .insert(fixtures::new_task("t1", fixtures::now()))
        .await
        .unwrap();
    let workflow = ScriptedWorkflow::new(|_, _| Outcome::Retry);
    let engine = RunningEngine::start(
        store.clone(),
        SingleWorkflowDispatcher(workflow.clone()),
        fast_config("w1").with_backoff(immediate_retries(2)),
    );

    wait_until(|| task_is_finished(&store, "t1")).await;
    let metrics = engine.stop().await.snapshot();

    let task = store.find("t1").await.unwrap().unwrap();
    assert_eq!(task.business_status, business_status::RETRIES_EXCEEDED);
    let retry_counts: Vec<i32> = workflow
        .executions()
        .await
        .into_iter()
        .map(|execution| execution.retry_count)
        .collect();
    assert_eq!(retry_counts, vec![0, 1, 2]);
    assert_eq!(metrics.retried, 2);
    assert_eq!(metrics.failed, 1);
}

#[tokio::test]
async fn recurring_task_reschedules_itself_with_retry_at() {
    let store = MemoryStore::new();
    store
        .insert(fixtures::new_task("heartbeat", fixtures::now()))
        .await
        .unwrap();
    let workflow = ScriptedWorkflow::new(|_, attempt| {
        if attempt < 3 {
            Outcome::RetryAt(time::OffsetDateTime::now_utc() + Duration::milliseconds(20))
        } else {
            Outcome::done(business_status::COMPLETED)
        }
    });
    let engine = RunningEngine::start(
        store.clone(),
        SingleWorkflowDispatcher(workflow.clone()),
        fast_config("w1"),
    );

    wait_until(|| task_is_finished(&store, "heartbeat")).await;
    let metrics = engine.stop().await.snapshot();

    let executions = workflow.executions().await;
    assert_eq!(executions.len(), 3);
    assert!(
        executions
            .iter()
            .all(|execution| execution.retry_count == 0)
    );
    assert_eq!(metrics.retried, 2);
    assert_eq!(metrics.finished, 1);
}

#[tokio::test]
async fn revoked_task_is_never_executed() {
    let store = MemoryStore::new();
    store
        .insert(fixtures::new_task("t1", fixtures::now()))
        .await
        .unwrap();
    store.set_business_status("t1", "REVOKED").await.unwrap();
    let workflow = ScriptedWorkflow::new(|_, _| Outcome::done(business_status::COMPLETED));
    let engine = RunningEngine::start(
        store.clone(),
        SingleWorkflowDispatcher(workflow.clone()),
        fast_config("w1"),
    );

    settle().await;
    let metrics = engine.stop().await.snapshot();

    assert_eq!(workflow.execution_count().await, 0);
    assert_eq!(task_status(&store, "t1").await, Status::New);
    assert_eq!(metrics.claimed, 0);
}

#[tokio::test]
async fn future_task_is_not_executed_before_its_schedule_time() {
    let store = MemoryStore::new();
    store
        .insert(fixtures::new_task(
            "later",
            fixtures::now() + Duration::hours(1),
        ))
        .await
        .unwrap();
    let workflow = ScriptedWorkflow::new(|_, _| Outcome::done(business_status::COMPLETED));
    let engine = RunningEngine::start(
        store.clone(),
        SingleWorkflowDispatcher(workflow.clone()),
        fast_config("w1"),
    );

    settle().await;
    engine.stop().await;

    assert_eq!(workflow.execution_count().await, 0);
    assert_eq!(task_status(&store, "later").await, Status::New);
}

#[tokio::test]
async fn stalled_running_task_is_recovered_and_executed() {
    let store = MemoryStore::new();
    let long_ago = fixtures::now() - Duration::hours(1);
    store
        .insert(fixtures::new_task("stuck", long_ago))
        .await
        .unwrap();
    let claimed = store
        .claim_due(long_ago, 1, "crashed-worker")
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);

    let workflow = ScriptedWorkflow::new(|_, _| Outcome::done(business_status::COMPLETED));
    let engine = RunningEngine::start(
        store.clone(),
        SingleWorkflowDispatcher(workflow.clone()),
        fast_config("w1").with_stalled_after(Duration::minutes(5)),
    );

    wait_until(|| task_is_finished(&store, "stuck")).await;
    let metrics = engine.stop().await.snapshot();

    assert_eq!(workflow.execution_count().await, 1);
    assert_eq!(metrics.recovered, 1);
    assert_eq!(metrics.finished, 1);
}

#[tokio::test]
async fn unknown_workflow_name_finishes_task_as_unknown_workflow() {
    let store = MemoryStore::new();
    store
        .insert(fixtures::new_task("t1", fixtures::now()))
        .await
        .unwrap();
    let engine = RunningEngine::start(store.clone(), NoWorkflowDispatcher, fast_config("w1"));

    wait_until(|| task_is_finished(&store, "t1")).await;
    let metrics = engine.stop().await.snapshot();

    let task = store.find("t1").await.unwrap().unwrap();
    assert_eq!(task.business_status, business_status::UNKNOWN_WORKFLOW);
    assert_eq!(metrics.failed, 1);
}

#[tokio::test]
async fn shutdown_waits_for_in_flight_task_to_complete() {
    let store = MemoryStore::new();
    store
        .insert(fixtures::new_task("slow", fixtures::now()))
        .await
        .unwrap();
    let workflow = ScriptedWorkflow::new(|_, _| Outcome::done(business_status::COMPLETED));
    let engine = RunningEngine::start(
        store.clone(),
        SingleWorkflowDispatcher(workflow.clone()),
        fast_config("w1"),
    );

    wait_until(|| async { task_status(&store, "slow").await == Status::Running }).await;
    engine.stop().await;

    assert_eq!(task_status(&store, "slow").await, Status::Finish);
    assert_eq!(workflow.execution_count().await, 1);
}

#[tokio::test]
async fn batch_size_caps_concurrent_executions() {
    let store = MemoryStore::new();
    for index in 0..6 {
        store
            .insert(fixtures::new_task(&format!("t{index}"), fixtures::now()))
            .await
            .unwrap();
    }
    let workflow = ScriptedWorkflow::new(|_, _| Outcome::done(business_status::COMPLETED));
    let engine = RunningEngine::start(
        store.clone(),
        SingleWorkflowDispatcher(workflow.clone()),
        fast_config("w1").with_batch_size(2),
    );

    wait_until(|| async {
        store
            .all_tasks()
            .await
            .iter()
            .all(|task| task.status == Status::Finish)
    })
    .await;
    engine.stop().await;

    assert_eq!(workflow.execution_count().await, 6);
    assert!(workflow.max_in_progress() <= 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_engines_on_one_store_never_double_execute() {
    let store = MemoryStore::new();
    let total = 60;
    for index in 0..total {
        store
            .insert(fixtures::new_task(&format!("t{index:02}"), fixtures::now()))
            .await
            .unwrap();
    }
    let workflow = ScriptedWorkflow::new(|_, _| Outcome::done(business_status::COMPLETED));
    let first = RunningEngine::start(
        store.clone(),
        SingleWorkflowDispatcher(workflow.clone()),
        fast_config("w1").with_batch_size(5),
    );
    let second = RunningEngine::start(
        store.clone(),
        SingleWorkflowDispatcher(workflow.clone()),
        fast_config("w2").with_batch_size(5),
    );

    wait_until(|| async {
        store
            .all_tasks()
            .await
            .iter()
            .all(|task| task.status == Status::Finish)
    })
    .await;
    let first_metrics = first.stop().await.snapshot();
    let second_metrics = second.stop().await.snapshot();

    let executions = workflow.executions().await;
    let unique: HashSet<&str> = executions
        .iter()
        .map(|execution| execution.id.as_str())
        .collect();
    assert_eq!(executions.len(), total);
    assert_eq!(unique.len(), total);
    assert_eq!(first_metrics.claimed + second_metrics.claimed, total as u64);
    assert!(first_metrics.claimed > 0);
    assert!(second_metrics.claimed > 0);
}
