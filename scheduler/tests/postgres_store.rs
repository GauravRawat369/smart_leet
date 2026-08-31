#![cfg(feature = "postgres")]

mod common;

use std::collections::HashSet;

use common::engine_harness::{
    RunningEngine, ScriptedWorkflow, SingleWorkflowDispatcher, fast_config, wait_until,
};
use common::fixtures;
use common::store_contract as contract;
use scheduler::postgres::{PgStore, PgStoreError};
use scheduler::{Outcome, SchedulerStore, Status, business_status};

const DATABASE_URL_ENV: &str = "SCHEDULER_TEST_DATABASE_URL";

async fn clean_store() -> PgStore {
    let url = std::env::var(DATABASE_URL_ENV)
        .unwrap_or_else(|_| panic!("set {DATABASE_URL_ENV} to run postgres tests"));
    let store = PgStore::connect(&url).await.expect("connect to postgres");
    store.ensure_schema().await.expect("apply schema");
    sqlx::query("TRUNCATE scheduler_tasks")
        .execute(store.pool())
        .await
        .expect("truncate table");
    store
}

#[tokio::test]
#[ignore]
async fn insert_returns_new_task_and_find_reads_it_back() {
    contract::insert_returns_new_task_and_find_reads_it_back(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn find_returns_none_for_unknown_id() {
    contract::find_returns_none_for_unknown_id(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn insert_rejects_duplicate_id() {
    contract::insert_rejects_duplicate_id(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn claim_due_picks_only_tasks_scheduled_at_or_before_now() {
    contract::claim_due_picks_only_tasks_scheduled_at_or_before_now(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn claim_due_marks_tasks_running_and_locked() {
    contract::claim_due_marks_tasks_running_and_locked(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn claim_due_orders_by_schedule_time_and_honors_limit() {
    contract::claim_due_orders_by_schedule_time_and_honors_limit(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn claim_due_skips_tasks_whose_business_status_is_not_pending() {
    let store = clean_store().await;
    contract::claim_due_skips_tasks_whose_business_status_is_not_pending(&store, async |id| {
        store.set_business_status(id, "REVOKED").await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn claim_due_skips_running_and_finished_tasks() {
    contract::claim_due_skips_running_and_finished_tasks(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn claim_due_with_zero_limit_claims_nothing() {
    contract::claim_due_with_zero_limit_claims_nothing(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn reschedule_returns_task_to_pending_with_new_time_and_retry_count() {
    contract::reschedule_returns_task_to_pending_with_new_time_and_retry_count(
        &clean_store().await,
    )
    .await;
}

#[tokio::test]
#[ignore]
async fn rescheduled_task_is_claimed_again_once_due() {
    contract::rescheduled_task_is_claimed_again_once_due(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn reschedule_unknown_id_is_an_error() {
    contract::reschedule_unknown_id_is_an_error(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn finish_marks_task_terminal_with_business_status() {
    contract::finish_marks_task_terminal_with_business_status(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn finish_unknown_id_is_an_error() {
    contract::finish_unknown_id_is_an_error(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn recover_stalled_returns_only_long_running_tasks_to_pending() {
    contract::recover_stalled_returns_only_long_running_tasks_to_pending(&clean_store().await)
        .await;
}

#[tokio::test]
#[ignore]
async fn recovered_task_is_claimable_again() {
    contract::recovered_task_is_claimable_again(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn recover_stalled_with_nothing_stuck_returns_zero() {
    contract::recover_stalled_with_nothing_stuck_returns_zero(&clean_store().await).await;
}

#[tokio::test]
#[ignore]
async fn duplicate_and_missing_ids_surface_typed_errors() {
    let store = clean_store().await;
    let now = fixtures::now();
    store.insert(fixtures::new_task("t1", now)).await.unwrap();

    assert!(matches!(
        store.insert(fixtures::new_task("t1", now)).await.unwrap_err(),
        PgStoreError::DuplicateTask(id) if id == "t1"
    ));
    assert!(matches!(
        store.set_business_status("missing", "REVOKED").await.unwrap_err(),
        PgStoreError::TaskNotFound(id) if id == "missing"
    ));
}

#[tokio::test]
#[ignore]
async fn ensure_schema_is_idempotent() {
    let store = clean_store().await;
    store.ensure_schema().await.unwrap();
    store.ensure_schema().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn concurrent_claims_never_hand_out_the_same_task() {
    let store = clean_store().await;
    let total = 100;
    let now = fixtures::now();
    for index in 0..total {
        store
            .insert(fixtures::new_task(&format!("t{index:03}"), now))
            .await
            .unwrap();
    }

    let mut claimers = Vec::new();
    for worker in 0..4 {
        let store = store.clone();
        claimers.push(tokio::spawn(async move {
            let mut claimed = Vec::new();
            loop {
                let batch = store
                    .claim_due(now, 7, &format!("worker-{worker}"))
                    .await
                    .unwrap();
                if batch.is_empty() {
                    break claimed;
                }
                claimed.extend(batch.into_iter().map(|task| task.id));
            }
        }));
    }

    let mut all_claimed = Vec::new();
    for claimer in claimers {
        all_claimed.extend(claimer.await.unwrap());
    }
    let unique: HashSet<&String> = all_claimed.iter().collect();
    assert_eq!(all_claimed.len(), total);
    assert_eq!(unique.len(), total);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn two_engines_on_postgres_execute_every_task_exactly_once() {
    let store = clean_store().await;
    let total = 40;
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
        let finished: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM scheduler_tasks WHERE status = $1")
                .bind(Status::Finish.as_str())
                .fetch_one(store.pool())
                .await
                .unwrap();
        finished == total as i64
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
}
