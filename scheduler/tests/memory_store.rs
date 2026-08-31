mod common;

use common::fixtures;
use common::store_contract as contract;
use scheduler::SchedulerStore;
use scheduler::memory::{MemoryStore, MemoryStoreError};

#[tokio::test]
async fn insert_returns_new_task_and_find_reads_it_back() {
    contract::insert_returns_new_task_and_find_reads_it_back(&MemoryStore::new()).await;
}

#[tokio::test]
async fn find_returns_none_for_unknown_id() {
    contract::find_returns_none_for_unknown_id(&MemoryStore::new()).await;
}

#[tokio::test]
async fn insert_rejects_duplicate_id() {
    contract::insert_rejects_duplicate_id(&MemoryStore::new()).await;
}

#[tokio::test]
async fn claim_due_picks_only_tasks_scheduled_at_or_before_now() {
    contract::claim_due_picks_only_tasks_scheduled_at_or_before_now(&MemoryStore::new()).await;
}

#[tokio::test]
async fn claim_due_marks_tasks_running_and_locked() {
    contract::claim_due_marks_tasks_running_and_locked(&MemoryStore::new()).await;
}

#[tokio::test]
async fn claim_due_orders_by_schedule_time_and_honors_limit() {
    contract::claim_due_orders_by_schedule_time_and_honors_limit(&MemoryStore::new()).await;
}

#[tokio::test]
async fn claim_due_skips_tasks_whose_business_status_is_not_pending() {
    let store = MemoryStore::new();
    contract::claim_due_skips_tasks_whose_business_status_is_not_pending(&store, async |id| {
        store.set_business_status(id, "REVOKED").await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn claim_due_skips_running_and_finished_tasks() {
    contract::claim_due_skips_running_and_finished_tasks(&MemoryStore::new()).await;
}

#[tokio::test]
async fn claim_due_with_zero_limit_claims_nothing() {
    contract::claim_due_with_zero_limit_claims_nothing(&MemoryStore::new()).await;
}

#[tokio::test]
async fn reschedule_returns_task_to_pending_with_new_time_and_retry_count() {
    contract::reschedule_returns_task_to_pending_with_new_time_and_retry_count(&MemoryStore::new())
        .await;
}

#[tokio::test]
async fn rescheduled_task_is_claimed_again_once_due() {
    contract::rescheduled_task_is_claimed_again_once_due(&MemoryStore::new()).await;
}

#[tokio::test]
async fn reschedule_unknown_id_is_an_error() {
    contract::reschedule_unknown_id_is_an_error(&MemoryStore::new()).await;
}

#[tokio::test]
async fn finish_marks_task_terminal_with_business_status() {
    contract::finish_marks_task_terminal_with_business_status(&MemoryStore::new()).await;
}

#[tokio::test]
async fn finish_unknown_id_is_an_error() {
    contract::finish_unknown_id_is_an_error(&MemoryStore::new()).await;
}

#[tokio::test]
async fn recover_stalled_returns_only_long_running_tasks_to_pending() {
    contract::recover_stalled_returns_only_long_running_tasks_to_pending(&MemoryStore::new()).await;
}

#[tokio::test]
async fn recovered_task_is_claimable_again() {
    contract::recovered_task_is_claimable_again(&MemoryStore::new()).await;
}

#[tokio::test]
async fn recover_stalled_with_nothing_stuck_returns_zero() {
    contract::recover_stalled_with_nothing_stuck_returns_zero(&MemoryStore::new()).await;
}

#[tokio::test]
async fn duplicate_and_missing_ids_surface_typed_errors() {
    let store = MemoryStore::new();
    let now = fixtures::now();
    store.insert(fixtures::new_task("t1", now)).await.unwrap();

    assert_eq!(
        store
            .insert(fixtures::new_task("t1", now))
            .await
            .unwrap_err(),
        MemoryStoreError::DuplicateTask("t1".to_owned())
    );
    assert_eq!(
        store
            .set_business_status("missing", "REVOKED")
            .await
            .unwrap_err(),
        MemoryStoreError::TaskNotFound("missing".to_owned())
    );
}

#[tokio::test]
async fn all_tasks_lists_every_task_sorted_by_id() {
    let store = MemoryStore::new();
    let now = fixtures::now();
    store.insert(fixtures::new_task("b", now)).await.unwrap();
    store.insert(fixtures::new_task("a", now)).await.unwrap();

    let ids: Vec<String> = store
        .all_tasks()
        .await
        .into_iter()
        .map(|task| task.id)
        .collect();
    assert_eq!(ids, vec!["a", "b"]);
    assert_eq!(store.task_count().await, 2);
}
