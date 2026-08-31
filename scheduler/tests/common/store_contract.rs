use scheduler::{NewTask, SchedulerStore, Status, business_status};
use serde_json::json;
use time::{Duration, OffsetDateTime};

pub const WORKER: &str = "worker-a";

pub fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
        .replace_nanosecond(0)
        .expect("valid time")
}

pub fn new_task(id: &str, schedule_time: OffsetDateTime) -> NewTask {
    NewTask {
        id: id.to_owned(),
        name: "job".to_owned(),
        payload: json!({ "id": id }),
        schedule_time,
    }
}

pub async fn insert_returns_new_task_and_find_reads_it_back<S: SchedulerStore>(store: &S) {
    let scheduled = now();
    let inserted = store.insert(new_task("t1", scheduled)).await.unwrap();

    assert_eq!(inserted.id, "t1");
    assert_eq!(inserted.status, Status::New);
    assert_eq!(inserted.business_status, business_status::PENDING);
    assert_eq!(inserted.retry_count, 0);
    assert_eq!(inserted.schedule_time, scheduled);
    assert_eq!(inserted.payload, json!({ "id": "t1" }));

    let found = store.find("t1").await.unwrap().expect("task exists");
    assert_eq!(found, inserted);
}

pub async fn find_returns_none_for_unknown_id<S: SchedulerStore>(store: &S) {
    assert!(store.find("missing").await.unwrap().is_none());
}

pub async fn insert_rejects_duplicate_id<S: SchedulerStore>(store: &S) {
    store.insert(new_task("dup", now())).await.unwrap();
    assert!(store.insert(new_task("dup", now())).await.is_err());
    assert_eq!(
        store.find("dup").await.unwrap().unwrap().status,
        Status::New
    );
}

pub async fn claim_due_picks_only_tasks_scheduled_at_or_before_now<S: SchedulerStore>(store: &S) {
    let at = now();
    store
        .insert(new_task("past", at - Duration::minutes(1)))
        .await
        .unwrap();
    store.insert(new_task("exact", at)).await.unwrap();
    store
        .insert(new_task("future", at + Duration::minutes(1)))
        .await
        .unwrap();

    let claimed = store.claim_due(at, 10, WORKER).await.unwrap();
    let ids: Vec<&str> = claimed.iter().map(|task| task.id.as_str()).collect();
    assert_eq!(ids, vec!["past", "exact"]);

    let future = store.find("future").await.unwrap().unwrap();
    assert_eq!(future.status, Status::New);
}

pub async fn claim_due_marks_tasks_running_and_locked<S: SchedulerStore>(store: &S) {
    let at = now();
    store.insert(new_task("t1", at)).await.unwrap();

    let claimed = store.claim_due(at, 10, WORKER).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].status, Status::Running);
    assert_eq!(claimed[0].locked_by.as_deref(), Some(WORKER));
    assert_eq!(claimed[0].locked_at, Some(at));

    let stored = store.find("t1").await.unwrap().unwrap();
    assert_eq!(stored.status, Status::Running);
    assert_eq!(stored.locked_by.as_deref(), Some(WORKER));
    assert_eq!(stored.locked_at, Some(at));
}

pub async fn claim_due_orders_by_schedule_time_and_honors_limit<S: SchedulerStore>(store: &S) {
    let at = now();
    store
        .insert(new_task("third", at - Duration::seconds(1)))
        .await
        .unwrap();
    store
        .insert(new_task("first", at - Duration::seconds(3)))
        .await
        .unwrap();
    store
        .insert(new_task("second", at - Duration::seconds(2)))
        .await
        .unwrap();

    let first_batch = store.claim_due(at, 2, WORKER).await.unwrap();
    let ids: Vec<&str> = first_batch.iter().map(|task| task.id.as_str()).collect();
    assert_eq!(ids, vec!["first", "second"]);

    let second_batch = store.claim_due(at, 2, WORKER).await.unwrap();
    let ids: Vec<&str> = second_batch.iter().map(|task| task.id.as_str()).collect();
    assert_eq!(ids, vec!["third"]);

    assert!(store.claim_due(at, 2, WORKER).await.unwrap().is_empty());
}

pub async fn claim_due_skips_tasks_whose_business_status_is_not_pending<S: SchedulerStore>(
    store: &S,
    revoke: impl AsyncFn(&str),
) {
    let at = now();
    store.insert(new_task("live", at)).await.unwrap();
    store.insert(new_task("revoked", at)).await.unwrap();
    revoke("revoked").await;

    let claimed = store.claim_due(at, 10, WORKER).await.unwrap();
    let ids: Vec<&str> = claimed.iter().map(|task| task.id.as_str()).collect();
    assert_eq!(ids, vec!["live"]);
    assert_eq!(
        store.find("revoked").await.unwrap().unwrap().status,
        Status::New
    );
}

pub async fn claim_due_skips_running_and_finished_tasks<S: SchedulerStore>(store: &S) {
    let at = now();
    store.insert(new_task("running", at)).await.unwrap();
    store.insert(new_task("finished", at)).await.unwrap();
    store.claim_due(at, 10, WORKER).await.unwrap();
    store
        .finish("finished", business_status::COMPLETED)
        .await
        .unwrap();

    assert!(store.claim_due(at, 10, WORKER).await.unwrap().is_empty());
}

pub async fn claim_due_with_zero_limit_claims_nothing<S: SchedulerStore>(store: &S) {
    let at = now();
    store.insert(new_task("t1", at)).await.unwrap();
    assert!(store.claim_due(at, 0, WORKER).await.unwrap().is_empty());
    assert_eq!(store.find("t1").await.unwrap().unwrap().status, Status::New);
}

pub async fn reschedule_returns_task_to_pending_with_new_time_and_retry_count<S: SchedulerStore>(
    store: &S,
) {
    let at = now();
    store.insert(new_task("t1", at)).await.unwrap();
    store.claim_due(at, 10, WORKER).await.unwrap();

    let next_run = at + Duration::minutes(5);
    store.reschedule("t1", next_run, 3).await.unwrap();

    let task = store.find("t1").await.unwrap().unwrap();
    assert_eq!(task.status, Status::Pending);
    assert_eq!(task.schedule_time, next_run);
    assert_eq!(task.retry_count, 3);
    assert_eq!(task.locked_by, None);
    assert_eq!(task.locked_at, None);
    assert_eq!(task.business_status, business_status::PENDING);
}

pub async fn rescheduled_task_is_claimed_again_once_due<S: SchedulerStore>(store: &S) {
    let at = now();
    store.insert(new_task("t1", at)).await.unwrap();
    store.claim_due(at, 10, WORKER).await.unwrap();
    store
        .reschedule("t1", at + Duration::minutes(5), 1)
        .await
        .unwrap();

    assert!(store.claim_due(at, 10, WORKER).await.unwrap().is_empty());
    let claimed = store
        .claim_due(at + Duration::minutes(5), 10, WORKER)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].retry_count, 1);
}

pub async fn reschedule_unknown_id_is_an_error<S: SchedulerStore>(store: &S) {
    assert!(store.reschedule("missing", now(), 1).await.is_err());
}

pub async fn finish_marks_task_terminal_with_business_status<S: SchedulerStore>(store: &S) {
    let at = now();
    store.insert(new_task("t1", at)).await.unwrap();
    store.claim_due(at, 10, WORKER).await.unwrap();
    store
        .finish("t1", business_status::COMPLETED)
        .await
        .unwrap();

    let task = store.find("t1").await.unwrap().unwrap();
    assert_eq!(task.status, Status::Finish);
    assert_eq!(task.business_status, business_status::COMPLETED);
    assert_eq!(task.locked_by, None);
    assert_eq!(task.locked_at, None);
}

pub async fn finish_unknown_id_is_an_error<S: SchedulerStore>(store: &S) {
    assert!(
        store
            .finish("missing", business_status::COMPLETED)
            .await
            .is_err()
    );
}

pub async fn recover_stalled_returns_only_long_running_tasks_to_pending<S: SchedulerStore>(
    store: &S,
) {
    let at = now();
    store
        .insert(new_task("stale", at - Duration::minutes(10)))
        .await
        .unwrap();
    store
        .claim_due(at - Duration::minutes(10), 1, WORKER)
        .await
        .unwrap();
    store.insert(new_task("fresh", at)).await.unwrap();
    store.claim_due(at, 1, WORKER).await.unwrap();
    store.insert(new_task("idle", at)).await.unwrap();

    let recovered = store
        .recover_stalled(at - Duration::minutes(5))
        .await
        .unwrap();
    assert_eq!(recovered, 1);

    let stale = store.find("stale").await.unwrap().unwrap();
    assert_eq!(stale.status, Status::Pending);
    assert_eq!(stale.locked_by, None);
    assert_eq!(stale.locked_at, None);
    assert_eq!(
        store.find("fresh").await.unwrap().unwrap().status,
        Status::Running
    );
    assert_eq!(
        store.find("idle").await.unwrap().unwrap().status,
        Status::New
    );
}

pub async fn recovered_task_is_claimable_again<S: SchedulerStore>(store: &S) {
    let at = now();
    store
        .insert(new_task("t1", at - Duration::minutes(10)))
        .await
        .unwrap();
    store
        .claim_due(at - Duration::minutes(10), 1, WORKER)
        .await
        .unwrap();
    store.recover_stalled(at).await.unwrap();

    let claimed = store.claim_due(at, 10, "worker-b").await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].locked_by.as_deref(), Some("worker-b"));
}

pub async fn recover_stalled_with_nothing_stuck_returns_zero<S: SchedulerStore>(store: &S) {
    let at = now();
    store.insert(new_task("t1", at)).await.unwrap();
    store.claim_due(at, 10, WORKER).await.unwrap();
    assert_eq!(
        store
            .recover_stalled(at - Duration::minutes(5))
            .await
            .unwrap(),
        0
    );
}
