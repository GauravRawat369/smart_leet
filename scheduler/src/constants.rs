pub mod business_status {
    pub const PENDING: &str = "Pending";
    pub const COMPLETED: &str = "COMPLETED";
    pub const RETRIES_EXCEEDED: &str = "RETRIES_EXCEEDED";
    pub const UNKNOWN_WORKFLOW: &str = "UNKNOWN_WORKFLOW";
}

pub mod status_label {
    pub const NEW: &str = "new";
    pub const RUNNING: &str = "running";
    pub const PENDING: &str = "pending";
    pub const FINISH: &str = "finish";
}

pub mod engine_defaults {
    pub const POLL_INTERVAL_SECONDS: i64 = 1;
    pub const BATCH_SIZE: usize = 10;
    pub const STALLED_AFTER_MINUTES: i64 = 5;
    pub const STALLED_CHECK_INTERVAL_MINUTES: i64 = 1;
    pub const MIN_TICK_INTERVAL_MILLIS: u64 = 1;
}

pub mod backoff_defaults {
    pub const START_AFTER_SECONDS: i64 = 0;
    pub const WINDOWS: [(i64, u32); 3] = [(60, 5), (300, 5), (1800, 5)];
}

#[cfg(feature = "postgres")]
pub mod postgres_sql {
    pub const SCHEMA: &str = include_str!("../migrations/0001_create_scheduler_tasks.sql");

    pub const INSERT_TASK: &str = "
INSERT INTO scheduler_tasks
    (id, name, payload, schedule_time, retry_count, status, business_status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
RETURNING *";

    pub const CLAIM_DUE: &str = "
UPDATE scheduler_tasks
SET status = $1, locked_by = $2, locked_at = $3, updated_at = $3
WHERE id IN (
    SELECT id FROM scheduler_tasks
    WHERE status IN ($4, $5) AND business_status = $6 AND schedule_time <= $3
    ORDER BY schedule_time, id
    LIMIT $7
    FOR UPDATE SKIP LOCKED
)
RETURNING *";

    pub const RESCHEDULE_TASK: &str = "
UPDATE scheduler_tasks
SET status = $2, schedule_time = $3, retry_count = $4, locked_by = NULL, locked_at = NULL, updated_at = $5
WHERE id = $1";

    pub const FINISH_TASK: &str = "
UPDATE scheduler_tasks
SET status = $2, business_status = $3, locked_by = NULL, locked_at = NULL, updated_at = $4
WHERE id = $1";

    pub const RECOVER_STALLED: &str = "
UPDATE scheduler_tasks
SET status = $1, locked_by = NULL, locked_at = NULL, updated_at = $3
WHERE status = $2 AND locked_at < $4";

    pub const FIND_TASK: &str = "SELECT * FROM scheduler_tasks WHERE id = $1";

    pub const SET_BUSINESS_STATUS: &str = "
UPDATE scheduler_tasks SET business_status = $2, updated_at = $3 WHERE id = $1";
}
