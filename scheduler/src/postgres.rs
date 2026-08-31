use async_trait::async_trait;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgRow};
use time::OffsetDateTime;

use crate::store::SchedulerStore;
use crate::task::{NewTask, Status, Task, UnknownStatus, business_status};

pub const SCHEMA_SQL: &str = include_str!("../migrations/0001_create_scheduler_tasks.sql");

const INSERT_TASK: &str = "
INSERT INTO scheduler_tasks
    (id, name, payload, schedule_time, retry_count, status, business_status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
RETURNING *";

const CLAIM_DUE: &str = "
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

const RESCHEDULE_TASK: &str = "
UPDATE scheduler_tasks
SET status = $2, schedule_time = $3, retry_count = $4, locked_by = NULL, locked_at = NULL, updated_at = $5
WHERE id = $1";

const FINISH_TASK: &str = "
UPDATE scheduler_tasks
SET status = $2, business_status = $3, locked_by = NULL, locked_at = NULL, updated_at = $4
WHERE id = $1";

const RECOVER_STALLED: &str = "
UPDATE scheduler_tasks
SET status = $1, locked_by = NULL, locked_at = NULL, updated_at = $3
WHERE status = $2 AND locked_at < $4";

const FIND_TASK: &str = "SELECT * FROM scheduler_tasks WHERE id = $1";

const SET_BUSINESS_STATUS: &str = "
UPDATE scheduler_tasks SET business_status = $2, updated_at = $3 WHERE id = $1";

#[derive(Debug, thiserror::Error)]
pub enum PgStoreError {
    #[error("task `{0}` already exists")]
    DuplicateTask(String),
    #[error("task `{0}` not found")]
    TaskNotFound(String),
    #[error("task `{id}` has an invalid stored status: {source}")]
    InvalidStatus { id: String, source: UnknownStatus },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Clone)]
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> Result<Self, PgStoreError> {
        Ok(Self::new(PgPool::connect(database_url).await?))
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn ensure_schema(&self) -> Result<(), PgStoreError> {
        sqlx::raw_sql(SCHEMA_SQL).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn set_business_status(
        &self,
        id: &str,
        business_status: &str,
    ) -> Result<(), PgStoreError> {
        let result = sqlx::query(SET_BUSINESS_STATUS)
            .bind(id)
            .bind(business_status)
            .bind(OffsetDateTime::now_utc())
            .execute(&self.pool)
            .await?;
        require_row_updated(result.rows_affected(), id)
    }
}

fn require_row_updated(rows_affected: u64, id: &str) -> Result<(), PgStoreError> {
    if rows_affected == 0 {
        return Err(PgStoreError::TaskNotFound(id.to_owned()));
    }
    Ok(())
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(db_error) if db_error.is_unique_violation())
}

fn task_from_row(row: &PgRow) -> Result<Task, PgStoreError> {
    let id: String = row.try_get("id")?;
    let stored_status: String = row.try_get("status")?;
    let status = stored_status
        .parse::<Status>()
        .map_err(|source| PgStoreError::InvalidStatus {
            id: id.clone(),
            source,
        })?;
    Ok(Task {
        id,
        name: row.try_get("name")?,
        payload: row.try_get("payload")?,
        schedule_time: row.try_get("schedule_time")?,
        retry_count: row.try_get("retry_count")?,
        status,
        business_status: row.try_get("business_status")?,
        locked_by: row.try_get("locked_by")?,
        locked_at: row.try_get("locked_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn tasks_from_rows(rows: &[PgRow]) -> Result<Vec<Task>, PgStoreError> {
    rows.iter().map(task_from_row).collect()
}

fn sort_by_schedule_then_id(tasks: &mut [Task]) {
    tasks.sort_by(|left, right| {
        left.schedule_time
            .cmp(&right.schedule_time)
            .then_with(|| left.id.cmp(&right.id))
    });
}

fn limit_as_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

#[async_trait]
impl SchedulerStore for PgStore {
    type Error = PgStoreError;

    async fn insert(&self, task: NewTask) -> Result<Task, Self::Error> {
        let task = task.into_task(OffsetDateTime::now_utc());
        let row = sqlx::query(INSERT_TASK)
            .bind(&task.id)
            .bind(&task.name)
            .bind(&task.payload)
            .bind(task.schedule_time)
            .bind(task.retry_count)
            .bind(task.status.as_str())
            .bind(&task.business_status)
            .bind(task.created_at)
            .fetch_one(&self.pool)
            .await
            .map_err(|error| {
                if is_unique_violation(&error) {
                    PgStoreError::DuplicateTask(task.id.clone())
                } else {
                    PgStoreError::Database(error)
                }
            })?;
        task_from_row(&row)
    }

    async fn claim_due(
        &self,
        now: OffsetDateTime,
        limit: usize,
        worker_id: &str,
    ) -> Result<Vec<Task>, Self::Error> {
        let rows = sqlx::query(CLAIM_DUE)
            .bind(Status::Running.as_str())
            .bind(worker_id)
            .bind(now)
            .bind(Status::New.as_str())
            .bind(Status::Pending.as_str())
            .bind(business_status::PENDING)
            .bind(limit_as_i64(limit))
            .fetch_all(&self.pool)
            .await?;
        let mut tasks = tasks_from_rows(&rows)?;
        sort_by_schedule_then_id(&mut tasks);
        Ok(tasks)
    }

    async fn reschedule(
        &self,
        id: &str,
        next_run: OffsetDateTime,
        retry_count: i32,
    ) -> Result<(), Self::Error> {
        let result = sqlx::query(RESCHEDULE_TASK)
            .bind(id)
            .bind(Status::Pending.as_str())
            .bind(next_run)
            .bind(retry_count)
            .bind(OffsetDateTime::now_utc())
            .execute(&self.pool)
            .await?;
        require_row_updated(result.rows_affected(), id)
    }

    async fn finish(&self, id: &str, business_status: &str) -> Result<(), Self::Error> {
        let result = sqlx::query(FINISH_TASK)
            .bind(id)
            .bind(Status::Finish.as_str())
            .bind(business_status)
            .bind(OffsetDateTime::now_utc())
            .execute(&self.pool)
            .await?;
        require_row_updated(result.rows_affected(), id)
    }

    async fn recover_stalled(&self, stuck_before: OffsetDateTime) -> Result<u64, Self::Error> {
        let result = sqlx::query(RECOVER_STALLED)
            .bind(Status::Pending.as_str())
            .bind(Status::Running.as_str())
            .bind(OffsetDateTime::now_utc())
            .bind(stuck_before)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    async fn find(&self, id: &str) -> Result<Option<Task>, Self::Error> {
        let row = sqlx::query(FIND_TASK)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(task_from_row).transpose()
    }
}
