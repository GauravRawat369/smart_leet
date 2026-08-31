use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::store::SchedulerStore;
use crate::task::{NewTask, Status, Task};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MemoryStoreError {
    #[error("task `{0}` already exists")]
    DuplicateTask(String),
    #[error("task `{0}` not found")]
    TaskNotFound(String),
}

#[derive(Debug, Clone, Default)]
pub struct MemoryStore {
    tasks: Arc<Mutex<HashMap<String, Task>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set_business_status(
        &self,
        id: &str,
        business_status: &str,
    ) -> Result<(), MemoryStoreError> {
        let mut tasks = self.tasks.lock().await;
        let task = tasks
            .get_mut(id)
            .ok_or_else(|| MemoryStoreError::TaskNotFound(id.to_owned()))?;
        task.business_status = business_status.to_owned();
        task.updated_at = OffsetDateTime::now_utc();
        Ok(())
    }

    pub async fn all_tasks(&self) -> Vec<Task> {
        let tasks = self.tasks.lock().await;
        let mut all: Vec<Task> = tasks.values().cloned().collect();
        all.sort_by(|left, right| left.id.cmp(&right.id));
        all
    }

    pub async fn task_count(&self) -> usize {
        self.tasks.lock().await.len()
    }
}

fn compare_by_schedule_then_id(left: &Task, right: &Task) -> std::cmp::Ordering {
    left.schedule_time
        .cmp(&right.schedule_time)
        .then_with(|| left.id.cmp(&right.id))
}

fn mark_running(task: &mut Task, now: OffsetDateTime, worker_id: &str) {
    task.status = Status::Running;
    task.locked_by = Some(worker_id.to_owned());
    task.locked_at = Some(now);
    task.updated_at = now;
}

fn release_to_pending(task: &mut Task, now: OffsetDateTime) {
    task.status = Status::Pending;
    task.locked_by = None;
    task.locked_at = None;
    task.updated_at = now;
}

fn is_stalled_before(task: &Task, stuck_before: OffsetDateTime) -> bool {
    task.status == Status::Running
        && task
            .locked_at
            .is_some_and(|locked_at| locked_at < stuck_before)
}

#[async_trait]
impl SchedulerStore for MemoryStore {
    type Error = MemoryStoreError;

    async fn insert(&self, task: NewTask) -> Result<Task, Self::Error> {
        let mut tasks = self.tasks.lock().await;
        if tasks.contains_key(&task.id) {
            return Err(MemoryStoreError::DuplicateTask(task.id));
        }
        let task = task.into_task(OffsetDateTime::now_utc());
        tasks.insert(task.id.clone(), task.clone());
        Ok(task)
    }

    async fn claim_due(
        &self,
        now: OffsetDateTime,
        limit: usize,
        worker_id: &str,
    ) -> Result<Vec<Task>, Self::Error> {
        let mut tasks = self.tasks.lock().await;
        let mut due: Vec<&mut Task> = tasks
            .values_mut()
            .filter(|task| task.is_claimable_at(now))
            .collect();
        due.sort_by(|left, right| compare_by_schedule_then_id(left, right));
        Ok(due
            .into_iter()
            .take(limit)
            .map(|task| {
                mark_running(task, now, worker_id);
                task.clone()
            })
            .collect())
    }

    async fn reschedule(
        &self,
        id: &str,
        next_run: OffsetDateTime,
        retry_count: i32,
    ) -> Result<(), Self::Error> {
        let mut tasks = self.tasks.lock().await;
        let task = tasks
            .get_mut(id)
            .ok_or_else(|| MemoryStoreError::TaskNotFound(id.to_owned()))?;
        release_to_pending(task, OffsetDateTime::now_utc());
        task.schedule_time = next_run;
        task.retry_count = retry_count;
        Ok(())
    }

    async fn finish(&self, id: &str, business_status: &str) -> Result<(), Self::Error> {
        let mut tasks = self.tasks.lock().await;
        let task = tasks
            .get_mut(id)
            .ok_or_else(|| MemoryStoreError::TaskNotFound(id.to_owned()))?;
        task.status = Status::Finish;
        task.business_status = business_status.to_owned();
        task.locked_by = None;
        task.locked_at = None;
        task.updated_at = OffsetDateTime::now_utc();
        Ok(())
    }

    async fn recover_stalled(&self, stuck_before: OffsetDateTime) -> Result<u64, Self::Error> {
        let mut tasks = self.tasks.lock().await;
        let now = OffsetDateTime::now_utc();
        let mut recovered = 0;
        for task in tasks
            .values_mut()
            .filter(|task| is_stalled_before(task, stuck_before))
        {
            release_to_pending(task, now);
            recovered += 1;
        }
        Ok(recovered)
    }

    async fn find(&self, id: &str) -> Result<Option<Task>, Self::Error> {
        Ok(self.tasks.lock().await.get(id).cloned())
    }
}
