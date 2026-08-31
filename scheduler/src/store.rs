use async_trait::async_trait;
use time::OffsetDateTime;

use crate::task::{NewTask, Task};

#[async_trait]
pub trait SchedulerStore: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn insert(&self, task: NewTask) -> Result<Task, Self::Error>;

    async fn claim_due(
        &self,
        now: OffsetDateTime,
        limit: usize,
        worker_id: &str,
    ) -> Result<Vec<Task>, Self::Error>;

    async fn reschedule(
        &self,
        id: &str,
        next_run: OffsetDateTime,
        retry_count: i32,
    ) -> Result<(), Self::Error>;

    async fn finish(&self, id: &str, business_status: &str) -> Result<(), Self::Error>;

    async fn recover_stalled(&self, stuck_before: OffsetDateTime) -> Result<u64, Self::Error>;

    async fn find(&self, id: &str) -> Result<Option<Task>, Self::Error>;
}
