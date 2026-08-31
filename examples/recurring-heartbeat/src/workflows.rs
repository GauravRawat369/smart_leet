use scheduler::{Outcome, Task, Workflow, async_trait};
use time::{Duration, OffsetDateTime};

use crate::constants::DEFAULT_HEARTBEAT_INTERVAL_SECS;

pub struct HeartbeatWorkflow;

impl HeartbeatWorkflow {
    fn interval_from_payload(task: &Task) -> Duration {
        let seconds = task
            .payload
            .get("interval_secs")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_SECS);
        Duration::seconds(seconds)
    }
}

#[async_trait]
impl Workflow for HeartbeatWorkflow {
    async fn execute(&self, task: &Task) -> Outcome {
        let interval = Self::interval_from_payload(task);
        tracing::info!(
            task_id = %task.id,
            name = task.payload.get("name").and_then(serde_json::Value::as_str),
            interval_secs = interval.whole_seconds(),
            "heartbeat"
        );
        Outcome::RetryAt(OffsetDateTime::now_utc() + interval)
    }
}
