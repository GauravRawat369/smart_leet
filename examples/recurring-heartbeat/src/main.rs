mod dispatcher;
mod workflows;

use scheduler::postgres::{PgStore, PgStoreError};
use scheduler::{Config, NewTask, SchedulerStore, deterministic_task_id};
use serde_json::json;
use time::{Duration, OffsetDateTime};
use tracing_subscriber::EnvFilter;

use crate::dispatcher::ExampleDispatcher;
use crate::workflows::HEARTBEAT;

fn read_database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://scheduler:scheduler@localhost:55432/scheduler".to_owned())
}

fn read_heartbeat_interval_secs() -> i64 {
    std::env::var("HEARTBEAT_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10)
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
}

async fn schedule_heartbeat_once(
    store: &PgStore,
    name: &str,
    interval_secs: i64,
) -> Result<(), PgStoreError> {
    let task = NewTask {
        id: deterministic_task_id(HEARTBEAT, name),
        name: HEARTBEAT.to_owned(),
        payload: json!({ "name": name, "interval_secs": interval_secs }),
        schedule_time: OffsetDateTime::now_utc(),
    };
    match store.insert(task).await {
        Ok(task) => tracing::info!(task_id = %task.id, "scheduled heartbeat"),
        Err(PgStoreError::DuplicateTask(id)) => {
            tracing::info!(task_id = %id, "heartbeat already scheduled")
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let store = PgStore::connect(&read_database_url()).await?;
    store.ensure_schema().await?;
    schedule_heartbeat_once(&store, "example", read_heartbeat_interval_secs()).await?;

    let config = Config::default()
        .with_poll_interval(Duration::seconds(1))
        .with_stalled_after(Duration::minutes(2));
    let metrics = scheduler::run(store, ExampleDispatcher, config).await;
    tracing::info!(?metrics, "engine stopped");
    Ok(())
}
