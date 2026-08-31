mod constants;
mod dispatcher;
mod workflows;

use scheduler::postgres::{PgStore, PgStoreError};
use scheduler::{Config, NewTask, SchedulerStore, deterministic_task_id};
use serde_json::json;
use time::{Duration, OffsetDateTime};
use tracing_subscriber::EnvFilter;

use crate::constants::{
    DATABASE_URL_ENV, DEFAULT_DATABASE_URL, DEFAULT_HEARTBEAT_INTERVAL_SECS, DEFAULT_LOG_FILTER,
    HEARTBEAT_INTERVAL_ENV, HEARTBEAT_NAME, HEARTBEAT_TASK_NAME, POLL_INTERVAL_SECONDS,
    STALLED_AFTER_MINUTES,
};
use crate::dispatcher::ExampleDispatcher;

fn read_database_url() -> String {
    std::env::var(DATABASE_URL_ENV).unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_owned())
}

fn read_heartbeat_interval_secs() -> i64 {
    std::env::var(HEARTBEAT_INTERVAL_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_SECS)
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| DEFAULT_LOG_FILTER.into()),
        )
        .init();
}

async fn schedule_heartbeat_once(
    store: &PgStore,
    name: &str,
    interval_secs: i64,
) -> Result<(), PgStoreError> {
    let task = NewTask {
        id: deterministic_task_id(HEARTBEAT_TASK_NAME, name),
        name: HEARTBEAT_TASK_NAME.to_owned(),
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
    schedule_heartbeat_once(&store, HEARTBEAT_NAME, read_heartbeat_interval_secs()).await?;

    let config = Config::default()
        .with_poll_interval(Duration::seconds(POLL_INTERVAL_SECONDS))
        .with_stalled_after(Duration::minutes(STALLED_AFTER_MINUTES));
    let metrics = scheduler::run(store, ExampleDispatcher, config).await;
    tracing::info!(?metrics, "engine stopped");
    Ok(())
}
