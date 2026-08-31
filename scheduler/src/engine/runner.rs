use std::future::Future;
use std::sync::Arc;

use time::OffsetDateTime;
use tokio::task::{JoinError, JoinSet};
use tokio::time::{MissedTickBehavior, interval};
use tracing::Instrument;

use crate::engine::config::Config;
use crate::engine::metrics::Metrics;
use crate::engine::transition::apply_outcome;
use crate::outcome::Outcome;
use crate::store::SchedulerStore;
use crate::task::{Task, business_status};
use crate::workflow::Dispatcher;

pub struct Engine<S, D> {
    store: S,
    dispatcher: Arc<D>,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
}

impl<S: SchedulerStore, D: Dispatcher> Engine<S, D> {
    pub fn new(store: S, dispatcher: D, config: Config) -> Self {
        Self {
            store,
            dispatcher: Arc::new(dispatcher),
            config: Arc::new(config),
            metrics: Arc::new(Metrics::new()),
        }
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn run_until(self, shutdown: impl Future<Output = ()> + Send) -> Arc<Metrics> {
        let mut in_flight = JoinSet::new();
        let mut poll_ticks = build_ticker(to_std_duration(self.config.poll_interval));
        let mut stalled_ticks = build_ticker(to_std_duration(self.config.stalled_check_interval));
        let mut shutdown = std::pin::pin!(shutdown);

        tracing::info!(worker_id = %self.config.worker_id, "scheduler engine started");
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = poll_ticks.tick() => self.claim_and_spawn_due_tasks(&mut in_flight).await,
                _ = stalled_ticks.tick() => self.recover_stalled_tasks().await,
                Some(joined) = in_flight.join_next(), if !in_flight.is_empty() => log_join_result(joined),
            }
        }

        tracing::info!(
            worker_id = %self.config.worker_id,
            in_flight = in_flight.len(),
            "shutdown requested, draining in-flight tasks"
        );
        while let Some(joined) = in_flight.join_next().await {
            log_join_result(joined);
        }
        tracing::info!(worker_id = %self.config.worker_id, "scheduler engine stopped");
        self.metrics
    }

    pub async fn run_until_ctrl_c(self) -> Arc<Metrics> {
        self.run_until(wait_for_ctrl_c()).await
    }

    async fn claim_and_spawn_due_tasks(&self, in_flight: &mut JoinSet<()>) {
        let capacity = self.config.batch_size.saturating_sub(in_flight.len());
        if capacity == 0 {
            return;
        }
        let now = OffsetDateTime::now_utc();
        match self
            .store
            .claim_due(now, capacity, &self.config.worker_id)
            .await
        {
            Ok(tasks) => {
                self.metrics.record_claimed(tasks.len() as u64);
                for task in tasks {
                    self.spawn_task_execution(in_flight, task);
                }
            }
            Err(error) => {
                self.metrics.record_store_error();
                tracing::error!(error = %error, "failed to claim due tasks");
            }
        }
    }

    fn spawn_task_execution(&self, in_flight: &mut JoinSet<()>, task: Task) {
        let span = tracing::info_span!(
            "task",
            id = %task.id,
            name = %task.name,
            retry_count = task.retry_count,
            worker_id = %self.config.worker_id
        );
        in_flight.spawn(
            execute_claimed_task(
                self.store.clone(),
                Arc::clone(&self.dispatcher),
                Arc::clone(&self.config),
                Arc::clone(&self.metrics),
                task,
            )
            .instrument(span),
        );
    }

    async fn recover_stalled_tasks(&self) {
        let stuck_before = OffsetDateTime::now_utc() - self.config.stalled_after;
        match self.store.recover_stalled(stuck_before).await {
            Ok(0) => {}
            Ok(recovered) => {
                self.metrics.record_recovered(recovered);
                tracing::warn!(recovered, "returned stalled tasks to pending");
            }
            Err(error) => {
                self.metrics.record_store_error();
                tracing::error!(error = %error, "failed to recover stalled tasks");
            }
        }
    }
}

async fn execute_claimed_task<S: SchedulerStore, D: Dispatcher>(
    store: S,
    dispatcher: Arc<D>,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    task: Task,
) {
    let outcome = resolve_and_execute(dispatcher.as_ref(), &task).await;
    let now = OffsetDateTime::now_utc();
    match apply_outcome(&store, &task, &outcome, &config.backoff, &metrics, now).await {
        Ok(transition) => tracing::debug!(?transition, "applied workflow outcome"),
        Err(error) => {
            metrics.record_store_error();
            tracing::error!(error = %error, ?outcome, "failed to persist workflow outcome");
        }
    }
}

async fn resolve_and_execute<D: Dispatcher>(dispatcher: &D, task: &Task) -> Outcome {
    match dispatcher.resolve(task) {
        Some(workflow) => workflow.execute(task).await,
        None => {
            tracing::error!("no workflow registered for task name");
            Outcome::give_up(business_status::UNKNOWN_WORKFLOW)
        }
    }
}

fn log_join_result(joined: Result<(), JoinError>) {
    if let Err(error) = joined {
        tracing::error!(error = %error, "task execution aborted; stalled recovery will requeue it");
    }
}

fn build_ticker(period: std::time::Duration) -> tokio::time::Interval {
    let mut ticker = interval(period.max(std::time::Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker
}

fn to_std_duration(duration: time::Duration) -> std::time::Duration {
    duration.try_into().unwrap_or(std::time::Duration::ZERO)
}

async fn wait_for_ctrl_c() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %error, "failed to listen for ctrl-c; engine will run until killed");
        std::future::pending::<()>().await;
    }
}

pub async fn run<S: SchedulerStore, D: Dispatcher>(
    store: S,
    dispatcher: D,
    config: Config,
) -> Arc<Metrics> {
    Engine::new(store, dispatcher, config)
        .run_until_ctrl_c()
        .await
}

pub async fn run_until<S: SchedulerStore, D: Dispatcher>(
    store: S,
    dispatcher: D,
    config: Config,
    shutdown: impl Future<Output = ()> + Send,
) -> Arc<Metrics> {
    Engine::new(store, dispatcher, config)
        .run_until(shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negative_durations_collapse_to_zero() {
        assert_eq!(
            to_std_duration(time::Duration::seconds(-5)),
            std::time::Duration::ZERO
        );
        assert_eq!(
            to_std_duration(time::Duration::milliseconds(250)),
            std::time::Duration::from_millis(250)
        );
    }
}
