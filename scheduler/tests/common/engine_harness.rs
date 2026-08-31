use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration as StdDuration;

use scheduler::{
    Config, Dispatcher, Engine, Metrics, Outcome, SchedulerStore, Task, Workflow, async_trait,
};
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execution {
    pub id: String,
    pub retry_count: i32,
}

type OutcomeScript = Arc<dyn Fn(&Task, usize) -> Outcome + Send + Sync>;

#[derive(Clone)]
pub struct ScriptedWorkflow {
    executions: Arc<Mutex<Vec<Execution>>>,
    in_progress: Arc<AtomicUsize>,
    max_in_progress: Arc<AtomicUsize>,
    script: OutcomeScript,
}

impl ScriptedWorkflow {
    pub fn new(script: impl Fn(&Task, usize) -> Outcome + Send + Sync + 'static) -> Self {
        Self {
            executions: Arc::new(Mutex::new(Vec::new())),
            in_progress: Arc::new(AtomicUsize::new(0)),
            max_in_progress: Arc::new(AtomicUsize::new(0)),
            script: Arc::new(script),
        }
    }

    pub async fn executions(&self) -> Vec<Execution> {
        self.executions.lock().await.clone()
    }

    pub async fn execution_count(&self) -> usize {
        self.executions.lock().await.len()
    }

    pub fn max_in_progress(&self) -> usize {
        self.max_in_progress.load(Ordering::SeqCst)
    }

    fn enter(&self) {
        let current = self.in_progress.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_progress.fetch_max(current, Ordering::SeqCst);
    }

    fn leave(&self) {
        self.in_progress.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Workflow for ScriptedWorkflow {
    async fn execute(&self, task: &Task) -> Outcome {
        self.enter();
        let attempt = {
            let mut executions = self.executions.lock().await;
            executions.push(Execution {
                id: task.id.clone(),
                retry_count: task.retry_count,
            });
            executions
                .iter()
                .filter(|execution| execution.id == task.id)
                .count()
        };
        let outcome = (self.script)(task, attempt);
        tokio::time::sleep(StdDuration::from_millis(5)).await;
        self.leave();
        outcome
    }
}

pub struct SingleWorkflowDispatcher(pub ScriptedWorkflow);

impl Dispatcher for SingleWorkflowDispatcher {
    fn resolve(&self, _task: &Task) -> Option<Box<dyn Workflow>> {
        Some(Box::new(self.0.clone()))
    }
}

pub struct NoWorkflowDispatcher;

impl Dispatcher for NoWorkflowDispatcher {
    fn resolve(&self, _task: &Task) -> Option<Box<dyn Workflow>> {
        None
    }
}

pub fn fast_config(worker_id: &str) -> Config {
    Config::default()
        .with_worker_id(worker_id)
        .with_poll_interval(time::Duration::milliseconds(10))
        .with_stalled_check_interval(time::Duration::milliseconds(20))
        .with_stalled_after(time::Duration::seconds(30))
}

pub struct RunningEngine {
    shutdown: Option<oneshot::Sender<()>>,
    handle: JoinHandle<Arc<Metrics>>,
}

impl RunningEngine {
    pub fn start<S: SchedulerStore, D: Dispatcher>(
        store: S,
        dispatcher: D,
        config: Config,
    ) -> Self {
        let (shutdown, shutdown_signal) = oneshot::channel();
        let engine = Engine::new(store, dispatcher, config);
        let handle = tokio::spawn(async move {
            engine
                .run_until(async move {
                    let _ = shutdown_signal.await;
                })
                .await
        });
        Self {
            shutdown: Some(shutdown),
            handle,
        }
    }

    pub async fn stop(mut self) -> Arc<Metrics> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.handle.await.expect("engine task completed")
    }
}

pub async fn wait_until<F, Fut>(condition: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(10);
    while !condition().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within 10s"
        );
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

pub async fn settle() {
    tokio::time::sleep(StdDuration::from_millis(150)).await;
}
