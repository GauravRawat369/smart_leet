use async_trait::async_trait;

use crate::outcome::Outcome;
use crate::task::Task;

#[async_trait]
pub trait Workflow: Send + Sync {
    async fn execute(&self, task: &Task) -> Outcome;
}

pub trait Dispatcher: Send + Sync + 'static {
    fn resolve(&self, task: &Task) -> Option<Box<dyn Workflow>>;
}

impl<F> Dispatcher for F
where
    F: Fn(&Task) -> Option<Box<dyn Workflow>> + Send + Sync + 'static,
{
    fn resolve(&self, task: &Task) -> Option<Box<dyn Workflow>> {
        self(task)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::NewTask;
    use serde_json::Value;
    use time::OffsetDateTime;

    struct EchoWorkflow;

    #[async_trait]
    impl Workflow for EchoWorkflow {
        async fn execute(&self, task: &Task) -> Outcome {
            Outcome::Done(task.name.clone())
        }
    }

    fn sample_task(name: &str) -> Task {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        NewTask {
            id: "task-1".to_owned(),
            name: name.to_owned(),
            payload: Value::Null,
            schedule_time: now,
        }
        .into_task(now)
    }

    fn route_echo_only(task: &Task) -> Option<Box<dyn Workflow>> {
        (task.name == "echo").then(|| Box::new(EchoWorkflow) as Box<dyn Workflow>)
    }

    #[tokio::test]
    async fn closure_dispatcher_routes_known_task_name() {
        let workflow = route_echo_only
            .resolve(&sample_task("echo"))
            .expect("resolved");
        assert_eq!(
            workflow.execute(&sample_task("echo")).await,
            Outcome::Done("echo".to_owned())
        );
    }

    #[test]
    fn closure_dispatcher_returns_none_for_unknown_task_name() {
        assert!(route_echo_only.resolve(&sample_task("other")).is_none());
    }
}
