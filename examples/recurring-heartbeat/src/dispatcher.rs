use scheduler::{Dispatcher, Task, Workflow};

use crate::constants::HEARTBEAT_TASK_NAME;
use crate::workflows::HeartbeatWorkflow;

pub struct ExampleDispatcher;

impl Dispatcher for ExampleDispatcher {
    fn resolve(&self, task: &Task) -> Option<Box<dyn Workflow>> {
        match task.name.as_str() {
            HEARTBEAT_TASK_NAME => Some(Box::new(HeartbeatWorkflow)),
            _ => None,
        }
    }
}
