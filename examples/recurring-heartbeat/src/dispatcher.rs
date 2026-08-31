use scheduler::{Dispatcher, Task, Workflow};

use crate::workflows::{HEARTBEAT, HeartbeatWorkflow};

pub struct ExampleDispatcher;

impl Dispatcher for ExampleDispatcher {
    fn resolve(&self, task: &Task) -> Option<Box<dyn Workflow>> {
        match task.name.as_str() {
            HEARTBEAT => Some(Box::new(HeartbeatWorkflow)),
            _ => None,
        }
    }
}
