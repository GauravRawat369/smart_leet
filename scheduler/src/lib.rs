#![forbid(unsafe_code)]

pub mod outcome;
pub mod task;

pub use outcome::Outcome;
pub use task::{NewTask, Status, Task, UnknownStatus, business_status, deterministic_task_id};
