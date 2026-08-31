#![forbid(unsafe_code)]

pub mod backoff;
pub mod outcome;
pub mod task;

pub use backoff::{BackoffConfig, RetryWindow};
pub use outcome::Outcome;
pub use task::{NewTask, Status, Task, UnknownStatus, business_status, deterministic_task_id};
