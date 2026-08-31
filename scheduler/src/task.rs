use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

pub mod business_status {
    pub const PENDING: &str = "Pending";
    pub const COMPLETED: &str = "COMPLETED";
    pub const RETRIES_EXCEEDED: &str = "RETRIES_EXCEEDED";
    pub const UNKNOWN_WORKFLOW: &str = "UNKNOWN_WORKFLOW";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    New,
    Running,
    Pending,
    Finish,
}

impl Status {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Running => "running",
            Self::Pending => "pending",
            Self::Finish => "finish",
        }
    }

    pub const fn is_claimable(self) -> bool {
        matches!(self, Self::New | Self::Pending)
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Finish)
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown task status `{0}`")]
pub struct UnknownStatus(pub String);

impl FromStr for Status {
    type Err = UnknownStatus;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "new" => Ok(Self::New),
            "running" => Ok(Self::Running),
            "pending" => Ok(Self::Pending),
            "finish" => Ok(Self::Finish),
            other => Err(UnknownStatus(other.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub name: String,
    pub payload: Value,
    #[serde(with = "time::serde::rfc3339")]
    pub schedule_time: OffsetDateTime,
    pub retry_count: i32,
    pub status: Status,
    pub business_status: String,
    pub locked_by: Option<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub locked_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl Task {
    pub fn is_claimable_at(&self, now: OffsetDateTime) -> bool {
        self.status.is_claimable()
            && self.business_status == business_status::PENDING
            && self.schedule_time <= now
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewTask {
    pub id: String,
    pub name: String,
    pub payload: Value,
    #[serde(with = "time::serde::rfc3339")]
    pub schedule_time: OffsetDateTime,
}

impl NewTask {
    pub fn into_task(self, now: OffsetDateTime) -> Task {
        Task {
            id: self.id,
            name: self.name,
            payload: self.payload,
            schedule_time: self.schedule_time,
            retry_count: 0,
            status: Status::New,
            business_status: business_status::PENDING.to_owned(),
            locked_by: None,
            locked_at: None,
            created_at: now,
            updated_at: now,
        }
    }
}

pub fn deterministic_task_id(name: &str, key: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, format!("{name}:{key}").as_bytes()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp")
    }

    #[test]
    fn status_round_trips_through_string() {
        for status in [
            Status::New,
            Status::Running,
            Status::Pending,
            Status::Finish,
        ] {
            assert_eq!(status.as_str().parse::<Status>(), Ok(status));
        }
    }

    #[test]
    fn unknown_status_string_is_rejected() {
        assert_eq!(
            "bogus".parse::<Status>(),
            Err(UnknownStatus("bogus".to_owned()))
        );
    }

    #[test]
    fn only_new_and_pending_are_claimable() {
        assert!(Status::New.is_claimable());
        assert!(Status::Pending.is_claimable());
        assert!(!Status::Running.is_claimable());
        assert!(!Status::Finish.is_claimable());
    }

    #[test]
    fn new_task_starts_in_initial_lifecycle_state() {
        let now = fixed_now();
        let task = NewTask {
            id: "task-1".to_owned(),
            name: "job".to_owned(),
            payload: json!({ "k": 1 }),
            schedule_time: now,
        }
        .into_task(now);

        assert_eq!(task.status, Status::New);
        assert_eq!(task.business_status, business_status::PENDING);
        assert_eq!(task.retry_count, 0);
        assert_eq!(task.locked_by, None);
        assert_eq!(task.locked_at, None);
        assert_eq!(task.created_at, now);
        assert_eq!(task.updated_at, now);
    }

    #[test]
    fn task_is_claimable_only_when_due_pending_and_unrevoked() {
        let now = fixed_now();
        let mut task = NewTask {
            id: "task-1".to_owned(),
            name: "job".to_owned(),
            payload: Value::Null,
            schedule_time: now,
        }
        .into_task(now);

        assert!(task.is_claimable_at(now));
        assert!(!task.is_claimable_at(now - time::Duration::seconds(1)));

        task.business_status = "REVOKED".to_owned();
        assert!(!task.is_claimable_at(now));

        task.business_status = business_status::PENDING.to_owned();
        task.status = Status::Running;
        assert!(!task.is_claimable_at(now));
    }

    #[test]
    fn task_serializes_timestamps_as_rfc3339() {
        let now = fixed_now();
        let task = NewTask {
            id: "task-1".to_owned(),
            name: "job".to_owned(),
            payload: Value::Null,
            schedule_time: now,
        }
        .into_task(now);

        let encoded = serde_json::to_value(&task).expect("serializable");
        assert_eq!(encoded["schedule_time"], "2023-11-14T22:13:20Z");
        assert_eq!(encoded["status"], "new");
        assert_eq!(encoded["locked_at"], Value::Null);

        let decoded: Task = serde_json::from_value(encoded).expect("deserializable");
        assert_eq!(decoded, task);
    }

    #[test]
    fn deterministic_id_is_stable_for_same_inputs() {
        assert_eq!(
            deterministic_task_id("forecast", "merchant-1"),
            deterministic_task_id("forecast", "merchant-1")
        );
    }

    #[test]
    fn deterministic_id_differs_across_inputs() {
        assert_ne!(
            deterministic_task_id("forecast", "merchant-1"),
            deterministic_task_id("forecast", "merchant-2")
        );
        assert_ne!(
            deterministic_task_id("forecast", "merchant-1"),
            deterministic_task_id("refund", "merchant-1")
        );
    }
}
