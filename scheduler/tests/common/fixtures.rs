use scheduler::NewTask;
use serde_json::json;
use time::OffsetDateTime;

pub const WORKER: &str = "worker-a";

pub fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
        .replace_nanosecond(0)
        .expect("valid time")
}

pub fn new_task(id: &str, schedule_time: OffsetDateTime) -> NewTask {
    NewTask {
        id: id.to_owned(),
        name: "job".to_owned(),
        payload: json!({ "id": id }),
        schedule_time,
    }
}
