CREATE TABLE IF NOT EXISTS scheduler_tasks (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    payload JSONB NOT NULL,
    schedule_time TIMESTAMPTZ NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    business_status TEXT NOT NULL,
    locked_by TEXT,
    locked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS scheduler_tasks_status_schedule_time_idx
    ON scheduler_tasks (status, schedule_time);
