# scheduler

A durable, retrying, recurring background-job scheduler for Rust whose storage layer is pluggable across any database. The library owns the engine and the task lifecycle; you own the job logic and a single storage trait implementation for your database. Fully async on `tokio`, no Redis, no message broker.

A database row *is* a job. The database is the source of truth, so jobs survive restarts. The engine repeatedly finds due jobs, atomically claims them, runs them, and reschedules the ones that need to run again.

## The model

```
   [ insert ]
       │
       ▼
   ┌───────┐  status=New, business_status="Pending", retry_count=0, schedule_time=T
   │  New  │
   └───────┘
       │  engine scan: status IN (New,Pending) AND business_status='Pending'
       │              AND schedule_time <= now  →  atomic claim
       ▼
   ┌─────────┐
   │ Running │  engine runs your Workflow for this task
   └─────────┘
       │
   ┌───┴───────────────┐
   ▼                   ▼
 Done / GiveUp      Retry / RetryAt
   │                   │  status=Pending, new schedule_time (+retry_count for Retry)
   ▼                   └──▶ engine re-picks Pending on the next scan → loops
 ┌────────┐
 │ Finish │  terminal (COMPLETED / RETRIES_EXCEEDED / …) — never runs again
 └────────┘
```

| Concept | Type | Meaning |
|---|---|---|
| `Task` | struct | One row: `id`, `name`, `payload` (`serde_json::Value`), `schedule_time`, `retry_count`, `status`, `business_status`, lock columns, timestamps |
| `NewTask` | struct | Insert shape: `id`, `name`, `payload`, first `schedule_time` |
| `Status` | enum | `New`, `Running`, `Pending`, `Finish` — the engine's lifecycle |
| `business_status` | `String` | Yours. `"Pending"` means claimable; anything else (e.g. `"REVOKED"`) is a cancellation guard; on `Finish` it carries the terminal reason |
| `Outcome` | enum | Returned by a workflow: `Done(bs)`, `Retry`, `RetryAt(time)`, `GiveUp(bs)` |
| `BackoffConfig` | struct | `start_after` + ordered `(interval, count)` windows; `delay(retry_count)` returns `None` when retries are exhausted |

## Architecture

```
LIBRARY (generic, async, no DB assumptions)          YOUR CRATE
┌──────────────────────────────────────┐            ┌──────────────────────────────┐
│ engine loop (poll→claim→run→apply)     │  calls →   │ Workflows (job logic)         │
│ retry/backoff + recurrence             │            │ Dispatcher (name→workflow)    │
│ Task type + status lifecycle           │  calls →   ├──────────────────────────────┤
│ SchedulerStore TRAIT ──────────────────┼──────────▶ │ ONE SchedulerStore impl        │
└──────────────────────────────────────┘            │   for your DB (PG/Mongo/…)     │
                                                     └──────────────────────────────┘
```

Crate layout:

| Module | Contents |
|---|---|
| `task` | `Task`, `NewTask`, `Status`, `business_status` constants, `deterministic_task_id` |
| `outcome` | `Outcome` |
| `backoff` | `BackoffConfig`, `RetryWindow` |
| `store` | `SchedulerStore` trait |
| `workflow` | `Workflow`, `Dispatcher` traits |
| `engine` | `Config`, `Metrics`, `Engine`, `run`, `run_until`, `plan_transition`, `apply_outcome` |
| `memory` | `MemoryStore` — feature `memory` (default), for tests |
| `postgres` | `PgStore` — feature `postgres` (via `sqlx`) |

Core dependencies: `tokio`, `serde`, `serde_json`, `time`, `uuid`, `async-trait`, `thiserror`, `tracing`. Nothing database-specific unless you enable an adapter feature.

## Quick start

```toml
[dependencies]
scheduler = { path = "scheduler", features = ["postgres"] }
```

### 1. Storage — pick an adapter or implement the trait

```rust
use scheduler::postgres::PgStore;

let store = PgStore::connect("postgres://user:pass@localhost/db").await?;
store.ensure_schema().await?;
```

### 2. Job logic — a `Workflow` per task name

```rust
use scheduler::{Outcome, Task, Workflow, async_trait};
use time::{Duration, OffsetDateTime};

struct ForecastWorkflow;

#[async_trait]
impl Workflow for ForecastWorkflow {
    async fn execute(&self, task: &Task) -> Outcome {
        let merchant_id = task.payload["merchant_id"].as_str().unwrap_or_default();
        match run_forecast(merchant_id).await {
            Ok(()) => Outcome::RetryAt(OffsetDateTime::now_utc() + Duration::hours(6)),
            Err(error) if error.is_transient() => Outcome::Retry,
            Err(_) => Outcome::give_up("FORECAST_FAILED"),
        }
    }
}
```

### 3. Routing — a `Dispatcher`

```rust
use scheduler::{Dispatcher, Task, Workflow};

struct MyDispatcher;

impl Dispatcher for MyDispatcher {
    fn resolve(&self, task: &Task) -> Option<Box<dyn Workflow>> {
        match task.name.as_str() {
            "forecast" => Some(Box::new(ForecastWorkflow)),
            _ => None,
        }
    }
}
```

Closures work too: `|task: &Task| -> Option<Box<dyn Workflow>> { ... }` implements `Dispatcher`.

### 4. Run the engine

```rust
use scheduler::Config;

scheduler::run(store.clone(), MyDispatcher, Config::default()).await;
```

`run` stops on ctrl-c. `run_until(store, dispatcher, config, shutdown_future)` stops when your future resolves. Both finish in-flight tasks before returning and hand back an `Arc<Metrics>`.

Run as many instances as you like against one store. Coordination is entirely the atomicity of `claim_due`.

### Schedule a job

```rust
use scheduler::{NewTask, SchedulerStore, deterministic_task_id};
use serde_json::json;

store.insert(NewTask {
    id: deterministic_task_id("forecast", merchant_id),
    name: "forecast".into(),
    payload: json!({ "merchant_id": merchant_id }),
    schedule_time: OffsetDateTime::now_utc() + Duration::hours(6),
}).await?;
```

A deterministic id means one live timer per logical job: inserting the same job twice is a duplicate-key error you can treat as "already scheduled".

### Cancel a job

Set `business_status` to anything other than `"Pending"`, e.g. `"REVOKED"`. `claim_due` only claims `"Pending"` rows, so the task is never picked up again, and the row stays for audit. Both adapters expose `set_business_status(id, status)`; with your own adapter this is a one-column update.

## Engine behaviour

Each `Config` field and what it drives:

| Field | Default | Effect |
|---|---|---|
| `worker_id` | random UUID v4 | Written to `locked_by` on claim; appears in tracing spans |
| `poll_interval` | 1s | How often the engine calls `claim_due` |
| `batch_size` | 10 | Max tasks per claim **and** max concurrent tasks per engine instance (it claims `batch_size - in_flight`) |
| `stalled_after` | 5min | A `Running` task with `locked_at` older than this is considered crashed |
| `stalled_check_interval` | 1min | How often `recover_stalled` runs (also once at startup) |
| `backoff` | `0s` start, `60s×5, 300s×5, 1800s×5` | Retry delays |

Per claimed task the engine spawns a tokio task that does:

1. `dispatcher.resolve(&task)` — `None` finishes the task with `business_status = "UNKNOWN_WORKFLOW"` so it never sits in `Running`.
2. `workflow.execute(&task).await`
3. Apply the `Outcome`:

| Outcome | Store call | Metric |
|---|---|---|
| `Done(bs)` | `finish(id, bs)` | `finished` |
| `GiveUp(bs)` | `finish(id, bs)` | `failed` |
| `Retry` | `delay(retry_count + 1)` → `Some(d)`: `reschedule(id, now + d, retry_count + 1)` | `retried` |
| `Retry` (exhausted) | `finish(id, "RETRIES_EXCEEDED")` | `failed` |
| `RetryAt(t)` | `reschedule(id, t, 0)` | `retried` |

`plan_transition` is a pure function of `(outcome, retry_count, backoff, now)`; `apply_outcome` persists it. Both are public if you want to drive the store from your own loop.

### Backoff

```rust
use scheduler::{BackoffConfig, RetryWindow};
use time::Duration;

let backoff = BackoffConfig::new(
    Duration::ZERO,
    vec![RetryWindow::seconds(30, 3), RetryWindow::seconds(300, 2)],
);
```

`delay(n)` walks the windows cumulatively: retries 1–3 wait 30s, retries 4–5 wait 300s, retry 6 → `None` → the engine finishes the task with `RETRIES_EXCEEDED`. `delay(0)` returns `start_after`, useful for computing a first `schedule_time`.

### Recurrence

Return `Outcome::RetryAt(next_time)`. The task goes back to `Pending` with `retry_count = 0` and is claimed again when due. A recurring task lives forever until a workflow returns `Done`/`GiveUp` or someone changes its `business_status`. The [example crate](examples/recurring-heartbeat) is exactly this.

### Stalled-task recovery

If a worker dies mid-execution the row stays `Running`. Every `stalled_check_interval` the engine calls `recover_stalled(now - stalled_after)`, which returns those rows to `Pending`; the next scan re-runs them. A workflow that panics is handled the same way: the panic is logged and the row is left for recovery. Pick `stalled_after` comfortably larger than your slowest workflow.

### Observability

`Metrics` (via `Engine::metrics()` or the return value of `run`) counts `claimed`, `finished`, `retried`, `failed`, `recovered`, `store_errors`. Every task execution runs inside a `tracing` span `task{id, name, retry_count, worker_id}`; claim, recovery, and store failures are logged at `error`/`warn`.

## Guarantees

- **Durable** — jobs live in your database and survive restarts.
- **At-least-once** — a task can execute more than once (worker crash after the workflow ran but before `finish` was written, or stalled recovery firing while a slow worker is still finishing). Exactly-once is not offered and is impossible in general. Make workflows idempotent, and use deterministic ids so re-scheduling the same logical job dedupes at insert time.
- **Cancellable without deletion** — the `business_status == "Pending"` guard in `claim_due`.
- **Safe horizontal scaling** — any number of engine instances, no broker; `claim_due` atomicity is the only coordination.

## Implementing `SchedulerStore` for a new database

```rust
#[async_trait]
pub trait SchedulerStore: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn insert(&self, task: NewTask) -> Result<Task, Self::Error>;
    async fn claim_due(&self, now: OffsetDateTime, limit: usize, worker_id: &str) -> Result<Vec<Task>, Self::Error>;
    async fn reschedule(&self, id: &str, next_run: OffsetDateTime, retry_count: i32) -> Result<(), Self::Error>;
    async fn finish(&self, id: &str, business_status: &str) -> Result<(), Self::Error>;
    async fn recover_stalled(&self, stuck_before: OffsetDateTime) -> Result<u64, Self::Error>;
    async fn find(&self, id: &str) -> Result<Option<Task>, Self::Error>;
}
```

Semantics each method must honour (`NewTask::into_task(now)` gives you the exact initial row):

- `insert` — status `New`, `business_status = "Pending"`, `retry_count = 0`. A duplicate id must be detectable (return an error, or the existing row).
- `claim_due` — **the correctness primitive.** Atomically select rows with `status IN (New, Pending) AND business_status = "Pending" AND schedule_time <= now`, ordered by `schedule_time`, at most `limit`, and in the same atomic step set `status = Running`, `locked_by = worker_id`, `locked_at = now`. Two concurrent callers must never receive the same row. `limit == 0` returns nothing.
- `reschedule` — `status = Pending`, `schedule_time = next_run`, `retry_count = retry_count`, clear `locked_by`/`locked_at`. Unknown id is an error.
- `finish` — `status = Finish`, `business_status = business_status`, clear lock. Unknown id is an error.
- `recover_stalled` — rows with `status = Running AND locked_at < stuck_before` → `Pending`, lock cleared; return the count.
- `find` — by id, `None` if absent.

How the atomic claim maps onto common databases:

| Backend | Atomic-claim primitive | Fit |
|---|---|---|
| **PostgreSQL / MySQL 8+ / SQLite** | `UPDATE … SET status='running', locked_by=$w, locked_at=$now WHERE id IN (SELECT id FROM tasks WHERE status IN ('new','pending') AND business_status='Pending' AND schedule_time <= $now ORDER BY schedule_time LIMIT $n FOR UPDATE SKIP LOCKED) RETURNING *` — this is what `PgStore` does | ideal |
| **MongoDB** | `findOneAndUpdate` with the claim filter and `$set` of the lock fields, looped up to `limit`; each call is atomic per document | good |
| **DynamoDB** | conditional `UpdateItem` (`ConditionExpression` on `status`/`business_status`/`schedule_time`) per candidate from a GSI on `(status, schedule_time)`; a failed condition means another worker won | good |
| **Cassandra / wide-column** | lightweight transactions (`IF status = 'pending'`) over time-bucketed partitions — possible, but queue workloads produce tombstone-heavy partitions; prefer another store if you can | caveat |

Then run the shared contract suite against your adapter: `scheduler/tests/common/store_contract.rs` is generic over `S: SchedulerStore` and is exactly what `MemoryStore` and `PgStore` are tested with (see `tests/memory_store.rs` / `tests/postgres_store.rs`).

## PostgreSQL adapter

Feature `postgres`. Table `scheduler_tasks`, created by [`migrations/0001_create_scheduler_tasks.sql`](scheduler/migrations/0001_create_scheduler_tasks.sql) (also embedded as `scheduler::postgres::SCHEMA_SQL`; `PgStore::ensure_schema()` applies it idempotently):

| Column | Type |
|---|---|
| `id` | `TEXT PRIMARY KEY` |
| `name` | `TEXT` |
| `payload` | `JSONB` |
| `schedule_time` | `TIMESTAMPTZ` |
| `retry_count` | `INTEGER` |
| `status` | `TEXT` (`new`/`running`/`pending`/`finish`) |
| `business_status` | `TEXT` |
| `locked_by` | `TEXT NULL` |
| `locked_at` | `TIMESTAMPTZ NULL` |
| `created_at`, `updated_at` | `TIMESTAMPTZ` |

Index: `(status, schedule_time)`. The adapter enables `sqlx` with `runtime-tokio, postgres, time, json` only; add `tls-rustls` or `tls-native-tls` in your own `Cargo.toml` if you need TLS.

## Example

[`examples/recurring-heartbeat`](examples/recurring-heartbeat): a workflow that logs and reschedules itself every `HEARTBEAT_INTERVAL_SECS` via `RetryAt`, a dispatcher, an idempotent deterministic-id insert, and `run()` against Postgres.

```sh
docker run -d --name scheduler-pg \
  -e POSTGRES_PASSWORD=scheduler -e POSTGRES_USER=scheduler -e POSTGRES_DB=scheduler \
  -p 55432:5432 postgres:16-alpine

HEARTBEAT_INTERVAL_SECS=3 cargo run -p recurring-heartbeat
```

Start it twice: the second process logs `heartbeat already scheduled` and both instances share the load.

## Tests

```sh
cargo test
```

Runs unit tests (backoff walk, status parsing, transition planning) and integration tests on `MemoryStore`: the store contract, outcome application, and the engine lifecycle — insert → claim → retry → finish, recurrence, cancellation via `business_status`, stalled recovery, unknown workflow, graceful shutdown, batch cap, and two engines on one store never double-executing.

Postgres tests are `#[ignore]` and need a database:

```sh
SCHEDULER_TEST_DATABASE_URL=postgres://scheduler:scheduler@localhost:55432/scheduler \
  cargo test --features postgres --test postgres_store -- --ignored --test-threads=1
```

They truncate `scheduler_tasks`, so point them at a scratch database.

## Non-goals

- No job/business logic in the library.
- No Redis or broker; an optional low-latency wake-up notifier could be added later but is not needed for correctness.
- No exactly-once. At-least-once plus idempotent workflows and deterministic ids is the contract.
- Core crate stays DB-neutral; every backend is an optional feature or adapter.
