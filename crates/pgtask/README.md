# pgtask

Durable tasks and workflows that live entirely in PostgreSQL.

There is no broker, no coordinator, and no extension to install. If your database is up, your queue is up.

> **Warning**
> `pgtask` is under active development. It is not ready for production use.

## Installation

```console
cargo add pgtask tokio tokio-util serde_json
```

## Run a worker

A worker declares which tasks it can handle and claims work from a queue. Every claim takes a lease, so
when a worker dies its tasks return to the queue instead of disappearing with it.

```rust,no_run
use std::error::Error;

use pgtask::{
    core::{HandlerVersion, QueueName, RetryPolicy, TaskName},
    postgres::Store,
    worker::{HandlerRegistry, Worker, WorkerConfig},
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let store = Store::connect(&std::env::var("PGTASK_DATABASE_URL")?).await?;
    store.migrate().await?;

    let mut registry = HandlerRegistry::new();
    registry.register(
        TaskName::new("reports.render")?,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |task| async move { Ok(json!({ "attempt": task.attempt })) },
    );

    let config = WorkerConfig::new(QueueName::new("reports")?);
    Worker::new(store, registry, config)?.run(CancellationToken::new()).await?;
    Ok(())
}
```

Cancel the token to shut down. The worker stops claiming, finishes what it is already running, and lets
the rest expire back to the queue.

## Enqueue inside your transaction

This is the reason to keep the queue in the database. You pass your own transaction, so the task commits
with your data or not at all. Roll back and the task never existed.

```rust,no_run
use std::error::Error;

use pgtask::{
    core::{EnqueueRequest, QueueName, TaskName},
    postgres::Store,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let store = Store::connect(&std::env::var("PGTASK_DATABASE_URL")?).await?;

    let mut request = EnqueueRequest::new(TaskName::new("reports.render")?, json!({"report_id": "report-123"}));
    request.queue_name = QueueName::new("reports")?;

    let mut transaction = store.pool().begin().await?;
    // Your own writes belong here, in the same transaction.
    let result = Store::enqueue_on(&mut transaction, &request).await?;
    transaction.commit().await?;

    println!("enqueued {}", result.task_id);
    Ok(())
}
```

Without a transaction of your own, `store.enqueue(&request)` does the same on its own connection.

## Crates

This crate re-exports the others. Depend on it unless you need one layer on its own.

| Crate | What it holds |
| --- | --- |
| `pgtask-core` | Task, queue, schedule, and retry types shared by every layer |
| `pgtask-postgres` | The storage protocol: migrate, enqueue, claim, complete |
| `pgtask-worker` | The worker runtime, scheduler, and retention loop |
| `pgtask-otel` | OpenTelemetry spans, metrics, and trace propagation |

## Documentation

[kludex.github.io/pgtask](https://kludex.github.io/pgtask/)
