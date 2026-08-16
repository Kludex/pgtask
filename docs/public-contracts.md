# Public contracts

## Version

`0.1.0` is the first integration release. Its public surface is versioned, but it is not the 1.0 freeze. Pilot feedback may still remove or rename APIs before 1.0. Every breaking change before 1.0 must be recorded in the release notes.

All Rust crates, the Python package, the TypeScript package, the Go module, container images, and `appVersion` use the
same engine version. The Helm chart has its own package version and declares the engine version through `appVersion`.

## Rust

The Rust facade re-exports the core, storage, worker, and telemetry crates:

```rust
use pgtask::{core, postgres::Store, worker};

fn build_worker(store: Store, registry: worker::HandlerRegistry) -> worker::Worker {
    let config = worker::WorkerConfig::new(core::QueueName::new("emails").unwrap());
    worker::Worker::new(store, registry, config).unwrap()
}
```

The `pgtask` crate is the supported entry point. It exposes `core`, `postgres`, `otel`, and `worker`. Public types and methods in those modules follow Cargo semantic versioning. The benchmark crate and native Python extension are implementation packages, not Rust APIs.

The Rust contract includes:

- Domain identifiers, task and schedule values, retry policy, and the `STORAGE_PROTOCOL_MIN_VERSION` and
  `STORAGE_PROTOCOL_MAX_VERSION` compatibility bounds.
- `Store` construction, migrations, enqueueing, inspection, scheduling, signals, and administrative operations.
- `HandlerRegistry`, `TaskContext`, `Worker`, `WorkerConfig`, `WorkerControl`, and their error types.
- OpenTelemetry propagation and metric recording functions.

## Python

The Python package pairs a typed async API with the native Rust runtime:

```python
from __future__ import annotations

import asyncio

from pgtask import Client, JSONValue, Task, TaskRegistry, Worker

registry = TaskRegistry("emails")


@registry.task("email.send")
async def send_email(task: Task, payload: dict[str, str]) -> JSONValue:
    return {"message_id": payload["message_id"], "attempt": task.attempt}


async def run(database_url: str) -> None:
    client = await Client.connect(database_url)
    handle = await client.enqueue(send_email.request({"message_id": "welcome-42"}))
    worker = Worker(database_url, registry)
    running = asyncio.create_task(worker.run())
    result = await handle.result()
    worker.shutdown()
    await running
    assert result is not None
```

The supported imports are `Client`, `EnqueueRequest`, `JSONValue`, `Task`, `TaskDefinition`, `TaskHandle`, `TaskHandler`, `TaskRegistry`, `TaskResult`, `TaskState`, `TransactionConnection`, and `Worker`. `pgtask._native` is private.

Task registration is explicit. A registry owns one queue. A definition owns the stable task name, handler version, payload type, result type, and retry policy. There is no global discovery or ARQ compatibility API.

The retry policy is immutable within a queue, task name, and handler version. PostgreSQL persists the definition and each task snapshots it before execution.

## TypeScript

The TypeScript client is a producer. It defines tasks and enqueues them:

```typescript
const render = defineTask<{ reportId: string }, { rendered: string }>("reports.render");
const task = await client.enqueue(render.request({ reportId: "report-123" }));
const result = await task.result({ timeoutMs: 30_000 });
```

The supported entry point is `@pgtask/client`. Its public surface is `Client`, `EnqueueRequest`, `TaskDefinition`,
`TaskHandle`, `defineTask`, and their exported JSON, option, request, and result types.

## Go

The Go client is a producer with generic task definitions:

```go
render, err := pgtask.DefineTask[renderRequest, renderResult]("reports.render", pgtask.DefinitionOptions{})
if err != nil {
	return err
}
task, err := render.Enqueue(ctx, client, renderRequest{ReportID: "report-123"}, pgtask.EnqueueOptions{})
```

The supported module is `github.com/Kludex/pgtask/sdks/go`. Its public surface is the typed task definition and handle,
`Client`, enqueue options and results, task results, and `QueryRowExecutor` for transactional enqueueing.

TypeScript and Go are producer SDKs. They enqueue, inspect, wait, signal, cancel, propagate OpenTelemetry context, and
join application transactions. Handler execution remains in the Rust and Python runtimes.

## SQL

Every SDK calls this surface, and so can you:

```sql
SELECT *
FROM pgtask.enqueue(
    task_name => 'email.send',
    handler_version => 1,
    payload => '{"message_id":"welcome-42"}'::jsonb,
    queue_name => 'emails',
    run_at => transaction_timestamp(),
    priority => 0,
    max_attempts => 5,
    idempotency_key => 'email:welcome-42',
    headers => '{}'::jsonb
);
```

Producer, worker, observer, and administrator functions and views are the cross-language protocol. Their arguments,
returned columns, authorization, fencing, and transaction behavior are covered by `pgtask.storage_protocol_range()`.
Workers and normal SDK clients reject a database whose range does not overlap their compiled range.

Queue configuration includes independent terminal-history and idempotency-retention windows. Idempotency reservations are not exposed to observer roles because keys may contain application identifiers.

Queue configuration also includes optional outstanding-task capacity and a starvation timeout. Capacity rejection uses
the stable SQLSTATE `PT001`. The observer overview exposes the limit and current outstanding count.

Additive SQL changes may keep the same protocol. A removed value, changed predicate, changed result shape, or incompatible authorization rule increments the protocol and requires an expand-and-contract migration.

## CLI

The CLI covers migration, health, and administrative operations:

```console
pgtask --database-url "$PGTASK_DATABASE_URL" health
pgtask --database-url "$PGTASK_DATABASE_URL" migrate
pgtask --database-url "$PGTASK_DATABASE_URL" queue put emails
```

The command names, option names, exit status, and documented stdout are public. Human-readable database errors on stderr may gain detail without a breaking release.

## OpenTelemetry

Trace context travels in task headers once propagation is configured:

```rust
fn configure() {
    pgtask::otel::configure_propagation();
}
```

Span names, metric instrument names, units, and bounded attribute keys and values are public. New instruments and attributes are additive. Renaming an instrument, changing its unit or aggregation meaning, or changing a bounded value set is breaking.

Task identifiers, payloads, results, errors, and idempotency keys are never metric attributes. See `docs/telemetry.md` for the complete current instrument table.

## Compatibility checks

The integration suite compiles against the public Rust entry point, checks the Python export list, type-checks the
TypeScript package, tests the Go module with the race detector, drives the CLI help and mutations, exercises every SQL
operation through PostgreSQL 17 and 18, and records telemetry through the public engine paths. Release CI builds every
artifact from one version and rejects an engine version mismatch.

The 1.0 contract freeze remains open until both adopter pilots pass. After 1.0, incompatible Rust, Python, SQL, CLI, or telemetry changes require a new major version.
