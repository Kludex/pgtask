# pgtask

`pgtask` is a durable task and workflow engine for PostgreSQL.

You use PostgreSQL as the queue, scheduler, and durable state store. You do not need Redis, a broker, a coordinator,
or a PostgreSQL extension.

> [!WARNING]
> `pgtask` is under active development. It is not ready for production use.

## Features

- Enqueue tasks inside the same transaction as your application data.
- Run tasks with leased, at-least-once delivery.
- Delay, retry, and schedule work.
- Persist workflow steps, sleeps, signals, and child tasks.
- Separate workloads with logical queues.
- Propagate OpenTelemetry context from producers to workers.
- Use the same database protocol from Rust, Python, TypeScript, Go, SQL, and the CLI.

PostgreSQL is the only service required for correctness.

## Quickstart

Clone the repository and install the Python package:

```console
git clone https://github.com/Kludex/pgtask.git
cd pgtask
uv sync --project sdks/python --group dev
```

Start PostgreSQL:

```console
docker compose up -d postgres
export PGTASK_DATABASE_URL=postgresql://pgtask:pgtask@localhost:54329/pgtask
```

Create `worker.py`:

```python
from __future__ import annotations

import asyncio
import os

from typing_extensions import TypedDict

from pgtask import Client, Task, TaskRegistry, Worker


class RenderRequest(TypedDict):
    report_id: str


class RenderResult(TypedDict):
    report_id: str
    attempt: int


tasks = TaskRegistry(queue_name="reports")


@tasks.task("reports.render")
async def render(task: Task, request: RenderRequest) -> RenderResult:
    return {"report_id": request["report_id"], "attempt": task.attempt}


async def main() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    await client.migrate()
    await Worker(database_url, tasks).run()


if __name__ == "__main__":
    asyncio.run(main())
```

Run the worker:

```console
uv run --project sdks/python python worker.py
```

Create `enqueue.py`:

```python
from __future__ import annotations

import asyncio
import os

from pgtask import Client
from worker import render


async def main() -> None:
    client = await Client.connect(os.environ["PGTASK_DATABASE_URL"])
    task = await client.enqueue(
        render.request(
            {"report_id": "report-123"},
            idempotency_key="report-123:v1",
        )
    )
    result = await task.result(timeout=30)
    if result is None:
        raise TimeoutError("report did not finish")
    if result.state != "succeeded":
        raise RuntimeError(f"report failed: {result.error!r}")
    if result.result != {"report_id": "report-123", "attempt": 1}:
        raise RuntimeError(f"unexpected result: {result.result!r}")


asyncio.run(main())
```

Run the producer in another terminal:

```console
uv run --project sdks/python python enqueue.py
```

The producer stores the task in PostgreSQL. PostgreSQL sends a notification after the transaction commits. The worker
claims the task, renews its lease while it runs, and stores the result.

`idempotency_key` makes repeated enqueue requests return the same task. It does not make external side effects exactly
once. Its queue-level retention is independent from task history, so deleting old tasks does not release an unexpired
key. Pass a stable idempotency key to external APIs from your handler when they support one.

## Delivery model

`pgtask` uses at-least-once delivery.

A worker can complete an external side effect and crash before it stores the result. PostgreSQL makes the task available
again after the lease expires. Your handler must be safe to run again.

The engine provides these guarantees:

- A committed task is not silently lost.
- A task in a rolled-back transaction is never visible.
- A task does not run before its `run_at` time.
- A stale worker cannot complete a task claimed by another worker.
- An unknown task name or handler version stays pending without consuming an attempt.
- A recurring schedule creates each occurrence at most once.

Task payloads, headers, results, errors, and durable step values use JSON.

## Transactional enqueueing

Use `Client.enqueue_on()` when a task depends on an application write:

```python
from __future__ import annotations

import asyncio
import os

from psycopg import AsyncConnection

from pgtask import Client
from worker import render


async def main() -> None:
    connection = await AsyncConnection.connect(
        os.environ["PGTASK_DATABASE_URL"],
        autocommit=True,
    )
    try:
        await connection.execute("CREATE TABLE IF NOT EXISTS reports (id text PRIMARY KEY)")
        async with connection.transaction():
            await connection.execute(
                "INSERT INTO reports (id) VALUES (%s) ON CONFLICT DO NOTHING",
                ("report-123",),
            )
            await Client.enqueue_on(
                connection,  # type: ignore[arg-type]
                render.request(
                    {"report_id": "report-123"},
                    idempotency_key="report-123:render",
                ),
            )
    finally:
        await connection.close()


asyncio.run(main())
```

The report and the task commit together. If either write fails, PostgreSQL rolls back both writes. `enqueue_on()` does
not open another connection or commit the transaction for you.

## Durable workflows

A task can persist the result of a step:

```python
from __future__ import annotations

import asyncio
import os

from typing_extensions import TypedDict

from pgtask import Client, Task, TaskRegistry, Worker


class ChargeRequest(TypedDict):
    order_id: str


tasks = TaskRegistry(queue_name="orders")


@tasks.task("orders.charge")
async def charge(task: Task, request: ChargeRequest) -> dict[str, str]:
    async def create_charge() -> dict[str, str]:
        return {"charge_id": f"charge:{request['order_id']}"}

    return await task.step("create-charge", create_charge)


async def main() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    await client.migrate()
    await Worker(database_url, tasks).run()


if __name__ == "__main__":
    asyncio.run(main())
```

If the task retries, `task.step()` returns the stored JSON result instead of running `create_charge()` again.

You can also use:

- `task.sleep_for()` and `task.sleep_until()` to suspend without occupying a worker slot.
- `task.wait_for_signal()` to wait for an immutable external signal.
- `task.spawn()` to create a child task.
- `task.wait_for_result()` to wait for a child task.

Keep step names and occurrences stable. Code outside a completed step can run again. Increment `handler_version` when a
deployment changes the order or meaning of durable operations.

See [Durable execution](docs/durable-execution.md) for the complete behavior and failure model.

## PostgreSQL connections

Workers require session-capable PostgreSQL connections.

`LISTEN` and `NOTIFY` are the normal dispatch path. A low-frequency poll recovers notifications missed during a
disconnect. A transaction-pooling proxy cannot replace the worker's session connection.

The worker does not hold a database transaction while your handler runs.

## Worker configuration

```python
worker = Worker(
    database_url,
    tasks,
    concurrency=10,
    poll_interval=30.0,
    lease_duration=30.0,
    health_address="0.0.0.0:8081",
)
```

| Option | Default | Purpose |
| --- | ---: | --- |
| `concurrency` | `10` | Maximum number of handlers running at once. |
| `poll_interval` | `30.0` | Reconciliation interval for missed notifications. |
| `lease_duration` | `30.0` | Time before another worker can reclaim abandoned work. |
| `health_address` | `None` | Address for the worker health server. |

Use separate queues for workloads that need separate concurrency, resources, or scaling.

## Operations

The CLI applies migrations and manages queues:

```console
cargo run -p pgtask-cli --bin pgtask -- --database-url "$PGTASK_DATABASE_URL" migrate
cargo run -p pgtask-cli --bin pgtask -- --database-url "$PGTASK_DATABASE_URL" health
cargo run -p pgtask-cli --bin pgtask -- --database-url "$PGTASK_DATABASE_URL" queue put reports
```

Production deployments should use separate database roles for the schema owner, producers, workers, observers, and
administrators. See [Operations](docs/operations.md) for grants, backup, restore, retention, draining, and incident
response.

## Development

Run the Rust checks:

```console
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Run the Python checks:

```console
cd sdks/python
uv sync --group dev
uv run ruff format --check python tests
uv run ruff check python tests
uv run mypy python tests
uv run pytest --cov=pgtask --cov=tests
```

Run the TypeScript checks:

```console
cd sdks/typescript
npm ci
npm run typecheck
npm run lint
npm test
```

Run the Go checks:

```console
cd sdks/go
golangci-lint run ./...
go test -race ./...
```

Run the Helm lifecycle suite:

```console
./scripts/test-kind-lifecycle.sh
```

See [Contributing](CONTRIBUTING.md) for the complete development workflow.

## Documentation

- [Architecture](docs/architecture.md)
- [Python SDK](docs/sdk/python.md)
- [TypeScript SDK](docs/sdk/typescript.md)
- [Go SDK](docs/sdk/go.md)
- [Durable execution](docs/durable-execution.md)
- [Failure model](docs/failure-model.md)
- [SQL protocol](docs/sql-protocol.md)
- [OpenTelemetry](docs/telemetry.md)
- [Operations](docs/operations.md)
- [Schema compatibility](docs/schema-compatibility.md)
- [Security review](docs/security-review.md)
- [UI](docs/ui.md)
- [Public contracts](docs/public-contracts.md)
- [Roadmap and release gates](PLAN.md)

## License

This project is licensed under the [MIT License](LICENSE).
