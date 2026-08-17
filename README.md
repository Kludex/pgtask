# pgtask

Durable tasks and workflows that live entirely in PostgreSQL.

You get a queue, a scheduler, and a durable workflow runtime out of a database you already run. There is no broker, no
coordinator, and no extension to install. If your database is up, your queue is up.

> [!WARNING]
> `pgtask` is under active development. It is not ready for production use.

## Install

| Language or tool | Install |
| --- | --- |
| Python | `pip install pgtask` |
| Rust | `cargo add pgtask` |
| TypeScript | `npm install @pgtask/client` |
| Go | `go get github.com/Kludex/pgtask/sdks/go` |
| CLI | `cargo install pgtask-cli` |
| Container image | `ghcr.io/kludex/pgtask` |
| Helm chart | `oci://ghcr.io/kludex/charts/pgtask` |

The image and the chart are tagged with the release version and signed with Sigstore. There is no `latest`, so ask for
the version you want.

Every client calls the same versioned SQL functions, so you can enqueue from one language and run the handler in
another. You need PostgreSQL 17 or newer, and nothing else.

## Your first task

Point everything at a database:

```console
export PGTASK_DATABASE_URL=postgresql://localhost:5432/pgtask
```

Write `worker.py`. A registry maps a task name to the function that runs it, and `migrate()` creates the schema:

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

Run it with `python worker.py`, then enqueue from anywhere with `enqueue.py`:

```python
from __future__ import annotations

import asyncio
import os

from pgtask import Client
from worker import render


async def main() -> None:
    client = await Client.connect(os.environ["PGTASK_DATABASE_URL"])
    task = await client.enqueue(render.request({"report_id": "report-123"}, idempotency_key="report-123:v1"))
    print(await task.result(timeout=30))


asyncio.run(main())
```

The producer writes a row and commits. PostgreSQL notifies the worker, which claims the task under a lease, renews that
lease while your handler runs, and stores the result.

`idempotency_key` makes a repeated enqueue return the same task. It does not make your side effects exactly once, so
pass a stable key to external APIs from inside the handler when they support one.

## Enqueue inside your transaction

This is the reason to keep the queue in the database. Pass your own connection and the task commits with your data or
not at all. Roll back and the task never existed, which is what an outbox table is usually for:

```python
async with connection.transaction():
    await connection.execute("INSERT INTO reports (id) VALUES (%s)", ("report-123",))
    await Client.enqueue_on(connection, render.request({"report_id": "report-123"}))
```

`connection` is your own `psycopg` connection. `enqueue_on()` neither opens another one nor commits for you.

## Durable workflows

A handler can persist the result of a step, so a retry resumes instead of starting over:

```python
@tasks.task("orders.charge")
async def charge(task: Task, request: ChargeRequest) -> dict[str, str]:
    async def create_charge() -> dict[str, str]:
        return {"charge_id": f"charge:{request['order_id']}"}

    return await task.step("create-charge", create_charge)
```

If the task runs again, `task.step()` returns the stored JSON instead of calling `create_charge()` a second time.

A handler can also wait without holding a worker:

- `task.sleep_for()` and `task.sleep_until()` suspend the task and release its lease.
- `task.wait_for_signal()` waits for an external signal.
- `task.spawn()` creates a child task, and `task.wait_for_result()` waits for it.

A task sleeping for six hours costs one row, not a worker slot. Code outside a completed step runs again on retry, so
keep step names stable and raise `handler_version` when a deployment changes what the steps mean.

See [Durable execution](https://kludex.github.io/pgtask/durable-execution/) for the full model.

## Delivery model

Delivery is at least once. A handler can finish an external side effect and lose its lease before recording the result,
so handlers must be safe to run again.

Within that, the engine guarantees:

- A committed task is not silently lost.
- A task in a rolled-back transaction is never visible.
- A task does not run before its `run_at` time.
- A stale worker cannot complete a task another worker claimed.
- An unknown task name or handler version waits instead of consuming an attempt.
- A recurring schedule creates each occurrence at most once.

Workers need session-capable connections. `LISTEN`/`NOTIFY` is the dispatch path and a slow poll recovers anything
missed, so a transaction-pooling proxy cannot stand in for the worker's session connection. No transaction is held open
while your handler runs.

## Operate it

The CLI reads `PGTASK_DATABASE_URL` and applies migrations, inspects health, and configures queues:

```console
pgtask migrate
pgtask health
pgtask queue put reports --max-outstanding-tasks 100000 --starvation-timeout-seconds 300
```

Run `pgtask migrate` before the workers that need the new schema. It takes an advisory lock, so concurrent runs apply
the schema once.

Give the schema owner, producers, workers, observers, and administrators separate database roles with
`pgtask configure-grants`. Each role reaches the data only through functions and security-barrier views, so a producer
cannot claim a task or read another queue's payloads.

See [Operations](https://kludex.github.io/pgtask/operations/) for grants, retention, draining, and incident response,
and [Upgrading](https://kludex.github.io/pgtask/upgrading/) for migrating a database that already has work in it.

## Documentation

[kludex.github.io/pgtask](https://kludex.github.io/pgtask/)

- [What pgtask is](https://kludex.github.io/pgtask/start/what-pgtask-is/)
- [Architecture](https://kludex.github.io/pgtask/architecture/)
- [Failure model](https://kludex.github.io/pgtask/failure-model/)
- [Python](https://kludex.github.io/pgtask/sdk/python/), [TypeScript](https://kludex.github.io/pgtask/sdk/typescript/),
  and [Go](https://kludex.github.io/pgtask/sdk/go/) SDKs
- [SQL protocol](https://kludex.github.io/pgtask/sql-protocol/)
- [OpenTelemetry](https://kludex.github.io/pgtask/telemetry/)

## Contributing

See [Contributing](CONTRIBUTING.md) for the development environment and the checks CI runs.

## License

This project is licensed under the [MIT License](LICENSE).
