# Python SDK

## Run a worker

```python
from __future__ import annotations

import asyncio
import os

from typing_extensions import TypedDict

from pgtask import Task, TaskRegistry, Worker


class RenderRequest(TypedDict):
    report_id: str


class RenderResult(TypedDict):
    report_id: str
    attempt: int


tasks = TaskRegistry(queue_name="reports")


@tasks.task("reports.render")
async def render(task: Task, request: RenderRequest) -> RenderResult:
    return {"report_id": request["report_id"], "attempt": task.attempt}


asyncio.run(
    Worker(
        os.environ["PGTASK_DATABASE_URL"],
        tasks,
        health_address=os.getenv("PGTASK_HEALTH_ADDRESS"),
        listener_url=os.getenv("PGTASK_LISTENER_DATABASE_URL"),
    ).run()
)
```

The Rust runtime owns claiming, PostgreSQL notifications, leases, retries, scheduling, shutdown, and OpenTelemetry spans. The registered Python coroutine only runs the task body.

`TaskRegistry` is explicit worker configuration. It owns one logical queue and does not discover modules or use global state. The decorator returns a `TaskDefinition`, so producers can import `render` without repeating its durable name, queue, handler version, payload type, or result type.

Set `handler_version` when a long-lived task changes its durable protocol. An exception is retryable by default. Pass `retry_delay=None` for a terminal failure on the first exception.

## Enqueue and wait

```python
from __future__ import annotations

import asyncio
import os

from pgtask import Client
from worker import render


async def main() -> None:
    client = await Client.connect(
        os.environ["PGTASK_DATABASE_URL"],
        listener_url=os.getenv("PGTASK_LISTENER_DATABASE_URL"),
    )
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


asyncio.run(main())
```

`TaskHandle.result` subscribes to the task's deterministic result channel before it reads task state. Completion cannot be lost between the subscription and the read. One Rust listener connection multiplexes all result waits. A timeout returns `None` without changing the task. The handle also exposes `inspect`, `signal`, and `cancel`.

The query and listener endpoints default to the same URL. Set `listener_url` when queries use a transaction-pooling proxy. The listener endpoint must support PostgreSQL sessions. `max_query_connections` defaults to 10. `max_listener_connections` defaults to 1 because the Rust engine multiplexes subscriptions.

The client injects the active Python OpenTelemetry context into task headers. The worker restores that context around the Python handler. Database cancellation cancels the Python coroutine and runs its `finally` blocks.

## Run a durable workflow

```python
from __future__ import annotations

import asyncio
import os

from typing_extensions import TypedDict

from pgtask import Client, JSONValue, Task, TaskRegistry, Worker


class RenderRequest(TypedDict):
    report_id: str


tasks = TaskRegistry(queue_name="reports")
waiting = asyncio.Event()


@tasks.task("reports.render")
async def render(task: Task, request: RenderRequest) -> str:
    return f"rendered:{request['report_id']}"


@tasks.task("reports.approve")
async def approve(task: Task, request: RenderRequest) -> JSONValue:
    async def load_report() -> RenderRequest:
        return request

    report = await task.step("load-report", load_report)
    await task.sleep_for("settle", 0.1)
    waiting.set()
    approval = await task.wait_for_signal("wait-for-approval", "approval")
    child_id = await task.spawn("render-report", render.request(report))
    rendered = await task.wait_for_result("wait-for-render", child_id)
    return {"approval": approval, "rendered": rendered}


async def main() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    await client.migrate()
    worker = Worker(database_url, tasks, concurrency=10)
    running = asyncio.create_task(worker.run())
    try:
        workflow = await client.enqueue(approve.request({"report_id": "report-123"}))
        await waiting.wait()
        await workflow.signal("approval", {"approved": True})
        result = await workflow.result(timeout=30)
        if result is None or result.state != "succeeded":
            raise RuntimeError(f"workflow failed: {result!r}")
    finally:
        worker.shutdown()
        await running


asyncio.run(main())
```

`step` stores the operation result as JSON and reuses it after a retry or restart. `sleep_for`, `sleep_until`, `wait_for_signal`, and `wait_for_result` suspend the task and release its worker slot. `spawn` creates the child and records its identifier atomically.

Keep names and occurrences stable. Code outside a completed `step` can run again. Use `handler_version` when a deployment changes the order or meaning of durable operations.

## Enqueue in an application transaction

```python
from __future__ import annotations

import asyncio
import os

from psycopg import AsyncConnection

from pgtask import Client
from worker import render


async def main() -> None:
    connection = await AsyncConnection.connect(os.environ["PGTASK_DATABASE_URL"])
    try:
        async with connection.transaction():
            await connection.execute("INSERT INTO reports (id) VALUES (%s)", ("report-123",))
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

`enqueue_on` uses the existing Psycopg connection. The task commit and the application write succeed or roll back together. It does not open another connection or commit the caller's transaction.

## Migrate from ARQ

```python
from __future__ import annotations

import asyncio
import os

from datetime import datetime, timedelta, timezone

from pgtask import Client
from worker import render


async def main() -> None:
    client = await Client.connect(os.environ["PGTASK_DATABASE_URL"])
    task = await client.enqueue(
        render.request(
            {"report_id": "report-123"},
            idempotency_key="report-123:v1",
            run_at=datetime.now(timezone.utc) + timedelta(seconds=5),
        )
    )
    await task.result(timeout=30)


asyncio.run(main())
```

Translate ARQ calls at the application boundary. Use payload objects, normal keyword arguments, and absolute `run_at` times. pgtask deliberately has no ARQ-shaped client, magic `_defer_by` options, positional payload encoding, or Redis compatibility layer.
