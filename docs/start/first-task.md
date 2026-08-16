# Your first task

You need two processes: one that runs handlers, and one that enqueues work. Start with the worker.

## Write a handler

A handler is an async function bound to a task name in a registry. This one echoes its payload back:

```python
# worker.py
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

Run it:

```console
uv run --project sdks/python python worker.py
```

Three things in that file matter more than they look.

`TaskRegistry(queue_name="reports")` binds this worker to one logical queue. A worker serves exactly one queue, which is
how you keep slow work from starving fast work.

`"reports.render"` is the task name stored in the database. The worker advertises the names and handler versions it can
run, and the database only hands back tasks it declared. A deployment that does not know a task name leaves it
`pending` instead of consuming an attempt and failing it.

`task.attempt` starts at `1` and increases on every retry. It is the honest signal that your handler may be running for
the second time.

## Enqueue work

The producer builds a request from the handler and waits for the result:

```python
# enqueue.py
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
    print(result.result)


asyncio.run(main())
```

Run it in another terminal:

```console
uv run --project sdks/python python enqueue.py
```

You get `{'report_id': 'report-123', 'attempt': 1}`.

`render.request(...)` builds the request from the handler you already declared, so the payload is checked against the
handler's signature instead of being a loose dictionary.

`task.result(timeout=30)` waits on a notification rather than polling. It returns `None` on timeout, so a timeout is a
value you handle and not an exception you catch.

## Enqueue inside your transaction

This is the reason to use a database-backed queue at all. Pass your existing connection and the task commits with your
data:

```python
async with connection.transaction():
    await connection.execute("INSERT INTO reports (id, status) VALUES ($1, 'pending')", report_id)
    await client.enqueue_in(connection, render.request({"report_id": report_id}))
```

If the transaction rolls back, the row and the task both disappear. There is no window where a task exists for a report
that does not, and no outbox to reconcile.

!!! warning "The notification is not the task"

    PostgreSQL sends the wake-up after your transaction commits. The task row is the source of truth; the notification
    only saves the worker from polling. If a notification is ever lost, a reconciliation poll finds the task anyway.

## What happens next

The producer commits a `pending` row. PostgreSQL notifies the queue's channel. A worker wakes, claims the task under a
lease, runs your handler, and records the result.

That path is worth understanding before you put real work through it. Read
[How a task runs](../architecture/task-lifecycle.md).
