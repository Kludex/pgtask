# pgtask

Durable tasks and workflows that live entirely in PostgreSQL.

There is no broker, no coordinator, and no extension to install. If your database is up, your queue is up.

> [!WARNING]
> `pgtask` is under active development. It is not ready for production use.

## Installation

```console
pip install pgtask
```

## Define a task and run a worker

A registry maps a task name to the function that runs it. A worker claims work for that queue, one lease
at a time, so a worker that dies hands its tasks back instead of taking them with it.

```python
from __future__ import annotations

import asyncio
import os

from pgtask import Client, Task, TaskRegistry, Worker

tasks = TaskRegistry(queue_name="reports")


@tasks.task("reports.render")
async def render(task: Task, payload: dict) -> dict:
    return {"report_id": payload["report_id"], "attempt": task.attempt}


async def main() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    client = await Client.connect(database_url)
    await client.migrate()
    await Worker(database_url, tasks).run()


asyncio.run(main())
```

Delivery is at least once, so write handlers that are safe to run again.

## Enqueue a task

```python
from __future__ import annotations

import asyncio
import os

from pgtask import Client, EnqueueRequest


async def main() -> None:
    client = await Client.connect(os.environ["PGTASK_DATABASE_URL"])
    handle = await client.enqueue(
        EnqueueRequest("reports.render", {"report_id": "report-123"}, queue_name="reports")
    )
    print(await handle.result(timeout=30.0))


asyncio.run(main())
```

## Enqueue inside your transaction

This is the reason to keep the queue in the database. Pass your own connection and the task commits with
your data or not at all. Roll back and the task never existed, which is what an outbox table is usually
for.

```python
async with connection.transaction():
    await connection.execute("INSERT INTO reports (id) VALUES (%s)", ("report-123",))
    await Client.enqueue_on(connection, request)
```

`connection` is your `psycopg` connection, not one of ours.

## Documentation

[kludex.github.io/pgtask](https://kludex.github.io/pgtask/) covers workers, durable execution, signals,
cancellation, scheduling, and OpenTelemetry propagation.
