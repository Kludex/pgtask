# pgtask vs Dramatiq

Dramatiq sends actor messages through RabbitMQ or Redis. This complete task uses Redis:

```python
# actors.py
from __future__ import annotations

import os

import dramatiq
from dramatiq.brokers.redis import RedisBroker


dramatiq.set_broker(RedisBroker(url=os.environ["REDIS_URL"]))


@dramatiq.actor(max_retries=5)
def render_report(report_id: str) -> dict[str, str]:
    return {"report_id": report_id, "status": "rendered"}
```

Run the worker, then enqueue the actor from another process:

```console
$ REDIS_URL=redis://localhost/0 dramatiq actors
$ REDIS_URL=redis://localhost/0 \
    python -c 'from actors import render_report; print(render_report.send("report-123").message_id)'
```

Dramatiq is a focused Python task queue. `pgtask` adds transactional enqueue and durable functions by making
PostgreSQL the queue.

## Compare the architecture

| | [Dramatiq](https://dramatiq.io/guide.html) | `pgtask` |
| --- | --- | --- |
| Pending work | RabbitMQ or Redis message | PostgreSQL row |
| Application transaction | Requires an outbox or reconciliation | Enqueue on the existing transaction |
| Retries | Automatic exponential backoff | Retry policy with fenced leases |
| Composition | Groups and pipelines | Child tasks and durable result waits |
| Long waits inside a workflow | Schedule another message and store state yourself | Durable sleep or signal wait |
| Rate and concurrency controls | Middleware and rate limiters | Queue and worker concurrency controls |
| Runtime | Python | Rust engine with Rust and Python handlers |

Dramatiq has a smaller programming model than Celery. Its middleware system covers common task concerns without adding
a durable workflow runtime.

## Choose Dramatiq when

Choose Dramatiq when you want a fast, compact Python queue and already operate RabbitMQ or Redis. It supports groups and
pipelines. More deeply nested workflow graphs require an additional package or application code.

The repository's
[local comparison](https://github.com/Kludex/pgtask/blob/main/benchmarks/2026-08-16-queue-comparison.md) found Dramatiq
much faster for trivial messages. Redis persistence was disabled, so the result measures overhead and not equivalent
durability.

## Choose pgtask when

Choose `pgtask` when a message must commit with an application row, or when one function must survive a process restart
between named steps. You trade broker throughput and middleware flexibility for one transactional state model.

`pgtask` also keeps task results, retries, sleeps, signals, and child relationships in the same database. You do not
need to design a separate workflow state table.

## Migrate from Dramatiq

A Dramatiq actor becomes a registered async handler:

```python
from __future__ import annotations

from typing_extensions import TypedDict

from pgtask import Task, TaskRegistry


class RenderRequest(TypedDict):
    report_id: str


tasks = TaskRegistry(queue_name="reports")


@tasks.task("reports.render")
async def render(task: Task, request: RenderRequest) -> dict[str, str]:
    return {"report_id": request["report_id"], "status": "rendered"}
```

Map actor options to the request or definition:

| Dramatiq | `pgtask` |
| --- | --- |
| Actor arguments | Typed JSON payload |
| Actor name | `TaskDefinition` name |
| Queue name | `TaskRegistry` queue |
| `max_retries` | Set `max_attempts` to `max_retries + 1` |
| Delayed message | Absolute UTC `run_at` |
| Message identifier | Application-derived `idempotency_key` |
| Group or pipeline | Explicit child tasks and result waits |

Stop producing to Dramatiq, then route new logical identifiers to `pgtask`. Keep the Dramatiq worker alive until every
ready, delayed, and in-flight message is finished. Keep the `pgtask` worker alive during rollback so committed database
tasks still drain.

Do not run the same periodic trigger against both systems. Disable the old trigger before enabling the new schedule.
