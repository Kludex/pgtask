# pgtask vs Celery

Celery sends task messages through a broker. This complete task uses Redis for both the broker and result backend:

```python
# tasks.py
from __future__ import annotations

import os

from celery import Celery


app = Celery(
    "reports",
    broker=os.environ["CELERY_BROKER_URL"],
    backend=os.environ["CELERY_RESULT_BACKEND"],
)


@app.task(autoretry_for=(OSError,), retry_backoff=True, max_retries=5)
def render_report(report_id: str) -> dict[str, str]:
    return {"report_id": report_id, "status": "rendered"}
```

Run the worker, then enqueue the task from another process:

```console
$ CELERY_BROKER_URL=redis://localhost/0 CELERY_RESULT_BACKEND=redis://localhost/1 \
    celery -A tasks worker --loglevel=INFO
$ CELERY_BROKER_URL=redis://localhost/0 CELERY_RESULT_BACKEND=redis://localhost/1 \
    python -c 'from tasks import render_report; print(render_report.delay("report-123").get(timeout=30))'
{'report_id': 'report-123', 'status': 'rendered'}
```

Celery is a distributed task queue with a large Python ecosystem. `pgtask` is a PostgreSQL task and durable workflow
engine. They overlap, but they optimize for different systems.

## Compare the architecture

| | [Celery](https://docs.celeryq.dev/en/stable/) | `pgtask` |
| --- | --- | --- |
| Pending work | Broker message | PostgreSQL row |
| Results | Optional result backend | PostgreSQL |
| Application transaction | Requires an outbox or reconciliation | Enqueue on the existing transaction |
| Delivery | Configurable acknowledgements and broker semantics | At least once with fenced leases |
| Workflows | Canvas chains, groups, chords, maps, and callbacks | Durable steps, sleeps, signals, child tasks, and result waits |
| Scheduling | Celery beat | Schedules claimed by workers |
| Rate limits | Per task type and worker | No global rate limiter |
| Languages | Python workers, language-independent protocol | Rust and Python workers, producer SDKs for Python, TypeScript, and Go |

Celery can use several brokers and result backends. That flexibility is valuable when PostgreSQL should not carry queue
traffic. It also means the delivery and result guarantees depend on the selected transports and configuration.

`pgtask` has one storage model. Every durable transition is a PostgreSQL transaction. This is narrower, but there are
fewer combinations to reason about.

## Choose Celery when

Choose Celery when you need its mature ecosystem, broker flexibility, task routing, rate limits, or
[Canvas](https://docs.celeryq.dev/en/stable/userguide/canvas.html). Canvas supports arbitrary combinations such as
chains, groups, and chords. `pgtask` does not provide a Canvas-compatible graph API.

Celery is also the better fit when the queue must scale independently from your application database or when your team
already operates RabbitMQ or Redis as durable infrastructure.

## Choose pgtask when

Choose `pgtask` when the task must commit with application data:

```python
async with connection.transaction():
    await connection.execute(
        "INSERT INTO reports (id, status) VALUES (%s, %s)",
        (report_id, "pending"),
    )
    await Client.enqueue_on(
        connection,
        render.request(
            {"report_id": report_id},
            idempotency_key=f"report:{report_id}",
        ),
    )
```

This removes the split write between the application database and the broker. Choose it when you also need a function
to survive sleeps, signals, child tasks, or process restarts without moving workflow state into application tables.

## Compare performance

The repository includes a
[local Celery comparison](https://github.com/Kludex/pgtask/blob/main/benchmarks/2026-08-16-queue-comparison.md). In that
run, Celery enqueued trivial tasks faster and `pgtask` drained them faster. The test used Redis without persistence and
one laptop, so it does not compare equivalent durability or production capacity.

Benchmark your broker durability, database latency, payloads, and handler duration. Do not select either system from a
single no-operation result.

## Migrate from Celery

A Celery task becomes a typed handler:

```python
from __future__ import annotations

from typing_extensions import TypedDict

from pgtask import Task, TaskRegistry


class RenderRequest(TypedDict):
    report_id: str


class RenderResult(TypedDict):
    report_id: str
    status: str


tasks = TaskRegistry(queue_name="reports")


@tasks.task("reports.render")
async def render(task: Task, request: RenderRequest) -> RenderResult:
    return {"report_id": request["report_id"], "status": "rendered"}
```

Translate calls at the application boundary:

| Celery | `pgtask` |
| --- | --- |
| Imported task or task-name string | Imported `TaskDefinition` |
| Positional and keyword arguments | Typed JSON payload |
| `task_id` | `idempotency_key` for logical deduplication |
| `eta` | `run_at` |
| `countdown` | Calculate an absolute UTC `run_at` |
| `queue` | Definition's `TaskRegistry` queue |
| `max_retries` | Set `max_attempts` to `max_retries + 1` |
| `AsyncResult` | Typed `TaskHandle` |

Do not translate Canvas mechanically. A chain may become one durable handler with named steps. A child task can replace
a separate unit of work. A group or chord needs an explicit design because `pgtask` has no equivalent graph primitive.

A custom Celery `task_id` also does not map directly. `pgtask` generates the task identifier. Use an
`idempotency_key` when the old identifier represented one logical piece of work.

Use a routing cutover:

1. Deploy the `pgtask` worker without changing producers.
2. Stop Celery beat before enabling equivalent `pgtask` schedules.
3. Route new task identifiers to `pgtask` behind one application switch.
4. Keep Celery workers running until the broker, scheduled messages, and active tasks are empty.
5. Roll back by routing new identifiers to Celery while `pgtask` drains its committed rows.

!!! warning "An idempotency key does not cross systems"

    A `pgtask` idempotency key does not deduplicate a Celery message. Use the same business identifier with every
    external API during the overlap.
