# pgtask vs ARQ

ARQ stores jobs in Redis and runs async Python functions. A worker function receives a context dictionary and the
arguments you enqueue:

```python
# worker.py
from __future__ import annotations

from arq.connections import RedisSettings


async def render_report(ctx: dict[str, object], report_id: str) -> dict[str, str]:
    return {"report_id": report_id, "status": "rendered"}


class WorkerSettings:
    functions = [render_report]
    redis_settings = RedisSettings(host="localhost")
```

Run the worker with `arq worker.WorkerSettings`. Enqueue `render_report` by name through an ARQ Redis pool.

ARQ is a compact async Python queue. `pgtask` stores the task in PostgreSQL and adds durable workflow operations.

## Compare the architecture

| | [ARQ](https://arq-docs.helpmanual.io/) | `pgtask` |
| --- | --- | --- |
| Storage | Redis | PostgreSQL |
| Application transaction | Requires an outbox or reconciliation | Enqueue on the existing transaction |
| Durable steps, sleeps, and signals | No | Yes |
| Payload | Positional and keyword arguments | Typed JSON object |
| Priority | No | Yes, with a starvation escape |
| Results | Redis with configured retention | PostgreSQL with per-queue retention |
| Worker recovery | Pessimistic execution reruns interrupted jobs | Lease expiry reruns work and fences stale writers |

Both systems provide async Python workers and deferred jobs. The storage boundary is the deciding difference.

## Choose ARQ when

Choose ARQ when your jobs are independent from database writes, Redis is already durable infrastructure, and you want a
small Python-only system. ARQ has less operational and API surface than `pgtask`.

Its pessimistic execution keeps a job available when a worker stops before completion. Your job must still be safe to
run again.

## Choose pgtask when

Choose `pgtask` when creating application data and creating its task must be one atomic write. Choose it when a workflow
must sleep, wait for a signal, spawn a child, or resume completed steps after a restart.

The cost is that PostgreSQL carries queue traffic and application traffic. Measure that shared load before you move.

## Migrate from ARQ

An ARQ function becomes a handler with a typed payload:

```python
from __future__ import annotations

from typing_extensions import TypedDict

from pgtask import Task, TaskRegistry


class ReportRequest(TypedDict):
    account_id: str
    period: str


tasks = TaskRegistry(queue_name="reports")


@tasks.task("reports.generate")
async def generate_report(task: Task, request: ReportRequest) -> None:
    await reports.generate(request["account_id"], request["period"])
```

Translate enqueue calls at the application boundary:

| ARQ | `pgtask` |
| --- | --- |
| Function-name string | Imported `TaskDefinition` |
| Positional arguments | Typed JSON payload |
| `_job_id` | `idempotency_key` |
| `_defer_until` | `run_at` |
| `_defer_by` | Calculate an absolute UTC `run_at` |
| `_queue_name` | Definition's `TaskRegistry` queue |
| `max_tries` | `max_attempts` |
| Redis result job | Typed `TaskHandle` |

Preserve the application transaction where the task depends on a new row:

```python
async with connection.transaction():
    await connection.execute(
        "INSERT INTO report_requests (account_id, period) VALUES (%s, %s)",
        (account_id, period),
    )
    await Client.enqueue_on(
        connection,
        generate_report.request(
            {"account_id": str(account_id), "period": period},
            idempotency_key=f"report:{account_id}:{period}",
        ),
    )
```

Use a routing cutover:

1. Deploy the `pgtask` worker without changing producers.
2. Remove ARQ cron entries before enabling equivalent `pgtask` schedules.
3. Route new logical identifiers to `pgtask`.
4. Keep ARQ running until queued, deferred, and in-progress jobs are finished.
5. Roll back by routing new identifiers to ARQ while `pgtask` drains its committed rows.

ARQ's `queued_jobs()` includes deferred work. Check it before stopping the old worker. Keep Redis until any retained job
results you still need have expired.

!!! warning "An idempotency key does not cross systems"

    A `pgtask` key does not deduplicate an ARQ job. Pass the same business identifier to external systems during the
    overlap.
