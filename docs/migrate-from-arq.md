# Migrate from ARQ

## Why migrate

ARQ keeps your queue in Redis. That is a second datastore, and it is the one your task lives in while
your data lives somewhere else.

Everything below follows from moving that state into the database you already have:

| | ARQ | pgtask |
| --- | --- | --- |
| Storage | Redis | PostgreSQL |
| Enqueue in your transaction | No | Yes |
| Durable steps, sleeps, signals | No | Yes |
| Typed payloads | Untyped arguments | Checked against the handler |
| Priority | No | Yes, with a starvation escape |
| Results | Redis, one hour by default | PostgreSQL, per-queue retention |
| Stale worker writes | Job requeued on shutdown | Rejected by lease token |

The one that changes how you write code is transactional enqueue. In ARQ, creating a row and
enqueueing the job that processes it are two writes to two systems, so either can fail alone. You
either accept the inconsistency or build an outbox table, which is a queue in your database feeding a
queue in Redis.

The one that changes what you can build is durable execution. A workflow that renders a report, waits
six hours, then emails it is not an ARQ job. In pgtask it is one handler, because the wait releases
the worker and the task resumes from its checkpoints.

ARQ is not careless with your work. Its pessimistic execution keeps a job in the queue until it
succeeds or fails, so a worker shutdown requeues rather than drops it. The difference is what backs
that queue and whether the enqueue can join your transaction.

!!! note "Stay on ARQ if"

    Your jobs are fire-and-forget, you already run Redis, and nothing you enqueue has to agree with a
    database write. ARQ is smaller, older, and has none of the operational surface pgtask adds. A
    migration is only worth it for transactional enqueue, durable workflows, or dropping Redis.

## Define the task

An ARQ task function becomes a registered handler with a typed payload:

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

Import the returned `TaskDefinition` from producers. Do not reproduce ARQ's string lookup, positional payloads, global registry, or underscore-prefixed enqueue options.

## Translate enqueue calls

`enqueue_job` becomes `enqueue`, which returns a handle you can await a result on:

```python
handle = await client.enqueue(
    generate_report.request(
        {"account_id": str(account_id), "period": period},
        idempotency_key=f"report:{account_id}:{period}",
    )
)
```

| ARQ | `pgtask` |
| --- | --- |
| function-name string | imported `TaskDefinition` |
| positional arguments | typed JSON payload |
| `_job_id` | `idempotency_key` |
| `_defer_until` | `run_at` |
| `_defer_by` | calculate an absolute UTC `run_at` |
| `_queue_name` | definition's `TaskRegistry` queue |
| `max_tries` | `max_attempts` |
| Redis result job | typed `TaskHandle` |
| ARQ cron | interval or six-field UTC schedule |

## Preserve an application transaction

This is the part ARQ cannot do. Pass your connection and the task commits with your data:

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

Use transactional enqueue when the task depends on a row written by the same request. This removes the outbox race without adding another service.

## Process the existing queue

Do not move jobs between the two systems. Drain the old queue instead.

A job in Redis has no equivalent row in PostgreSQL, and copying one across gives you a job that both
workers can run. Every safe cutover has the same shape: stop producing into ARQ, let ARQ finish what
it already holds, and produce into `pgtask` from that moment on.

Both workers run at once during the drain, which is fine as long as each is only claiming its own
backlog.

### Stop cron before you enable schedules

This is the step that causes duplicate work, so do it first and confirm it.

ARQ cron jobs and `pgtask` schedules do not know about each other. If both are live, every occurrence
runs twice. Remove the `cron_jobs` entry from the ARQ worker settings and deploy that worker **before**
you create the equivalent schedule in `pgtask`.

A short gap where neither runs is safe. An overlap where both run is not, unless the task is
genuinely idempotent.

### Wait for deferred jobs

A job enqueued with `_defer_by` or `_defer_until` sits in Redis until its time arrives. It is not
visible as work in progress, and it will run even after you think the queue is empty.

Find the furthest one and keep the ARQ worker alive until it has passed:

```python
from arq.connections import create_pool

redis = await create_pool(settings)
for job in await redis.queued_jobs():
    print(job.function, job.enqueue_time, job.score)
```

`queued_jobs()` includes deferred jobs, so an empty result means nothing is pending or waiting. If
your application defers work by days, the ARQ worker has to outlive the longest deferral.

### Confirm the queue is finished

ARQ is done when three things hold at once:

- `queued_jobs()` returns nothing.
- No job reports `JobStatus.in_progress`.
- The latest deferred time has passed.

Then stop the ARQ worker. Keep Redis until you no longer need the job results it holds; results
expire on their own default of one hour unless you configured otherwise.

### Protect the overlap with idempotency keys

While both systems run, a retry in your application can produce the same logical work twice, once on
each side.

Derive the idempotency key from your data rather than from a request, and pass the same value to any
external system that accepts one:

```python
await client.enqueue(
    generate_report.request(
        {"account_id": str(account_id), "period": period},
        idempotency_key=f"report:{account_id}:{period}",
    )
)
```

The key deduplicates the task inside `pgtask`. It does not deduplicate against ARQ, which is why the
external key matters more during a cutover than at any other time.

## Roll out and roll back

1. Measure the old task's volume, duration, failures, retries, and queue age.
2. Deploy the `pgtask` worker without changing producers.
3. Route new requests to `pgtask` behind one explicit application switch.
4. Keep the ARQ worker running with cron production disabled until its queue and in-progress set remain empty.
5. Compare latency, attempts, external duplicates, resources, and operator visibility.
6. Roll back by routing new requests to ARQ. Keep the `pgtask` worker draining already committed rows.

Never enqueue the same logical request to both systems unless its external idempotency key is shared and verified.

Configure `idempotency_retention_seconds` for at least the longest period in which an ARQ producer may repeat a job identifier. Task history may use a shorter retention window without releasing that identifier.
