# Migrate from ARQ

## Define the task

```python
from __future__ import annotations

from typing_extensions import TypedDict

from pgtask import Task, TaskRegistry


class ReportRequest(TypedDict):
    user_id: str
    period: str


tasks = TaskRegistry(queue_name="ops")


@tasks.task("reports.generate")
async def generate_report(task: Task, request: ReportRequest) -> None:
    await generate_report(request["user_id"], request["period"])
```

Import the returned `TaskDefinition` from producers. Do not reproduce ARQ's string lookup, positional payloads, global registry, or underscore-prefixed enqueue options.

## Translate enqueue calls

```python
handle = await client.enqueue(
    generate_report.request(
        {"user_id": str(user_id), "period": period},
        idempotency_key=f"report:{user_id}:{period}",
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

```python
async with connection.transaction():
    await connection.execute("INSERT INTO report_requests (user_id) VALUES (%s)", (user_id,))
    await Client.enqueue_on(
        connection,
        generate_report.request(
            {"user_id": str(user_id), "period": period},
            idempotency_key=f"report:{user_id}:{period}",
        ),
    )
```

Use transactional enqueue when the task depends on a row written by the same request. This removes the outbox race without adding another service.

## Roll out and roll back

1. Measure the old task's volume, duration, failures, retries, and queue age.
2. Deploy the `pgtask` worker without changing producers.
3. Route new requests to `pgtask` behind one explicit application switch.
4. Keep the ARQ worker running with cron production disabled until its queue and in-progress set remain empty.
5. Compare latency, attempts, external duplicates, resources, and operator visibility.
6. Roll back by routing new requests to ARQ. Keep the `pgtask` worker draining already committed rows.

Never enqueue the same logical request to both systems unless its external idempotency key is shared and verified.
