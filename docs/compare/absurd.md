# pgtask vs Absurd

Absurd and `pgtask` both run durable functions on PostgreSQL. Completed steps survive retries and process restarts.

This `pgtask` workflow stores each durable boundary under a stable name:

```python
from __future__ import annotations

from typing_extensions import TypedDict

from pgtask import JSONValue, Task, TaskRegistry


class ExportRequest(TypedDict):
    export_id: str


tasks = TaskRegistry(queue_name="exports")


@tasks.task("exports.build", handler_version=1)
async def build_export(task: Task, request: ExportRequest) -> JSONValue:
    async def load_export() -> JSONValue:
        return await exports.load(request["export_id"])

    export = await task.step("load-export", load_export)
    child_id = await task.spawn("render-export", render_export.request(export))
    result = await task.wait_for_result("wait-for-render", child_id, timeout=600)
    return await task.step("record-result", lambda: exports.complete(request["export_id"], result))
```

Code outside a completed step can run again. Absurd uses the same replay model with its step checkpoints.

## The difference is around the workflow

| | [Absurd](https://earendil-works.github.io/absurd/) | `pgtask` |
| --- | --- | --- |
| Engine | One SQL schema with thin SDKs | Rust engine with SQL functions and language SDKs |
| Queue storage | A set of tables per queue | Shared task and operation tables |
| Durable steps, sleeps, and events | Yes | Yes |
| Durable child waits | Cross-queue result polling | Stored parent relationship with cascade cancellation |
| Recurring schedules | External scheduler or `pg_cron` | Interval and cron schedules claimed by workers |
| Database roles | Application role runs the SDK operations | Producer, worker, observer, and administrator roles |
| Deployment surface | SQL schema, workers, optional CLI and UI | Engine packages, workers, CLI, chart, and web interface |

Both systems avoid a broker. Both make PostgreSQL carry queue traffic and application traffic. The decision is about
how much policy you want the engine to own.

## Choose Absurd when

Choose Absurd when durable steps, sleeps, and events cover the workflow and you want the smallest engine. Its core is
a SQL schema. Its SDKs keep the application surface close to the database operations.

Absurd also has a longer public track record. `pgtask` is still under active development and is not ready for
production.

## Choose pgtask when

Choose `pgtask` when you need child-task ownership with cascade cancellation, queue admission limits, priority,
embedded schedules, or strict database role separation. These features add deployment surface, but they keep more
operational invariants inside the engine.

Priority includes a starvation escape. Queue capacity can reject new work before an overloaded queue consumes the
database. Child cancellation is enforced by the database rather than remembered by a handler.

## Migrate from Absurd

Do not move a workflow while it is running. Absurd checkpoints do not have the same identity or result shape as
`pgtask` operations.

Use a routing cutover:

1. Map every Absurd step, model call, tool call, sleep, event, and result wait to one stable `pgtask` operation name.
2. Keep the existing product identifier as the `pgtask` idempotency key.
3. Route new workflow identifiers to `pgtask`.
4. Keep Absurd workers running until every pending, running, and sleeping task reaches a terminal state.
5. Roll back by routing new identifiers to Absurd while `pgtask` drains its committed work.

The longest sleep or event timeout determines the drain window. A quiet worker does not prove the queue is empty.

!!! warning "Do not enable both schedulers"

    Absurd uses an external scheduler or `pg_cron` for recurring application tasks. Disable that trigger before you
    enable the equivalent `pgtask` schedule. An overlap creates duplicate occurrences.

Kill the new worker after every durable boundary during validation. Restart it with the same handler version. Completed
steps must stay completed, incomplete work must resume, and a stale lease owner must not commit.
