# Migrate from Absurd

## Why migrate

Absurd and pgtask solve durable execution the same way. Steps store checkpoints, a sleep suspends the
task rather than blocking a worker, and a resume replays the handler from the top with completed
steps restored. If durable execution is all you need, you already have it.

So migrate for what sits around the workflow, not for the workflow itself:

| | Absurd | pgtask |
| --- | --- | --- |
| Durable steps, sleeps, events | Yes | Yes |
| Leases and stale-worker fencing | Yes | Yes |
| Priority | No | Yes, with a starvation escape |
| Child tasks and cascade cancellation | No | Yes |
| Role separation | Runs as your role | Producer, worker, observer, administrator |
| Queue admission limits | No | Optional hard capacity |
| Scheduling | `pg_cron` | Embedded in the worker, no extension |
| Footprint | One SQL file | Rust engine, chart, CLI, web interface |

Three of those tend to decide it.

**Priority**, because a queue that cannot say "this first" eventually needs a second queue to say it
for you.

**Child tasks**, because a workflow that fans out needs its children cancelled when it is cancelled,
and that has to be enforced by the database rather than remembered by a handler.

**Role separation**, because a producer that can also claim tasks, mark them succeeded, and read
every payload is a problem the moment more than one team shares the database. pgtask enforces this
with `SECURITY DEFINER` functions and no direct table grants.

!!! warning "Absurd is the more proven system"

    Absurd has been public since October 2025 and has production use behind it. pgtask does not, and
    its own README still says it is not production ready. If your workflows work today, that is a
    strong reason to leave them where they are.

!!! note "Do not move a workflow mid-flight"

    Checkpoint identities are not interchangeable between the two engines. Route new workflow
    identifiers to pgtask and let Absurd finish the runs it already started.

## Define durable boundaries

Absurd workflows become handlers whose durable points are named steps:

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

Map each existing checkpoint, model call, tool call, sleep, signal, spawn, and result wait to one stable operation name. Code outside a completed step can run again.

## Preserve protocol identity

- Keep the old product row as the user-facing status record.
- Use its identifier as the task idempotency key.
- Keep operation names stable for the lifetime of a handler version.
- Increment `handler_version` before changing operation order or meaning.
- Pass task and step identities to external systems as idempotency keys.

## Validate replay

Kill the worker after every durable boundary. Restart it with the same handler version. Verify completed steps do not run again, incomplete work resumes, stale lease owners cannot commit, and the product row reaches one correct terminal state.

## Roll out and roll back

Deploy the new worker beside Absurd. Route only new workflow identifiers to `pgtask`; let Absurd finish existing runs. Rollback routes new identifiers to Absurd while `pgtask` drains committed work.
