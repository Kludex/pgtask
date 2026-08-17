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
| Recurring schedules | None; `pg_cron` runs its own maintenance | Interval and cron, embedded in the worker |
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

## Process existing runs

An in-flight workflow cannot move. Its checkpoints belong to Absurd's tables and mean nothing to
`pgtask`, so a half-finished run has to finish where it started.

That makes the cutover a routing decision rather than a data migration. New workflow identifiers go
to `pgtask`. Absurd keeps its own runs until they reach a terminal state, and both engines run side
by side until it is empty.

### The longest sleep decides how long you run both

This is the part that surprises people, and it is worth working out before you start.

A durable workflow suspended for seven days keeps Absurd alive for seven days. The run is not
consuming a worker, but it is unfinished, and something has to wake it. The same applies to a signal
wait with a long timeout and to a child result wait.

So the drain window is not "until the queue looks quiet". It is **the longest sleep or timeout any
live run can still be holding**. Find that number from your own workflow definitions before you plan
the cutover, because it sets the date you can decommission Absurd.

If that window is unacceptable, you have two honest options, and neither is free:

- **Wait it out.** Run both engines for the full duration. Simplest, and usually correct.
- **Cancel and re-run.** Cancel the long sleepers in Absurd and start equivalent work in `pgtask`.
  Only safe when the completed steps have no external side effects you would repeat.

### Confirm Absurd is finished

Absurd stores each queue in its own tables, named `t_` followed by the queue name, so ask the tables
directly rather than inferring from worker activity:

```sql
SELECT queue_name FROM absurd.list_queues();

-- For each queue, count what has not reached a terminal state.
SELECT count(*) FROM absurd.t_orders
 WHERE state IN ('pending', 'running', 'sleeping');
```

A `sleeping` task is the one that matters. It is not failing and not running, so it does not show up
as activity, and it still has to be resumed before the queue is finished.

Check every queue rather than only the one you migrated, because a workflow can spawn work onto
another.

Keep the Absurd worker running until then. A suspended run that nobody resumes is not finished, it
is stuck.

### Do not enable both sides of a schedule

Absurd has no recurring schedules of its own, so anything periodic in your system is triggered from
outside it, whether that is `pg_cron`, a Kubernetes CronJob, or an external scheduler.

Point that trigger at exactly one engine. Moving it to a `pgtask` schedule means disabling the old
trigger first, in the same way you would when leaving any other scheduler.

## Roll out and roll back

Deploy the new worker beside Absurd. Route only new workflow identifiers to `pgtask`; let Absurd finish existing runs. Rollback routes new identifiers to Absurd while `pgtask` drains committed work.
