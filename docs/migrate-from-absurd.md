# Migrate from Absurd

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

Deploy the new worker beside Absurd. Route only new workflow identifiers to `pgtask`; let Absurd finish existing runs. Rollback routes new identifiers to Absurd while `pgtask` drains committed work. Do not move an in-progress workflow between engines because their checkpoint identities are not interchangeable.
