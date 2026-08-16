---
title: Retries
description: Retry policies, jitter, and why the policy belongs to the handler version.
---

A retry policy is registered per `(queue, task_name, handler_version)` and is immutable for that identity.

```python
@tasks.task("reports.render", retry=Retry.exponential(base_delay=1.0, factor=2, max_delay=300.0))
async def render(task: Task, request: RenderRequest) -> RenderResult:
    ...
```

## The shapes

| Policy | Delay before the next attempt |
| --- | --- |
| `never` | No retry; the first failure is terminal |
| `fixed` | The same delay every time |
| `exponential` | `base_delay × factor ^ (attempt - 1)`, capped at `max_delay` |

Every delay gets full jitter: the actual wait is uniformly random between zero and the computed delay.

Jitter is not a refinement. Without it, a hundred tasks that failed together during an outage retry together, hit the
recovering dependency at the same instant, and fail together again. Full jitter spreads them across the whole window.

## max_attempts is separate

The policy decides *when* to retry. `max_attempts` on the task decides *how many times*. When attempts are exhausted the
task becomes `failed` and stays there for inspection.

## Why the policy is frozen

A task snapshots its policy when it is enqueued, if the definition is registered, or when it is first claimed.

A deploy therefore cannot change how tasks already in flight retry. This matters during an incident: the retry behaviour
you observe is the behaviour that was in force when the work was created, not whatever shipped ten minutes ago.

To change retries, publish a new handler version.

:::note[Registering a conflicting policy is an error]
Two workers registering the same `(queue, task_name, handler_version)` with different policies is rejected by the
database. That prevents a half-rolled-out deploy from making retry behaviour depend on which worker claimed the task.
:::
