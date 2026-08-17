# Retries

A retry policy belongs to a `(queue, task_name, handler_version)` and is immutable for that identity.

In Python you set it when you register the handler, with `retry_delay` in seconds:

```python
@tasks.task("reports.render", retry_delay=1.0)
async def render(task: Task, request: RenderRequest) -> RenderResult:
    ...
```

`retry_delay=None` means never retry. The default is `1.0`, so a handler you register without thinking about it retries
after one second.

## The policy model

The engine stores one of three policies. Which of them you can reach depends on the client you use:

| Policy | Delay before the next attempt | Reachable from Python |
| --- | --- | --- |
| `never` | No retry; the first failure is terminal | `retry_delay=None` |
| `fixed` | The same delay every time | `retry_delay=<seconds>` |
| `exponential` | `base_delay × factor ^ (attempt - 1)`, capped at `max_delay` | Not yet |

Exponential backoff exists in the schema and in the Rust worker, but the Python registration API takes a single delay
and maps it to `fixed`. If you need exponential backoff today, register the handler from Rust or write the policy
through the SQL protocol.

## Jitter

Every delay gets full jitter: the actual wait is uniformly random between zero and the computed delay.

This is not a refinement. Without it, a hundred tasks that failed together during an outage retry together, hit the
recovering dependency at the same instant, and fail together again. You have built a system that synchronises its own
load spikes. Full jitter spreads them across the whole window.

## max_attempts is separate

The policy decides *when* to retry. `max_attempts` on the request decides *how many times*:

```python
render.request({"report_id": "report-123"}, max_attempts=3)
```

When attempts are exhausted the task becomes `failed` and stays there for inspection. The default is 5.

## Why the policy is frozen

A task snapshots its policy when it is enqueued, if the definition is registered, or when it is first claimed.

A deploy therefore cannot change how tasks already in flight retry. This matters during an incident, because the retry
behaviour you observe in the timeline afterwards is the behaviour that was in force when the work was created, not
whatever shipped ten minutes into the outage.

To change retries, publish a new handler version.

!!! note "Registering a conflicting policy is an error"

    Two workers registering the same `(queue, task_name, handler_version)` with different policies is rejected by the
    database. That prevents a half-rolled-out deploy from making retry behaviour depend on which worker happened to
    claim the task.
