# Queues

A queue is a name. Tasks carry it, workers serve exactly one of it, and everything shares one `tasks` table underneath.

You use queues to stop workloads interfering with each other.

```python
reports = TaskRegistry(queue_name="reports")   # slow, CPU-heavy
emails = TaskRegistry(queue_name="emails")     # fast, latency-sensitive
```

Run those as separate deployments and a backlog of reports cannot delay an email. In a single queue it would, because
concurrency slots are shared.

## What a queue controls

| Setting | Effect |
| --- | --- |
| `terminal_retention_seconds` | How long finished tasks stay before deletion |
| `idempotency_retention_seconds` | How long an idempotency key stays reserved after its task finishes |
| `max_outstanding_tasks` | Hard admission limit, or unlimited |
| `starvation_timeout_seconds` | When old work starts bypassing priority |
| `paused_at` | Whether claims are allowed |

```python
await client.put_queue(
    QueueConfig(
        name="reports",
        terminal_retention_seconds=7 * 24 * 3600,
        max_outstanding_tasks=10_000,
    )
)
```

## Pausing

A paused queue accepts enqueues and stops claims. Work accumulates and nothing runs.

That makes pause the right tool for an incident - you stop the damage without losing the work or asking producers to
change. Resume and the backlog drains.

## Choosing how many

Start with one queue per distinct operational profile, not one per task type.

Ask whether the work needs different concurrency, different resources, or different scaling behaviour. If the answer is
no, another queue only adds deployments to run.
