# Idempotency

There are two idempotency problems and they need two different tools.

## Deduplicating tasks

An idempotency key on enqueue makes repeated requests return the same task instead of creating a second one.

```python
task = await client.enqueue(
    render.request({"report_id": "report-123"}, idempotency_key="report-123:v1"),
)
```

Enqueue twice, get one task and the same identifier back. This protects against a retried HTTP request or an at-least-
once upstream event creating duplicate work.

The reservation is stored separately from the task. It stays active while the task is unfinished, then expires after the
queue's `idempotency_retention_seconds`.

!!! note "Deleting task history does not release the key"

    Reservations have their own retention deliberately. If they were tied to task rows, shortening your observability
    retention would silently shorten your deduplication window - a change nobody expects to make.

## Deduplicating side effects

An idempotency key on enqueue does nothing for what your handler does to the outside world. A handler can charge a card
and lose its lease before recording the result, and it will run again.

Pass a stable key to the external system:

```python
@tasks.task("billing.charge")
async def charge(task: Task, request: ChargeRequest) -> ChargeResult:
    receipt = await provider.charge(
        amount=request["amount"],
        idempotency_key=f"charge:{request['invoice_id']}",
    )
    return {"receipt": receipt.id}
```

Derive the key from your data, not from `task.attempt` or a fresh UUID. It must be identical across attempts, which is
exactly what a retry produces.

## When the external system has no key

Make the effect naturally repeatable, or record that you did it inside a durable step:

```python
await task.step("send-welcome", lambda: mailer.send(request["email"]))
```

The step's checkpoint means a replay returns the stored result rather than sending again. That closes the window between
attempts, though not the one between the side effect and the checkpoint commit - which is the irreducible part of
at-least-once.
