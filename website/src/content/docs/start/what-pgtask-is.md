---
title: What pgtask is
description: What pgtask does, what it guarantees, and when you should not use it.
---

`pgtask` is a durable task and workflow engine that keeps all of its state in PostgreSQL.

You get a queue, a scheduler, and a durable workflow runtime. You do not get a server to operate. Producers, workers,
and tools all connect straight to the database.

## What you can build with it

| You want to | You use |
| --- | --- |
| Run work outside a request | A task with a handler |
| Run work later | `run_at`, or a schedule |
| Retry failures with backoff | A retry policy on the handler version |
| Run something once per key | An idempotency key |
| Survive a restart mid-workflow | Checkpoints, durable sleeps, signals, child tasks |
| Keep workloads from starving each other | Separate queues |

## The one guarantee that matters

`pgtask` delivers **at least once**.

A worker can finish an external side effect and then lose its lease before it records the result. PostgreSQL makes the
task available again, and your handler runs a second time. This is not a defect you can configure away; it is what any
system that survives a process dying at an arbitrary instant must do.

So the rule is: **your handler must be safe to run again.**

```python
@tasks.task("billing.charge")
async def charge(task: Task, request: ChargeRequest) -> ChargeResult:
    # The provider deduplicates on this key, so a second attempt does not double-charge.
    receipt = await provider.charge(
        amount=request["amount"],
        idempotency_key=f"charge:{request['invoice_id']}",
    )
    return {"receipt": receipt.id}
```

:::note[Idempotency keys work at two levels]
An idempotency key on `enqueue` deduplicates *tasks*. An idempotency key you pass to an external API deduplicates
*side effects*. They solve different problems, and you usually want both. See [Idempotency](/pgtask/concepts/idempotency/).
:::

## What it does not do

Being honest about the edges is more useful than a feature list.

- **It does not give you exactly-once side effects.** Nothing that talks to an external system can.
- **It does not scale past your database.** Queue traffic, retention, and your application queries share one PostgreSQL
  instance. That is the trade you are making in exchange for transactional enqueue.
- **It does not work through a transaction-pooling proxy alone.** Workers need one session-capable connection for
  `LISTEN`. See [Scaling and deployment](/pgtask/architecture/scaling/).
- **It has no global rate limiter.** Concurrency is per worker and per queue.

## Why PostgreSQL only

A separate broker gives you throughput and takes away consistency. The moment your queue lives outside your database,
enqueueing becomes a second write that can succeed when your transaction fails, or fail when it succeeds. You then
spend real effort on outbox tables and reconciliation jobs to get back what a single transaction gave you for free.

`pgtask` starts from the other end. Your task row is application data. It commits with your other writes or not at all.

The cost is that PostgreSQL must carry the load. For the workloads this engine targets - thousands of tasks per second,
not millions - that is a trade worth making, and one you can measure before you commit to it.
