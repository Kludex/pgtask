# Compare pgtask

Choose the storage model before you compare task decorators.

`pgtask` keeps task state in PostgreSQL. You can enqueue work in the same transaction as your application data:

```python
async with connection.transaction():
    await connection.execute(
        "INSERT INTO reports (id, status) VALUES (%s, %s)",
        (report_id, "pending"),
    )
    await Client.enqueue_on(
        connection,
        render.request(
            {"report_id": report_id},
            idempotency_key=f"report:{report_id}",
        ),
    )
```

If the transaction rolls back, neither write exists. This is the main reason to choose `pgtask` over a broker-backed
queue.

## Choose by constraint

| Choose | When you need |
| --- | --- |
| [`pgtask`](../start/what-pgtask-is.md) | Transactional enqueue, PostgreSQL as the only state store, or durable functions |
| [Absurd](absurd.md) | Durable functions with the smallest PostgreSQL-native footprint |
| [Celery](celery.md) | A mature Python ecosystem, several broker choices, or Canvas workflows |
| [Dramatiq](dramatiq.md) | A small, fast Python task queue backed by RabbitMQ or Redis |
| [ARQ](arq.md) | A compact async Python queue when Redis is already part of your stack |

There is no universal winner. Celery has a much larger ecosystem. Dramatiq is faster in the repository's local
no-operation benchmark. Absurd has a smaller engine. ARQ has less surface area. `pgtask` chooses database consistency
and durable execution over all four.

## Compare guarantees

| Question | `pgtask` | Broker-backed queues | Absurd |
| --- | --- | --- | --- |
| Where does pending work live? | PostgreSQL | A broker such as Redis or RabbitMQ | PostgreSQL |
| Can enqueue join your application transaction? | Yes | Not without an outbox | Yes, when you use the same database transaction |
| Can a function sleep and resume after a restart? | Yes | Compose another task or add application state | Yes |
| What must you operate? | PostgreSQL and workers | A broker, workers, and sometimes a result backend or scheduler | PostgreSQL and workers |

Compare the failure model before the feature list. A fast enqueue that can disagree with your application row is not
equivalent to a committed database task. A durable step that replays after a crash is not equivalent to a chain of
messages.

## Compare performance with your workload

The repository includes a
[local comparison against Celery and Dramatiq](https://github.com/Kludex/pgtask/blob/main/benchmarks/2026-08-16-queue-comparison.md).
It measures trivial tasks on one laptop. It is useful for finding overhead. It is not a production capacity claim.

Run the same test with your handler duration, payload size, database latency, broker durability, and worker shape. Those
choices move the result more than the library name.

## Migrate after you choose

Each comparison ends with a migration section when the systems have a safe cutover path. The migration comes last
because syntax is the easy part. Storage, delivery, scheduling, and workflow guarantees decide whether you should move
at all.
