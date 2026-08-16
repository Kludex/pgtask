# pgtask

`pgtask` is a durable task and workflow engine for PostgreSQL.

You store tasks in your database and run them from your own processes. There is no broker, no coordinator, and no
PostgreSQL extension.

You enqueue a task in the same transaction that writes your application data. If the transaction rolls back, the task
never existed. That one property removes most of the consistency problems queues usually introduce:

```python
async with connection.transaction():
    await create_report(connection, report_id)
    await client.enqueue_in(connection, render.request({"report_id": report_id}))
```

!!! warning "Not ready for production"

    `pgtask` is under active development. The contracts described here are still changing.

## What you get

Four properties describe the engine better than a feature list.

**One dependency.** PostgreSQL owns every durable transition. Workers are ordinary processes you deploy and scale
yourself.

**Leases, not acknowledgements.** A worker holds a fenced lease while it runs your handler. If the worker disappears,
the task returns automatically.

**Durable workflows.** Steps, sleeps, signals, and child tasks are database transitions, so a workflow survives a
process restart.

**One protocol, many languages.** Rust, Python, TypeScript, Go, and plain SQL speak the same versioned set of database
functions.

## Where to start

Pick the entry point that matches what you need right now.

- [What pgtask is](start/what-pgtask-is.md) explains the guarantees and the limits before you commit to it.
- [Your first task](start/first-task.md) gets a handler running.
- [The shape of the system](architecture/index.md) explains how it works and why it is built this way.
