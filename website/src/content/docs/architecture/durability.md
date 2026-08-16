---
title: Durable execution
description: How steps, sleeps, signals, and child tasks survive a process dying.
---

A durable workflow is not an in-memory object graph that a scheduler keeps alive. It is a task that suspends itself into
the database and gets picked up again later, possibly by a different process on a different machine.

That has one consequence you have to design around: **your handler runs from the top every time it resumes.**

## Replay, not resumption

There is no stack to restore. When a suspended task becomes ready, a worker claims it and calls your handler from the
first line.

What makes that useful is that completed durable operations do not run twice. Each one stores a checkpoint keyed by
`(task_id, handler_version, step_name, occurrence)`. On replay, the step returns its stored value instead of executing.

```python
@tasks.task("reports.publish")
async def publish(task: Task, request: PublishRequest) -> PublishResult:
    # Runs once. On replay, returns the stored URL without calling the renderer again.
    url = await task.step("render", lambda: renderer.render(request["report_id"]))

    # Runs once. On replay, returns immediately if the deadline has already passed.
    await task.sleep("cool-off", seconds=300)

    await task.step("notify", lambda: mailer.send(request["email"], url))
    return {"url": url}
```

So the shape of a correct handler is: cheap deterministic work at the top, everything with a side effect inside a step.

:::caution[Work outside a step runs on every replay]
Reading a row, computing a value, building a request - fine. Charging a card, sending an email, writing a file - put it
in a step, or it happens again on every resume.
:::

## Step names are the identity

A checkpoint is found by its name and occurrence, not by position in the file. Two rules follow.

**Names must be stable across deploys.** Rename a step and the workflow no longer recognises its own history: the step
runs again. If you need to change what a step does in an incompatible way, change the handler version instead.

**Names must be unique within a run.** If you call the same step in a loop, give each iteration its own occurrence so
the second pass does not read the first pass's checkpoint.

## The primitives

Each one is a database transition. Nothing is held in worker memory.

| Primitive | What the database does |
| --- | --- |
| Step | Stores one immutable checkpoint |
| Durable sleep | Stores a checkpoint, sets a deadline, returns the task to `pending`, releases the lease |
| Signal wait | Moves the task to `waiting`, releases the lease; the signal or timeout checkpoints and returns it to `pending` |
| Child result wait | Same as a signal wait, resolved by the child reaching a terminal state |
| Child spawn | Inserts the child, records the parent, checkpoints the child ID - in one transaction |

Notice what every suspension has in common: **it releases the lease.** A workflow sleeping for six hours holds no
worker, no connection, and no memory. It is a row with a deadline. This is why a durable sleep is not
`await asyncio.sleep()` - that would pin a worker slot for six hours and lose the work if the process restarted.

## Ownership and cleanup

Child tasks form a tree through a direct parent link, and the database enforces what that implies.

- A result wait may only await a **direct child**. Cyclic waits are rejected rather than deadlocking.
- When a parent reaches a terminal state, unfinished descendants are cancelled.
- A result wait that times out cancels the awaited child subtree, so an abandoned branch does not keep running.
- Retention deletes terminal workflow leaves before their parents, so a parent is never orphaned by cleanup.

Cancellation is cooperative. A cancelled task stops at its next durable boundary; it does not kill a running handler
mid-statement.

## Choosing a handler version

Handler versions are how you make an incompatible change safely.

Change the version when you rename or reorder steps, change what a step returns, or change the retry policy. Old tasks
keep running against the old version's rules, because their checkpoints and their snapshotted policy belong to that
version.

Keep the version when you fix a bug inside a step in a way that a replay would be happy with.
