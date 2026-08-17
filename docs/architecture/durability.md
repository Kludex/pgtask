# Durable execution

A workflow engine has to answer an awkward question: where does the workflow live while it is waiting?

If it lives in memory, a deploy kills it. If it lives in a dedicated service, you have added the coordinator this system
exists to avoid. `pgtask` puts it in the database and accepts the consequence that follows.

That consequence is worth stating plainly, because everything else is downstream of it: **your handler runs from the top
every time it resumes.** There is no stack to restore. When a suspended task becomes ready, a worker claims it and calls
your handler at line one, possibly on a different machine.

## Replay, and what makes it tolerable

Running from the top would be useless if it meant doing the work again. It does not, because each durable operation
stores a checkpoint keyed by `(task_id, handler_version, step_name, occurrence)`. On replay the step returns its stored
value instead of executing:

```python
@tasks.task("reports.publish")
async def publish(task: Task, request: PublishRequest) -> PublishResult:
    # Runs once. On replay, returns the stored URL without calling the renderer again.
    url = await task.step("render", lambda: renderer.render(request["report_id"]))

    # Runs once. On replay, returns immediately if the deadline has already passed.
    await task.sleep_for("cool-off", 300)

    await task.step("notify", lambda: mailer.send(request["email"], url))
    return {"url": url}
```

This is event sourcing with a very small vocabulary. The checkpoints are the log, the handler is the fold over it, and
replay is how state is reconstructed. Knowing that tells you where the sharp edge is: the handler has to be a
deterministic function of its checkpoints, or replay produces something different from the original run.

So the shape of a correct handler is cheap deterministic work at the top, and everything with a side effect inside a
step.

!!! warning "Work outside a step runs on every replay"

    Reading a row, computing a value, building a request are all fine. Charging a card, sending an email, or writing a
    file will happen again on every resume unless it is inside a step.

## Step names are the identity

A checkpoint is found by its name and occurrence, not by its position in the file. Two rules follow, and both bite
during ordinary refactoring.

**Names must be stable across deploys.** Rename a step and the workflow no longer recognises its own history, so the
step runs again. A rename is a refactor everywhere else in your codebase; here it is a semantic change. If you need to
change what a step does incompatibly, change the handler version instead.

**Names must be unique within a run.** If you call the same step in a loop, give each iteration its own occurrence, or
the second pass reads the first pass's checkpoint.

## The primitives

Each one is a database transition. Nothing is held in worker memory:

| Primitive | What the database does |
| --- | --- |
| Step | Stores one immutable checkpoint |
| Durable sleep | Stores a checkpoint, sets a deadline, returns the task to `pending`, releases the lease |
| Signal wait | Moves the task to `waiting`, releases the lease; the signal or timeout checkpoints and returns it to `pending` |
| Child result wait | Same as a signal wait, resolved by the child reaching a terminal state |
| Child spawn | Inserts the child, records the parent, checkpoints the child ID, in one transaction |

Notice what every suspension has in common: **it releases the lease.** A workflow sleeping for six hours holds no
worker, no connection, and no memory. It is a row with a deadline.

This is the payoff for accepting replay. `await asyncio.sleep(21600)` would pin a worker slot for six hours and lose the
work if the process restarted; a durable sleep costs a row and survives anything. The awkward property and the useful
one are the same property.

## Ownership and cleanup

Child tasks form a tree through a direct parent link, and the database enforces what that implies:

- A result wait may only await a **direct child**. Cyclic waits are rejected rather than deadlocking.
- When a parent reaches a terminal state, unfinished descendants are cancelled.
- A result wait that times out cancels the awaited child subtree, so an abandoned branch does not keep running.
- Retention deletes terminal workflow leaves before their parents, so a parent is never orphaned by cleanup.

Cancellation is cooperative. A cancelled task stops at its next durable boundary; it does not kill a running handler
mid-statement. That is a deliberate limit - a handler in the middle of a step will finish that step.

## Choosing a handler version

Handler versions are how you make an incompatible change safely.

Change the version when you rename or reorder steps, change what a step returns, or change the retry policy. Old tasks
keep running against the old version's rules, because their checkpoints and their snapshotted policy belong to that
version.

Keep the version when you fix a bug inside a step in a way that a replay would be happy with.

For the handler API and the replay rules in detail, see [Durable execution](../durable-execution.md).
