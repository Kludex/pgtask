---
title: The storage boundary
description: The SQL surface clients depend on, and the role model that enforces it.
---

Clients do not read and write tables. They call functions.

Every mutation goes through a `SECURITY DEFINER` function in the `pgtask` schema. Runtime roles hold no direct table
privileges at all:

```sql
REVOKE ALL ON SCHEMA pgtask FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA pgtask FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pgtask FROM PUBLIC;
```

That indirection is what makes the SQL surface a real protocol rather than a description of the current table layout.
Column names, index choices, and normalisation can change in a release; the function signatures are the contract.

## The five roles

`pgtask.configure_grants` assigns privileges to roles you supply, so you can name them to match your own conventions.

| Role | Can do |
| --- | --- |
| Owner | Everything; owns the schema and applies migrations |
| Producer | Enqueue, emit signals, read a task result |
| Worker | Claim, renew, complete, fail, checkpoint, spawn children, register itself |
| Observer | Read the observer views. Nothing else |
| Administrator | Observer reads, plus audited cancel, retry, and schedule control |

Give the application that enqueues work the producer role and it becomes unable to claim tasks, mark them succeeded, or
read another tenant's payloads directly. The restriction is enforced by PostgreSQL, not by your client library
remembering to behave.

## Observer views

Observers read `SECURITY BARRIER` views, never the tables:

`queue_overview`, `task_view`, `attempt_view`, `worker_view`, `worker_capability_view`, `checkpoint_view`,
`schedule_view`, `schedule_occurrence_view`, `signal_view`, `wait_view`, `result_wait_view`, `handler_policy_view`,
`administrator_audit_view`.

The barrier matters: without it, a cleverly written predicate in a user query can be evaluated before the view's own
filtering and leak rows the view was meant to hide.

## What lives in the schema

Logical queues share one `tasks` table with partial indexes for the hot paths - the claim path, the expired-lease path,
and the capacity path. Separate tables hold attempt history, checkpoints, signals, waits, schedules, worker
registrations, idempotency reservations, handler policies, and administrator audit records.

Two decisions there are worth calling out, because both look like normalisation mistakes and are not.

**Idempotency reservations have their own retention.** A reservation stays active while its task is unfinished, then
expires after the queue's idempotency retention. Deleting terminal task history does **not** release the key. If it did,
your deduplication window would silently become whatever your observability retention happened to be.

**Audit rows keep their task and schedule identifiers after the target is deleted.** There is deliberately no foreign
key. An audit record that vanishes when someone deletes the thing it describes is not an audit record.

## Capacity is a counter, not a query

A queue can set a hard outstanding-task limit. Admission increments an O(1) counter and rejects the insert with SQLSTATE
`PT001` when the queue is full.

The obvious implementation - `COUNT(*)` on admission - gets slower exactly as a queue gets busier, which is the worst
possible time. The counter keeps admission constant-time.

Queues without a limit skip the counter entirely, so they never touch that coordination point.

:::caution[Set the limit before producers start]
The counter is initialised when you configure the limit. Configure it first so its starting count is stable.
:::

Scheduled catch-up respects capacity: it materializes only the occurrences that fit and leaves the rest due.

## The protocol handshake

The schema and every client declare an inclusive storage protocol range. The ranges must overlap.

```sql
SELECT minimum, maximum FROM pgtask.storage_protocol_range();
```

Workers check before registering. Normal clients check at connection or first use.

This turns rolling compatibility into an explicit database contract instead of an assumption about package versions. A
worker built against a protocol the database does not speak refuses to start, with both ranges in the error, rather than
failing later on a function signature that moved.

The range - rather than a single version - is what makes rolling upgrades work. Raise `maximum` to support a new schema
while keeping `minimum`, and the new binary runs against the old database during the transition.

:::note[An empty database is not an incompatible one]
"Not installed yet" and "installed at a version I cannot speak" are different answers. A client connecting to a database
with no `pgtask` schema is told the schema is absent, so it can migrate and ask again.
:::

Schema changes within a rolling release stay additive, so old and new binaries can run at once. See
[Schema compatibility](/pgtask/reference/schema-compatibility/).
