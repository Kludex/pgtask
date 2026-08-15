# SQL protocol

## Scope

The SQL protocol is the stable cross-language boundary for transactional enqueueing and administrative integrations. Normal worker operations are implemented by `pgtask-postgres`, but follow the same state predicates.

All objects live in the `pgtask` schema. Identifiers passed by callers are values, never interpolated SQL identifiers.

## Roles

| Role | Capabilities |
| --- | --- |
| Owner | Install and migrate the schema; grant runtime roles |
| Producer | Enqueue tasks and emit signals; read explicitly returned identifiers |
| Worker | Claim and mutate leased tasks; materialize schedules; manage its own heartbeat |
| Observer | Read operational task, worker, schedule, and occurrence views without mutation |
| Administrator | Cancel, retry, configure queues and schedules, and run retention |

Runtime operations use `SECURITY DEFINER` functions owned by the schema owner. Every function fixes `search_path` to `pg_catalog, pgtask`, validates values rather than interpolating identifiers, and exposes only its fenced transition. Runtime roles do not receive direct table access.

Create the cluster roles with your infrastructure tooling, then grant their capabilities:

```console
pgtask --database-url "$PGTASK_DATABASE_URL" configure-grants \
  --owner app_pgtask_owner \
  --producer app_pgtask_producer \
  --worker app_pgtask_worker \
  --observer app_pgtask_observer \
  --administrator app_pgtask_administrator
```

The migration does not create cluster-global roles. This keeps schema installation compatible with managed PostgreSQL accounts that can grant privileges but cannot choose role names for the application.

## Public producer operations

### Enqueue

```sql
SELECT *
FROM pgtask.enqueue(
    task_name => 'send_email',
    handler_version => 1,
    payload => '{"user_id":"018f..."}'::jsonb,
    queue_name => 'notifications',
    run_at => transaction_timestamp(),
    priority => 0,
    max_attempts => 5,
    idempotency_key => 'welcome:018f...',
    headers => '{"traceparent":"00-..."}'::jsonb
);
```

The operation returns the stable task identifier and whether this call created it. The caller may execute it inside the same transaction as application writes.

### Batch enqueue

The batch operation accepts arrays or a JSON array and inserts all tasks in one transaction. It returns one result per requested item in request order. A malformed item aborts the batch.

### Emit signal

Signal identity is `(task_id, signal_name, occurrence)`. The first committed payload wins. Re-emitting the same identity returns the existing signal. Emitting before or after wait registration has the same result.

### Inspect and wait for a result

`pgtask.task_result` exposes state, result, error, and completion time for one known task identifier. A client that waits establishes `LISTEN pgtask_result` before reading this function. Every terminal transition sends the task identifier as the notification payload.

## Worker operations

### Claim

The caller provides one queue, a bounded limit, its worker identifier, lease duration, and registered handler capabilities. The operation returns only supported tasks. It creates an attempt and lease token atomically.

### Renew leases

Renewal accepts a batch of `(task_id, attempt, lease_token)` tuples. It returns which leases remain owned and which are lost or cancelled.

### Complete, fail, retry, and suspend

Each operation requires `(task_id, attempt, lease_token)`. A zero-row result means the lease was lost or the operation was already applied. A retry stores the next database timestamp. Completion and terminal failure set terminal timestamps and release the lease.

### Checkpoints

A checkpoint is identified by `(task_id, handler_version, step_name, occurrence)`. Reading a completed checkpoint returns its JSON result. Committing a result requires the active lease token.

The first committed value wins. Repeating the same step returns that value without replacing it.

### Schedule materialization

Schedulers claim due definitions with `pgtask.claim_due_schedules`. They calculate occurrences in Rust, then call `pgtask.materialize_schedule` in the same transaction. The function advances `next_run_at` and inserts ordinary tasks atomically. A unique `(schedule_id, scheduled_for)` index prevents duplicate logical occurrences.

### Signal waits

`pgtask.wait_for_signal` atomically consumes an existing immutable signal or registers a durable wait and releases the active lease. Signal delivery and timeout recovery write a checkpoint before changing the task back to `pending`. Replaying the step returns that checkpoint without registering another wait.

### Child tasks and result waits

`pgtask.spawn_task` atomically enqueues one child and checkpoints its identifier under the parent step identity. The function derives the child idempotency key from `(parent_task_id, parent_handler_version, step_name, occurrence)`.

`pgtask.wait_for_result` either checkpoints an already-terminal child or registers a durable result wait and releases the parent lease. A trigger on terminal task transitions checkpoints the child state, result, and error before waking the parent queue. This closes both result-before-wait and wait-before-result races.

Every worker establishes session-level `LISTEN` subscriptions to `pgtask_ready`, `pgtask_schedule`, and `pgtask_wait` before it claims or materializes work. A low-frequency database reconciliation remains required because notifications are not durable across disconnects.

## Observer operations

The observer reads `queue_overview`, `task_view`, `attempt_view`, `worker_view`, `worker_capability_view`, `checkpoint_view`, `signal_view`, `wait_view`, `result_wait_view`, `schedule_view`, and `schedule_occurrence_view`. It cannot read the underlying tables or invoke mutation functions.

## Value limits

| Value | Maximum encoded JSON size |
| --- | --- |
| Payload | 1 MiB |
| Headers | 64 KiB |
| Result | 1 MiB |
| Error | 256 KiB |
| Checkpoint | 1 MiB |

PostgreSQL constraints enforce these limits for Rust, Python, and direct SQL producers. A rejected transition remains atomic.

## Administrator operations

Administrative cancellation and retry record the acting principal and reason. A running cancellation is cooperative. Retrying a terminal task creates a new attempt on the same task while retaining previous attempt history.

Schedules are created or reconciled by stable name. Repeating an unchanged definition preserves its identifier, `next_run_at`, and update timestamp. Changing its definition or task resets `next_run_at`. Administrators may pause, resume, and delete definitions without deleting tasks already materialized from them.

Interval and six-field cron definitions use UTC. Misfire behavior is explicit:

| Policy | Due occurrences after downtime |
| --- | --- |
| `skip` | Materialize the oldest due occurrence and skip the remaining backlog |
| `latest` | Materialize only the most recent due occurrence |
| `catch_up` | Materialize the oldest configured number and skip the remaining backlog |

## Compatibility

The schema records one protocol version. Each binary declares an inclusive supported range. Startup fails before processing when the installed schema falls outside that range.

Migrations within one rolling release are additive. A renamed or removed column, function, state, or semantic requires an expand-and-contract sequence across releases.
