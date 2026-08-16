# SQL protocol

The SQL surface is the cross-language contract. Every SDK - Rust, Python, TypeScript, Go - calls these functions. So can
you, directly.

Clients never touch tables. Mutations go through `SECURITY DEFINER` functions; observers read `SECURITY BARRIER` views.

## Enqueue

Enqueue is the one call your application code makes most, and the only one you should run inside your own transaction:

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

Returns the stable task identifier and whether this call created it. Run it inside the same transaction as your
application writes - that is the point of the whole system.

`headers` is where trace context travels, which is how a worker span joins the producer's trace.

Idempotency keys are scoped to a queue. A reservation stays active for the task's whole nonterminal life and for the
queue's `idempotency_retention_seconds` after it finishes. It keeps returning the original identifier even after task
history is deleted.

Batch enqueue inserts every task in one transaction and returns one result per item, in request order. A malformed item
aborts the whole batch.

!!! warning "A full queue rejects with PT001"

    When a queue sets `max_outstanding_tasks`, admission above the limit fails with SQLSTATE `PT001`. An active idempotency
    reservation resolves before admission, so a duplicate enqueue still returns its original identifier while the queue is
    full.

## Claim

Claiming is the worker's half of the protocol. It hands back tasks already stamped with a lease, so no separate acknowledgement step exists:

```sql
SELECT *
FROM pgtask.claim(
    p_queue_name => 'notifications',
    p_worker_id => '018f...'::uuid,
    p_task_names => ARRAY['send_email'],
    p_handler_versions => ARRAY[1],
    p_limit => 8,
    p_lease_milliseconds => 30000
);
```

Selects with `FOR NO KEY UPDATE ... SKIP LOCKED`, filters to the capabilities you pass, creates an attempt, snapshots
the retry policy, and returns tasks stamped with a fresh lease token.

Pass only what you can actually run. Tasks you do not declare stay `pending` rather than failing.

## Lease-owned transitions

`renew_leases`, `complete_task`, `fail_task`, `suspend_task`, `commit_checkpoint`, `spawn_task`, `wait_for_signal`, and
`wait_for_result` all require the task ID, the attempt number, **and** the lease token.

They apply only while that exact lease still owns the task. A stalled worker that wakes after its lease expired matches
zero rows and cannot overwrite the worker that took over.

## Signals

Signal identity is `(task_id, signal_name, occurrence)`. The first committed payload wins, and re-emitting the same
identity returns the existing signal.

Emitting before or after the waiter registers gives the same result - there is no lost-wakeup window to design around.

## Results

`pgtask.task_result` returns state, result, error, and completion time for a task ID.

To wait rather than poll: resolve the task's deterministic `pgtask_result_*` shard, subscribe, **then** read the
function. Subscribing first is what closes the race where the task finishes between your read and your subscribe. Every
terminal transition notifies with the task identifier as the payload.

## Value limits

Enforced by check constraints, so an oversized value is rejected at write time rather than discovered later.

| Value | Maximum encoded JSON size |
| --- | --- |
| Payload | 1 MiB |
| Headers | 64 KiB |
| Result | 1 MiB |
| Error | 256 KiB |
| Checkpoint | 1 MiB |

Payloads are for identifiers and parameters. Put the bytes in object storage and pass a reference.

## Roles

| Role | Capabilities |
| --- | --- |
| Owner | Install and migrate the schema; grant runtime roles |
| Producer | Enqueue, emit signals, read returned identifiers |
| Worker | Claim and mutate leased tasks, reconcile schedules, recover waits and leases, run bounded retention, heartbeat |
| Observer | Read operational views; no mutation |
| Administrator | Cancel, retry, configure queues and schedules, run retention |

Assign them with `pgtask.configure_grants`, passing role names that match your own conventions.

## Compatibility

Before doing anything else, a client asks the database which protocols it speaks:

```sql
SELECT minimum, maximum FROM pgtask.storage_protocol_range();
```

Your client's inclusive range must overlap the database's. Do not test
`pgtask.storage_protocol_version()` for equality - it reports the current protocol, it does not define compatibility.

See [Schema compatibility](schema-compatibility.md) for how the range moves during a rollout.
