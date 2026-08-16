# Operations

## Apply migrations

```console
PGTASK_DATABASE_URL=postgresql://pgtask_owner:secret@postgres/pgtask pgtask migrate
```

Run one migration command before workers start. The migrator takes an advisory lock, so overlapping release jobs serialize. Keep the previous worker version available during a rolling release.

## Back up and restore

```console
pg_dump --format=custom --schema=pgtask --file=pgtask.dump "$PGTASK_DATABASE_URL"
createdb pgtask_restore
pg_restore --dbname=postgresql://postgres@localhost/pgtask_restore --schema=pgtask pgtask.dump
pgtask --database-url postgresql://postgres@localhost/pgtask_restore health
```

Back up the schema with the application data needed by handlers. A queue-only restore can re-run a task whose external side effect happened after the backup, so handlers must retain their external idempotency keys. Test restoration into an isolated database before treating a backup as usable.

## Configure a queue

```console
pgtask queue put default \
  --terminal-retention-seconds 604800 \
  --idempotency-retention-seconds 2592000 \
  --max-outstanding-tasks 100000 \
  --starvation-timeout-seconds 300
pgtask retention default --limit 1000
```

Omit `--max-outstanding-tasks` for an unlimited queue. A limited queue rejects new tasks with SQLSTATE `PT001` when its
pending, running, and waiting task count reaches the limit. Size workflow queues for their running parents and child
fan-out, or place child work on another queue. Existing idempotency keys continue to return their original task.

The starvation timeout reserves one claim slot for the oldest eligible task after it has waited that long. Set it to
zero for oldest-first rescue on every claim. Priority ordering fills the other slots.

Terminal history and idempotency reservations have separate per-queue retention windows. Keep idempotency retention at least as long as producers may retry a logical request. Cleanup uses bounded transactions. Run it repeatedly until it reports zero when reclaiming a backlog. Check autovacuum progress after a large cleanup. Do not use an unbounded `DELETE` against the task table.

An enqueue deduplicated after task history was removed returns the original task identifier with `created = false`. Result inspection then returns no task. This is intentional: history retention controls visibility, while idempotency retention controls whether the side effect may be requested again.

## Drain a worker

Stop new producers or route them to the replacement queue. Stop claim admission on the old Deployment and wait until its running count reaches zero. Kubernetes then sends `SIGTERM`; the worker stops claiming and waits for active handlers up to its configured grace period. A forced deletion is safe, but unfinished work waits for lease expiry and may execute again.

## Read health

```console
curl --fail http://127.0.0.1:8081/livez
curl --fail http://127.0.0.1:8081/readyz
```

`/livez` proves the dedicated supervisor is progressing. `/readyz` additionally requires claim admission, PostgreSQL connectivity, a healthy `LISTEN` session, and safe lease renewal. A database outage should fail readiness without failing liveness.

## Investigate a stuck queue

```sql
SELECT *
FROM pgtask.queue_overview
ORDER BY unroutable_count DESC, ready_count DESC;

SELECT id, task_name, state, run_at, attempt, max_attempts, lease_expires_at
FROM pgtask.task_view
WHERE queue_name = 'default'
ORDER BY run_at
LIMIT 100;

SELECT id, heartbeat_at, expires_at, live, draining
FROM pgtask.worker_view
WHERE queue_name = 'default'
ORDER BY heartbeat_at DESC;
```

If `unroutable_count` is nonzero, deploy a worker with the missing task name and handler version. Compare
`outstanding_count` with `max_outstanding_tasks` when producers report `PT001`. `ready_count` excludes delayed and
paused tasks. If `run_at` is in the future, check PostgreSQL time before changing task rows. If workers are live but
readiness fails, inspect database, listener, and lease-renewal telemetry.

## Respond to PostgreSQL loss

Keep worker processes running. They reconnect with bounded backoff, and active handlers remain fenced by their leases. Restore a session-capable endpoint for `LISTEN`; a transaction-pooling endpoint is not sufficient. After recovery, check lease-lost totals, duplicate attempts, oldest-ready age, and PostgreSQL WAL and connection pressure.

## Respond to overload

Leave overload enforcement disabled until observe-only proposals match the workload's safe limits. Separate CPU-bound, I/O-bound, and downstream-rate-limited handlers into queues. Lower per-pod concurrency before raising replica count when lease-renewal age or event-loop lag is unsafe. Kubernetes replica scaling and local admission control remain independent.

## Restore a release

Roll back application and chart configuration only while the installed schema is compatible with both worker versions. Migrations are forward-only. Use expand-and-contract changes so a rollback does not require reversing committed DDL. Route producers back first, keep the new workers draining committed tasks, then remove them after their queue is empty.
