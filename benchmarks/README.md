# Benchmarks

## Run a local scaling sweep

The sweep needs a running development environment and a database URL pointing at it:

```console
tilt up
export PGTASK_DATABASE_URL=postgresql://pgtask:pgtask@localhost:54329/pgtask
./scripts/benchmark-scaling.sh
./scripts/benchmark-scenarios.sh
./scripts/benchmark-queue-isolation.sh
./scripts/benchmark-database-disconnect.sh
./scripts/benchmark-rust-python.sh
./scripts/benchmark-postgres-metrics.sh
```

The sweep runs the real Rust worker runtime with 1, 2, 4, 8, 16, and 32 replicas. Each result is JSON. It includes enqueue throughput and drain throughput.

Set `PGTASK_BENCH_TASKS`, `PGTASK_BENCH_BATCH_SIZE`, `PGTASK_BENCH_CONCURRENCY`, or `PGTASK_BENCH_TIMEOUT_SECONDS` to change the workload. Every run uses a new logical queue so retained history does not enter the claim path.

Set `PGTASK_BENCH_QUEUE_CAPACITY` to measure bounded admission. Use a value at least as large as
`PGTASK_BENCH_TASKS` for the standard enqueue-then-drain scenarios. This exercises the capacity-limited queue counter
without intentionally rejecting the benchmark workload.

`PGTASK_BENCH_SCENARIO` accepts `noop`, `cpu-bound`, `io-bound`, `rate-limited`, `delayed-burst`, `retry-storm`, `retained-history`, `worker-death`, `database-disconnect`, or `multi-scheduler`. The retry storm defaults to three handler attempts per task. Change it with `PGTASK_BENCH_RETRY_ATTEMPTS`.

The CPU-bound profile performs computation on the handler runtime. The I/O-bound profile suspends each handler independently. The rate-limited profile shares one paced downstream boundary across every worker in the benchmark process. Run them as separate queues with independent concurrency values.

The worker-death scenario starts one worker, waits until it owns work, aborts its runtime, then starts the remaining workers. It uses a two-second lease and verifies that every task drains after recovery. Configure at least two workers.

The database-disconnect script runs slow handlers, stops every PostgreSQL process in the Tilt pod for two seconds during execution, and verifies that the same worker runtime drains every task after PostgreSQL returns. Handlers whose completion could not commit are recovered after their leases expire and may run again.

The multi-scheduler scenario creates one due occurrence per schedule and enables scheduling on four worker runtimes. The persisted success count verifies that scheduler contention materializes and drains every unique occurrence.

The retained-history scenario drains the queue, then deletes terminal rows in bounded batches. Its JSON report includes cleanup duration and deleted task count. Use a large task count while capturing table, index, WAL, and autovacuum measurements from PostgreSQL.

The PostgreSQL metrics wrapper captures WAL bytes, locks and lock waits, connections, cache hit ratio, transaction counts, temporary bytes, deadlocks, database size, and task table and index size before and after a benchmark. Managed benchmark runs use the provider's database CPU metric.

Local index statistics identified queue-scoped lease recovery as the first tuning target. The original `(lease_expires_at, id)` index read 492,077 tuples across many independent queue runtimes. The queue-scoped index leads with `queue_name`, matching the recovery predicate while retaining deadline order inside each queue. Re-running all 19 worker integration tests produced 664 index scans that read only six tuples.

The measured defaults remain bounded: claim batches do not exceed available concurrency, lease renewals share one batch, notification wake-ups use a 30-second reconciliation fallback, and retention deletes at most 1,000 terminal rows per transaction. The high-churn task table uses a two-percent autovacuum and analyze scale factor with a 1,000-row threshold. Change these defaults only with the same workload and PostgreSQL measurements.

The Rust-Python script uses the same task count and per-worker concurrency for a Rust no-op handler and a Python async no-op handler. Both reports stop only after PostgreSQL records every task as succeeded. Build the editable Python extension before running the comparison.

On an Apple Silicon development machine with PostgreSQL 17, 100 tasks, and concurrency 10, the Rust handler drained 803 tasks per second and the Python handler drained 412 tasks per second. This single short run is an integration baseline, not a publishable performance claim.

The queue-isolation run drains a small no-op queue while a separate retry-storm queue is busy. It preserves both JSON reports so you can compare the fast queue with the unloaded baseline.

The local result is a development diagnostic. Publishable comparisons require the managed PostgreSQL environment, fixed resource limits, repeated trials, and the database measurements defined in [`PLAN.md`](../PLAN.md).

See [the bounded-admission diagnostic](2026-08-16-bounded-admission.md) for the measured cost of a hard queue limit.
