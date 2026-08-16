# pgtask

## Purpose

`pgtask` is a PostgreSQL-native durable task and workflow engine. It replaces the Redis-backed task queue and scheduler provided by ARQ, and the durable execution primitives provided by Absurd, without requiring a broker, coordinator, or PostgreSQL extension.

The engine is implemented in Rust. PostgreSQL owns durable state and atomic transitions. Rust owns claiming, leases, retries, scheduling, worker supervision, and telemetry. Language SDKs remain thin clients of the same database protocol.

## Design principles

1. PostgreSQL is the only required service.
2. Immediate, delayed, retried, scheduled, and durable work use one task state machine.
3. Every worker uses `LISTEN` and every readiness transition uses `NOTIFY`.
4. Enqueueing can participate in an existing application transaction.
5. Workers never hold a database transaction while user code runs.
6. Every state transition is fenced against stale workers.
7. Queue isolation is explicit and independently scalable.
8. OpenTelemetry support is part of the engine.
9. The default deployment has few moving parts. Optional features remain optional.
10. Complexity must be justified by a measured workload or a demonstrated failure mode.

## Guarantees

- Delivery is at least once.
- A committed task is not silently lost.
- An enqueue in a rolled-back transaction is not visible.
- A task becomes claimable only after its transaction commits and `run_at` is due.
- A worker crash makes a task claimable after its lease expires.
- Completion, failure, retry, checkpoint, and suspension writes require the active lease token.
- A stale worker cannot finalize a task reclaimed by another worker.
- Automatic lease renewal does not depend on checkpoint activity.
- Unknown task names and unsupported handler versions remain pending and observable without consuming attempts.
- One recurring schedule occurrence creates at most one task.
- Database time determines task and schedule eligibility.
- Task payloads, headers, results, errors, and checkpoint values are JSON.

The engine does not promise exactly-once external side effects. A worker can finish an external request and crash before committing its checkpoint. Task handlers must use stable idempotency keys when the external system supports them.

## State model

```text
pending -> running -> succeeded
                   -> pending      retry, sleep, or expired lease
                   -> waiting      external signal
                   -> failed
                   -> cancelled

waiting -> pending                  signal or timeout
```

An immediate task has `run_at` set to the database timestamp. A delayed task, retry, and durable sleep use a future `run_at`. A recurring schedule materializes ordinary tasks. A durable workflow is an ordinary task with checkpoints and waits.

## Architecture

### PostgreSQL

The initial schema uses logical queues in a shared task table:

| Table | Purpose |
| --- | --- |
| `pgtask.tasks` | Current task state, payload, queue, priority, timing, lease, and result |
| `pgtask.attempts` | Append-only execution history |
| `pgtask.schedules` | Recurring schedule definitions and the next due occurrence |
| `pgtask.workers` | Worker heartbeats, versions, queues, and registered handlers |
| `pgtask.checkpoints` | Durable step results |
| `pgtask.signals` | Immutable external signals |
| `pgtask.waits` | Tasks suspended for a signal or timeout |
| `pgtask.schema_migrations` | Installed schema version |

The claim path uses a partial index over pending tasks ordered by queue, priority, `run_at`, and identifier. Claims use a bounded `SELECT ... FOR UPDATE SKIP LOCKED` inside an update statement. The transaction ends before the handler starts.

Every claim receives a random lease token. The worker renews active leases in batches. Every subsequent state transition includes the task identifier, attempt number, and lease token in its predicate.

The first release uses one unpartitioned task table. Retention deletes terminal rows in bounded batches. Time-based partitioning is introduced only if the load suite demonstrates that retention, vacuum, or index locality requires it.

### Rust engine

The workspace is divided by responsibility:

```text
crates/
  pgtask-core        Domain types, state model, retry policy, handler API
  pgtask-postgres    Queries, migrations, and schema management
  pgtask-worker      Tokio worker, leases, scheduler, and shutdown
  pgtask             Public Rust API
  pgtask-cli         Administrative and diagnostic CLI
  pgtask-bench       Correctness and load generator
  pgtask-web         Optional UI service
```

The worker runtime has separate supervised loops for claims, batched lease renewal, worker heartbeats, schedule materialization, notification listening, retention, and graceful shutdown. A handler is an async Rust function registered under an explicit stable name and version.

### Language SDKs

The Python SDK and async handler bridge live in `sdks/python`. The TypeScript and Go producer SDKs live alongside it in
`sdks/typescript` and `sdks/go`.

The Rust API is the reference client. The Python package has four explicit concepts: `TaskDefinition`, `TaskRegistry`,
`Client`, and `TaskHandle`. A definition statically links the payload and result types. A registry owns one logical queue,
holds stable task names and versions, and is passed directly to a worker. It contains no global discovery. A client
submits requests. A correspondingly typed handle inspects results, waits, signals, and cancels.

TypeScript and Go provide typed producer SDKs over the same SQL protocol. They support normal and transactional enqueue,
inspection, required `LISTEN`-based result waiting, signals, cancellation, and OpenTelemetry propagation. They do not
duplicate worker execution. A future language runtime must justify its handler bridge separately from its producer API.

Normal enqueueing uses the Rust-backed client. Transactional enqueueing accepts an existing Psycopg connection and invokes the public SQL enqueue function so it participates in the caller's transaction. Migration helpers translate at the application boundary and do not expose an ARQ-shaped public facade.

Task names are stable protocol identifiers. Long-lived workflows include an explicit handler version. A worker only claims task names and versions it registered.

## Queue semantics

- A worker runtime drains one queue with one concurrency budget.
- A process may host several runtimes, but each queue keeps its own semaphore and claim loop.
- Strict priority is supported within a queue. Queue separation is the mechanism for workload isolation.
- Claim batches never exceed currently available concurrency.
- Retry policies support fixed and exponential delay with full jitter.
- Final failures remain queryable and may be retried administratively.
- Cancellation is cooperative. A running handler receives cancellation when the next heartbeat observes it.
- Graceful shutdown stops claiming, waits for handlers up to the configured grace period, and releases or expires unfinished leases safely.
- Payload and result size limits are enforced before writes. Large artifacts belong in object storage.

## Worker capacity and overload protection

Task execution is opaque to the engine. `pgtask` does not infer whether a handler is CPU-bound, I/O-bound, or limited by a downstream service. The fixed concurrency default is 10. Deployments separate workloads into queues and set concurrency, resources, and replica bounds independently for each queue.

Each worker process contains a Rust supervisor and a handler executor. The supervisor runs on a dedicated thread so a stalled handler event loop cannot also stall health sampling. This is an execution context inside the existing process, not another service or container. It owns admissions and lease health. The executor reports handler starts and completions to it.

The configured concurrency is the maximum number of active handlers. An optional overload protector may lower an in-memory effective limit when sustained event-loop lag, late lease renewal, or database failure shows that the worker cannot safely accept more work. Lowering the limit never cancels active handlers. The claim loop stops claiming until active work falls below the effective limit. Recovery increases the limit gradually and never exceeds the configured value.

The first implementation is observe-only. It exports the proposed decision without changing admissions. Fault tests and load tests must establish default thresholds before automatic reduction is enabled. Process CPU and memory remain platform metrics. They inform operators and replica autoscaling but do not cause the worker to guess the handler workload type.

Workers expose pod-local HTTP health endpoints without requiring a Kubernetes Service. `/livez` reports only whether the supervisor is making progress. `/readyz` reports whether the worker can safely claim work, including database connectivity, notification-listener health, and lease-renewal health. Dependency failure does not make `/livez` fail.

Kubernetes controls replica count separately. It consumes queue demand and pod resource health, not the supervisor's internal decision. Queue-specific Deployments may use optional KEDA or HPA custom metrics based on ready-task count or oldest-ready age. This integration is not required for correctness and does not install another `pgtask` coordinator. A scheduler-enabled Deployment must retain at least one replica unless another scheduler-enabled Deployment is guaranteed to remain available.

Comparable designs include [Azure Functions dynamic concurrency](https://learn.microsoft.com/azure/azure-functions/functions-concurrency), [Temporal resource-based worker tuners](https://temporal.io/changelog/announcing-auto-tuning-for-workers-in-pre-release), [Netflix Concurrency Limits](https://github.com/Netflix/concurrency-limits), and [KEDA event-driven scaling](https://keda.sh/docs/2.20/concepts/scaling-deployments/). The `pgtask` design applies the same separation of local admission control and replica scaling to PostgreSQL leases and queue-specific workers.

## Wake-up strategy

Every worker owns a session connection that runs `LISTEN pgtask_ready`, `LISTEN pgtask_schedule`, and `LISTEN pgtask_wait`. Result clients use `LISTEN pgtask_result`. Enqueue and reschedule transactions notify the ready channel. Schedule mutations notify the schedule channel so replicas recalculate their database-derived deadline. Signal-wait registration notifies the wait channel so replicas recalculate the next database timeout. Terminal transitions notify result clients and resume durable result waits. Notifications are the normal dispatch path.

The listener connects and commits `LISTEN` before the worker starts claiming. It reconnects automatically after failover. A low-frequency reconciliation poll covers notifications lost during disconnects because PostgreSQL notifications are not durable. Deployments must provide session-capable connections for listeners; transaction-pooling proxies cannot replace this runtime requirement.

## Scheduling

Scheduling is part of the worker runtime rather than a separate singleton service.

Each scheduler replica:

1. Claims a bounded set of due schedule rows with `SKIP LOCKED`.
2. Reads the database timestamp used for the scheduling decision.
3. Computes the due occurrences in Rust.
4. Inserts tasks with a unique `(schedule_id, scheduled_for)` identity.
5. Advances `next_run_at` in the same transaction.

The first scheduler supports one-time delayed tasks, fixed intervals, and six-field cron expressions evaluated in UTC. Misfire policies are `skip`, `latest`, and bounded `catch_up`. Named time zones are added after daylight-saving behavior is specified and tested.

Named time zones are intentionally rejected in 1.0. Enabling them requires an explicit policy for nonexistent and repeated local times, a pinned time-zone database update policy, and tests for both sides of daylight-saving transitions. UTC schedules have none of those ambiguities.

Schedules may be declared by code or created dynamically through the database API. Declarative schedules have stable names and are reconciled idempotently by workers.

## Durable execution

A task context provides:

- `step(name, operation)` to persist and reuse a JSON result.
- `sleep_until(name, timestamp)` and `sleep_for(name, duration)` to suspend without occupying a worker.
- `wait_for_signal(name, timeout)` to suspend until an immutable signal arrives.
- `spawn(...)` to create child tasks.
- `result(task_id)` and `wait_result(task_id)` to inspect task completion.
- `heartbeat()` for handlers that need an explicit cancellation or lease check.

Step names are stable identifiers. A completed step is returned without re-running its function. Code outside a step may execute again. Documentation must show how to version long-lived handlers and how to derive external idempotency keys from the task and step identities.

Dependency graphs, batches, fan-in, and Celery canvas equivalents are not part of 1.0. They may later be built from child tasks and durable waits if concrete use cases justify them.

## OpenTelemetry

The engine emits OpenTelemetry-compatible traces, metrics, and structured logs through `tracing`.

### Traces

- Producer span for enqueue.
- Consumer span for every attempt.
- Stored W3C `traceparent`, `tracestate`, and baggage propagation.
- Child spans for claims, checkpoints, retries, sleeps, signals, and schedule materialization.
- PostgreSQL spans through the database client instrumentation.

Empty polling does not create a span by default.

### Metrics

- Enqueued, claimed, succeeded, failed, retried, cancelled, and lease-lost totals.
- Queue latency and execution duration histograms.
- Schedule lag and materialization duration.
- Ready, delayed, waiting, running, and terminal task counts.
- Worker heartbeat age and active handler count.
- Configured and effective concurrency, available handler slots, event-loop lag, and lease-renewal age.
- Proposed and applied admission-limit changes with a bounded reason attribute.
- Claim batch size, empty poll count, and database operation latency.

Metric attributes remain low-cardinality. Task identifiers, payloads, results, and arbitrary idempotency keys do not become metric labels.

## Security and operations

The schema supports separate owner, producer, worker, observer, and administrator roles. Public functions validate queue names and task identifiers as values. Credentials are never stored in chart values or task telemetry. Error and result capture is size-limited.

Schema migrations are forward compatible across one rolling release. Workers check the installed schema range at startup. Migration execution uses a PostgreSQL advisory lock. Destructive schema changes require an expand-and-contract release sequence.

## Testing strategy

All state-machine behavior is exercised through the public Rust API against real PostgreSQL. Private query helpers are not treated as a separate API.

- Unit and property tests cover pure retry, schedule, and state logic.
- Integration tests run against PostgreSQL 17 and 18.
- Concurrency tests race claimers, schedulers, cancellation, completion, and lease expiry.
- Crash tests terminate workers before and after each persisted transition.
- Network tests interrupt PostgreSQL connectivity and validate recovery.
- Migration tests cover fresh install and every supported upgrade path.
- Rust coverage uses `cargo llvm-cov` with a 98 percent release gate. Every explicit source line is covered. The tracked
  raw 100 percent goal remains open while the stable toolchain attributes generated and macro-expanded spans without
  uncovered source locations.
- CI runs formatting, Clippy with warnings denied, documentation tests, dependency policy checks, and the full integration suite.

## Load testing

`pgtask-bench` produces tasks, runs no-op and configurable handlers, injects failures, and exports benchmark telemetry.

Scenarios include:

- Single and batched enqueueing.
- One through 32 worker replicas with multiple concurrency settings.
- Queue isolation under a flooded low-priority queue.
- Delayed task bursts.
- Retry storms with jitter.
- Worker death and lease recovery.
- Multiple schedulers racing on the same occurrences.
- Large retained history, cleanup, autovacuum, and index growth.
- Rust versus Python handler overhead.

Docker Compose provides repeatable database and query-plan testing. `kind` tests deployment lifecycle, pod deletion, rolling upgrades, graceful drain, and autoscaling. Publishable capacity results use an ephemeral managed PostgreSQL instance so the database does not compete with local worker containers.

Reports capture throughput, queue latency, schedule lag, duplicate attempts, PostgreSQL CPU, WAL volume, lock waits, connection count, cache hit rate, and table and index growth.

## Helm chart

The production Helm chart does not install PostgreSQL. It connects to an existing database. A development values file may enable a disposable PostgreSQL dependency for demonstrations and `kind` tests.

The chart includes:

- A migration Job protected by a PostgreSQL advisory lock.
- Configurable worker Deployments per queue.
- Independent replicas, concurrency, resources, and shutdown grace periods.
- Scheduler enablement per worker Deployment.
- Optional UI Deployment, Service, and Ingress.
- Startup, readiness, and liveness probes.
- Pod disruption budgets and topology spread constraints.
- Optional horizontal pod autoscaling.
- OpenTelemetry and OTLP configuration.
- An optional `ServiceMonitor`, rendered only when enabled.
- Existing Secret references for PostgreSQL credentials.
- ServiceAccount, NetworkPolicy, and restrictive pod security defaults.
- Extension points for environment variables, annotations, labels, volumes, and sidecars.

Chart verification runs `helm lint`, `helm template`, `kubeconform`, `chart-testing`, a `kind` installation, enqueue-and-drain smoke tests, a rolling worker upgrade, graceful termination, forced pod deletion, lease recovery, chart upgrade, and rollback.

The release publishes the chart to an OCI registry alongside signed container images.

## Optional UI

`pgtask-web` is a separate Axum service using an observer database role. The first release is read-only and server-rendered. It shows queues, tasks, attempts, schedules, workers, checkpoints, errors, and timing. Administrative retry, cancel, and schedule mutation require a separately enabled administrator role and audit trail.

## Migration strategy

An adopter pilot validates both halves of the product:

1. Move one low-risk ARQ queue while leaving its old worker registered for draining and rollback.
2. Move one Absurd workflow and validate checkpoint replay after forced termination.
3. Run both paths through the Helm deployment.
4. Compare queue latency, failures, duplicates, resource usage, and operational visibility.
5. Expand queue by queue only after the pilot gates pass.

The public API does not reproduce ARQ or Celery. Migration translates calls at the application boundary into typed payloads, explicit scheduling, and stable task definitions. There is no implicit task discovery, pickle serialization, broker/result-backend split, canvas API, or compatibility configuration surface.

## Roadmap

### Milestone 0: RFC and foundation

- Record the guarantees, state transitions, SQL protocol, failure matrix, and non-goals.
- Create the Cargo workspace, licensing, contribution guide, CI, local PostgreSQL environment, and documentation structure.
- Pin Rust 1.94 as the minimum supported Rust version and PostgreSQL 17 as the minimum database version.

Exit gate: every transition and crash point has a documented outcome.

### Milestone 1: Queue kernel spike

- Implement schema installation, enqueue, claim, lease renewal, success, failure, and retry.
- Add a minimal Rust handler registry and worker runtime.
- Add OpenTelemetry spans and metrics for the kernel.
- Build the first crash and contention tests.
- Establish an initial performance baseline.

Exit gate: concurrent and crash tests demonstrate no loss of committed tasks.

### Milestone 2: Queue alpha

- Add logical queues, priorities, delayed tasks, idempotency, cancellation, worker registration, graceful shutdown, retention, and the CLI.
- Require notification wake-ups with low-frequency reconciliation polling.
- Complete PostgreSQL 17 and 18 testing and the failure matrix.

Exit gate: the queue API is usable from Rust and all correctness suites pass.

### Milestone 3: Scheduling

- Add interval and UTC cron schedules.
- Add misfire policies, code registration, dynamic schedules, pause, resume, and deletion.
- Test multiple schedulers, clock skew, restarts, and missed occurrences.

Exit gate: every logical schedule occurrence produces at most one task.

### Milestone 4: Python SDK

- Add typed async task registration and worker execution.
- Add normal and transactional enqueue APIs.
- Add stable task definitions, explicit registries, typed requests, and task handles.
- Document how ARQ task names, deferred execution, job identifiers, attempts, and retries map to the new API.
- Publish wheels through `maturin`.

Exit gate: a Python producer and worker complete a task without Redis through the public API.

### Milestone 5: Durable execution

- Add checkpoints, sleeps, signals, result waiting, child spawning, and handler versions.
- Document replay safety and code evolution.
- Add crash-resume and overlapping-lease tests.

Exit gate: forced termination resumes from completed checkpoints without repeating them.

### Milestone 4a: TypeScript and Go producer SDKs

- Add typed task definitions, enqueueing, handles, result waiting, signals, and cancellation.
- Preserve application transactions through `pg` and pgx connection interfaces.
- Inject active OpenTelemetry context with each language's standard propagation API.
- Test the public SDKs against PostgreSQL 17 and 18 with 100 percent SDK coverage.

Exit gate: TypeScript and Go applications enqueue and await work executed by the Rust or Python runtime.

### Milestone 6: Scale validation

- Complete `pgtask-bench`, Docker Compose performance environments, and `kind` chaos tests.
- Add worker capacity telemetry and pod-local health signals.
- Validate an observe-only overload detector before it can change admission limits.
- Test an optional downward safety brake and gradual recovery under event-loop, lease, and database pressure.
- Tune claims, lease batching, polling, retention, and indexes from measurements.
- Decide whether optional partitioning is justified.

Exit gate: scaling, recovery, and database-load budgets are documented and met.

### Milestone 7: Helm chart

- Package migrations, queue-specific worker Deployments, scheduler settings, telemetry, security defaults, and optional UI resources.
- Replace database-only worker probes with the pod-local liveness and readiness endpoints.
- Add optional queue-demand autoscaling without installing or requiring KEDA.
- Prevent scale-to-zero configurations from disabling every embedded scheduler.
- Add chart validation and lifecycle tests.
- Publish the chart as an OCI artifact.

Exit gate: fresh install, upgrade, rollback, scaling, graceful drain, and pod-kill recovery pass in `kind`.

### Milestone 8: UI

- Add the observer API and read-only server-rendered interface.
- Add task, attempt, schedule, worker, and checkpoint inspection.
- Add optional audited administrative actions.

Exit gate: the default UI runs with observer-only database permissions.

### Milestone 9: Adopter pilots

- Migrate one ARQ queue and one Absurd workflow.
- Exercise deployment, rollback, drain, crash recovery, and telemetry in staging.
- Record differences from the old systems and close correctness or operational gaps.

Exit gate: both pilots meet their service objectives and rollback procedure.

### Milestone 10: 1.0 hardening

- Freeze the public API and schema compatibility policy.
- Complete security, dependency, and operational reviews.
- Publish crates, Python wheels, the TypeScript package, the Go module, images, Helm chart, reference documentation, and
  migration guides.

Exit gate: every explicit guarantee and release artifact is verified from a clean environment.

## 1.0 non-goals

- Exactly-once external side effects.
- Celery canvas compatibility.
- Arbitrary workflow DAGs and batches.
- Global concurrency and distributed rate limiting.
- Kafka-style broadcast or consumer groups.
- Per-tenant physical tables or row-level security policy management.
- Transparent payload encryption.
- Required PostgreSQL extensions.
- Headline performance claims based only on a developer laptop.
