# Failure model

## Delivery boundary

`pgtask` guarantees at-least-once execution. PostgreSQL commits define durability. A caller knows that a task exists only after its transaction commits successfully.

External side effects are outside the PostgreSQL transaction. A handler may complete an external operation and crash before recording its checkpoint or task completion. The operation can then run again. Use an idempotency key derived from the stable task and step identifiers.

## Fencing

Every claim creates a new attempt number and random lease token. A mutation from a running handler succeeds only when all of these values still match:

- Task identifier.
- `running` state.
- Attempt number.
- Lease token.

A stale handler receives a lease-lost result and must stop. Fencing prevents stale database writes. It cannot undo an external side effect already performed by stale code.

## Transition matrix

| Operation | Failure before commit | Failure after commit | Recovery |
| --- | --- | --- | --- |
| Enqueue | No task exists | Task is pending | Normal claim path |
| Idempotent enqueue | Reservation and task both remain absent | Reservation and task commit together | Retry returns the reserved task ID |
| Claim | Task remains pending | Task is running with a lease | Handler runs or the lease expires |
| Lease renewal | Existing lease remains | Lease expiry advances | Retry renewal or lose the lease |
| Complete | Task remains running | Task is terminal and successful | Lease expiry or no action |
| Fail with retry | Task remains running | Task is pending with future `run_at` derived from its stored policy | Lease expiry or later claim |
| Fail terminally | Task remains running | Task is terminal and failed | Lease expiry or no action |
| Cancel pending | Task remains pending | Task is cancelled | Normal claim or no action |
| Request running cancellation | Handler keeps running | Cancellation is visible | Heartbeat cancels the handler |
| Write checkpoint | Step remains absent | Step result is durable | Step runs again or is replayed |
| Sleep | Task remains running | Task is pending at wake time | Lease expiry or later claim |
| Register signal wait | Task remains running | Task is waiting | Lease expiry or signal delivery |
| Register child-result wait | Task remains running | Parent is waiting for its direct child | Child completion or database timeout |
| Child-result timeout | Parent and child remain unchanged | Parent is pending and the child subtree is cancelled | Parent replays the timeout checkpoint |
| Parent terminal transition | Parent and children remain unchanged | Unfinished descendants are cancelled | No action |
| Emit signal | Signal remains absent | Signal is immutable and durable | Emitter retries or waiter wakes |
| Materialize schedule | No task and no advance | Task insert and schedule advance commit together | Another scheduler retries |
| Retention delete | Rows remain | Terminal rows are removed | Later bounded cleanup pass |

## Process failures

### Graceful termination

The runtime stops claiming, marks the worker as draining, and waits for active handlers. Lease renewal continues during the grace period. At the deadline, remaining handlers are cancelled and their leases are allowed to expire or are released with a fenced transition.

### Panic in a handler

The runtime records a structured failure for the active attempt and applies the retry policy. A panic in one handler does not terminate unrelated handlers.

### Runtime termination

No completion is written. Automatic renewal stops. Another worker returns the task to pending after lease expiry, or
marks it failed when no attempts remain. A pending task can then be claimed with a new attempt.

Lease recovery runs independently from claiming. A failed recovery pass leaves expired leases for a later pass and does not block claims.

### Unknown handler

A worker only claims names and versions it registered. An unsupported task stays pending. Worker capability heartbeats make the mismatch visible to operators.

## PostgreSQL failures

### Connection loss while idle

The claim and notification loops reconnect with bounded exponential backoff and jitter. In-memory handler work continues. Lease renewal failure eventually cancels affected handlers before their known lease deadline.

### Connection loss during a transaction

The client treats the transaction outcome as unknown. It reconnects and reads the authoritative task state before retrying a transition. Every mutation is idempotent for the same task, attempt, and lease token.

### Primary failover

Workers reconnect and resume from committed state. Transactions acknowledged by PostgreSQL follow the durability guarantees of the configured database. `pgtask` cannot strengthen an installation that acknowledges transactions before they are durably replicated.

### Database clock movement

Claims, leases, and schedule decisions use PostgreSQL time. Worker wall clocks are not compared with stored deadlines. A large database clock adjustment may make delayed tasks early or late relative to civil time, so database time synchronization remains an operational requirement.

## Notification failures

Every worker establishes `LISTEN` before its first claim. Missing, duplicated, or coalesced notifications do not affect correctness because persisted state remains authoritative and every worker performs a low-frequency reconciliation poll. A disconnected listener reconnects and triggers an immediate claim pass.

## Scheduler failures

Materialization and advancement share one transaction. A unique schedule occurrence key prevents duplicate tasks when schedulers race. Catch-up is bounded so long downtime cannot create an unbounded transaction or task burst.

## Retention failures

Task-history and idempotency retention are independent. Task retention deletes terminal workflow leaves before parents. Idempotency retention deletes expired reservations in separate bounded batches. Deleting task history does not release an unexpired key. A failed pass leaves rows for a later pass and does not block claiming pending tasks.

## Admission failures

Queue capacity is backpressure, not task loss. A producer receives SQLSTATE `PT001` and may retry later. Scheduled
occurrences remain at the schedule cursor until space is available. Capacity-limited enqueue operations serialize on
that queue's configuration row. Unlimited queues do not use this lock.
