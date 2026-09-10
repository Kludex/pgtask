# Formal specifications

TLA+ models of the parts of pgtask where an interleaving can lose work. Run
them with:

```console
./scripts/check-tla.sh              # model-check every configuration
./scripts/verify-tla-coverage.sh    # prove the models are not vacuous
```

Both need a JVM and `specs/tla2tools.jar`. Set `JAVA_BIN` or `TLA2TOOLS_JAR` if
they are somewhere unusual.

## Why the transaction boundary is in the model

The obvious way to model a system whose logic lives in `SECURITY DEFINER`
functions is to make each function one atomic step. That model is easy to write,
easy to check, and cannot express the only bug these specs were built to find.

Under `READ COMMITTED`, a row a transaction has written is invisible to every
other transaction until it commits. Two functions that each read before they
write can therefore both observe "nothing there" and both proceed. Collapsing a
function into a single atomic step erases exactly the gap where that happens.

`WaitProtocol.tla` keeps the snapshot read and the commit as separate actions,
and only committed writes are visible to the other party. That is the whole
reason it finds anything.

## `WaitProtocol.tla`

Models parking a task on a wait row against the transaction that is meant to
wake it. One constant decides everything:

`SourceLocked` — does the waiter hold a row lock that the waker's write
conflicts with, for the whole span between its read and its commit?

| Configuration | `SourceLocked` | Models | Result |
| --- | --- | --- | --- |
| `SignalWait.cfg` | `TRUE` | `wait_for_signal` vs `emit_signal` | passes |
| `ResultWait.cfg` | `FALSE` | `wait_for_result` vs `complete_task` | **fails** |
| `ResultWaitRecheck.cfg` | `FALSE` | re-reading the source before parking | **fails** |
| `ResultWaitFixed.cfg` | `TRUE` | locking the child row | passes |

The two failing configurations are meant to fail. `check-tla.sh` asserts the
expected outcome per configuration, so it also fails if a known bug stops
reproducing.

Why the signal path is `TRUE` and the result path is `FALSE`:

- `wait_for_signal` holds `FOR UPDATE` on the task row. `emit_signal` inserts
  into `pgtask.signals`, whose foreign key on `task_id` needs `FOR KEY SHARE`
  on that same row. Those conflict, so the emitter blocks until the waiter
  commits. Verified against a live database: the emitter waits on a `ShareLock`
  on the waiter's transaction id.
- `wait_for_result` holds `FOR UPDATE` on the **parent** row, but the waker is
  `complete_task` on the **child** row, and that takes no lock on the parent.
  Nothing serialises them.

`ResultWaitRecheck.cfg` exists to rule out the cheap fix. Reading the source
again after inserting the wait row narrows the window but does not close it: the
waker can still commit after that second read and before the waiter commits.

### `NoConcurrentRegistration`

A lost wake-up is possible exactly when the waiter's exposed window (decided to
park, not yet committed) can overlap the waker's (scanned, not yet committed).
The same predicate is used two ways: as an invariant under `SignalWait`, where
it holding *is* the safety argument, and as a reachability witness under
`ResultWait`, where it being violated *is* the bug.

## `TaskLifecycle.tla`

Claim, lease fencing, retry budget and expiry recovery. Delivery is at-least
once, so two workers running one task concurrently is allowed; what must never
happen is two of them writing a result.

`inflight` holds handler executions that still believe they own a task.
`ExpireLease` deliberately leaves them there, because a worker whose lease
expired keeps running and will try to write later. Without that, the fencing
invariant would be checking nothing.

Invariants: `AtMostOneFencedWriter`, `RunningIffLeased`, `TerminalUnleased`,
`AttemptBounded`, `TokensUnique`. Temporal: `TerminalIsStable`,
`AttemptMonotonic`, `EventuallyTerminal`.

`TaskLifecycle.cfg` checks safety and liveness at 2 tasks / 2 workers / 2
attempts. `TaskLifecycleLarge.cfg` widens to 3 tasks but checks safety only —
liveness checking is what makes the state space explode.

## Vacuity

An invariant also holds in a model that never reaches the interesting states, so
a clean run proves nothing on its own. Each `Cover*` predicate is the negation
of a state that must be reachable; TLC reporting it violated is the witness.
`verify-tla-coverage.sh` fails if any of them is *not* violated.

The one that matters most is `CoverStaleHandler`: if a handler can never outlive
its lease, `AtMostOneFencedWriter` is vacuous and the fencing result is
worthless.

## What is not modelled

- Schedules and cron materialisation.
- Queue capacity and the `capacity_outstanding_tasks` counter.
- Idempotency key reservation and expiry.
- Retention deletion ordering.
- `LISTEN`/`NOTIFY` delivery, which is deliberately not load-bearing: persisted
  state is authoritative and a slow poll recovers anything missed.
