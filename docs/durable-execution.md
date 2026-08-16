# Durable execution

## Checkpoint a step

```rust
use std::error::Error;

use pgtask::core::{HandlerVersion, RetryPolicy, StepName, TaskId, TaskName};
use pgtask::worker::{HandlerError, HandlerRegistry};
use serde_json::{Value, json};

async fn capture_payment(task_id: TaskId) -> Result<Value, HandlerError> {
    Ok(json!({"payment_id": task_id.to_string()}))
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut registry = HandlerRegistry::new();
    let step_name = StepName::new("capture-payment")?;
    registry.register_durable(
        TaskName::new("billing.capture")?,
        HandlerVersion::default(),
        RetryPolicy::default(),
        move |task, context| {
            let step_name = step_name.clone();
            async move {
                context
                    .step(&step_name, 0, || capture_payment(task.id))
                    .await
            }
        },
    );
    Ok(())
}
```

`step` reads the checkpoint before it calls the operation. The first committed JSON value wins. A retry returns that value without calling the operation again.

The operation can still finish an external side effect and lose its lease before the checkpoint commits. Derive an external idempotency key from `(task_id, handler_version, step_name, occurrence)` whenever the external system supports one.

## Name repeated steps

Use a stable step name for the source-code location. Use `occurrence = 0` for a step that appears once. For a repeated step, increment `occurrence` from a sequence stored in durable input or prior checkpoints.

Do not derive an occurrence from the attempt number, process time, a random value, or the worker identity. Those values change during replay and would execute the operation again.

Checkpoints are scoped by handler version. Bump the handler version when a deployed code change alters the meaning, order, or result shape of an existing step. Keep the old handler registered until its tasks have finished or have been migrated explicitly.

## Sleep without occupying a worker

```rust
context
    .sleep_for(
        &StepName::new("wait-before-retry")?,
        0,
        std::time::Duration::from_secs(30),
    )
    .await?;
```

The first call commits the sleep checkpoint, changes the task back to `pending`, releases its lease, and sets `run_at` in one transaction. The runtime stops the current attempt. After the database deadline, another attempt replays the checkpoint and continues after `sleep_for`.

`sleep_until` accepts an absolute UTC timestamp. `sleep_for` calculates its timestamp inside PostgreSQL, so worker clock skew does not change the delay.

## Wait for an external signal

```rust
use pgtask::core::{SignalName, StepName};
use serde_json::{Value, json};

async fn wait_for_approval(context: pgtask::worker::TaskContext) -> Result<Value, pgtask::worker::HandlerError> {
    let approval = context
        .wait_for_signal(
            &StepName::new("wait-for-approval").expect("valid step name"),
            0,
            &SignalName::new("approval").expect("valid signal name"),
            0,
            Some(std::time::Duration::from_secs(24 * 60 * 60)),
        )
        .await?;
    Ok(json!({"approval": approval}))
}
```

`wait_for_signal` checks for the signal and registers the wait in one database transaction. The task releases its lease while it waits. A signal or database-derived timeout changes the task back to `pending` and wakes workers through PostgreSQL notification channels.

Signal identity is `(task_id, signal_name, occurrence)`. The first committed JSON value wins. An emitted signal remains available when it arrives before the task starts waiting. A timeout returns `None`; a signal returns `Some(value)`.

Use a stable occurrence when a workflow waits for the same named signal more than once. The signal occurrence and step occurrence are independent: the first identifies the external event, and the second identifies the durable checkpoint in the handler.

## Spawn a child and wait for its result

```rust
use pgtask::core::{EnqueueRequest, StepName, TaskName};
use pgtask::worker::{HandlerError, TaskContext};
use serde_json::{Value, json};

async fn run_child(context: TaskContext) -> Result<Value, HandlerError> {
    let child = EnqueueRequest::new(
        TaskName::new("reports.render").expect("valid task name"),
        json!({"report_id": "report-123"}),
    );
    let child_id = context
        .spawn(
            &StepName::new("spawn-report").expect("valid step name"),
            0,
            &child,
        )
        .await?;
    context
        .wait_for_result(
            &StepName::new("wait-for-report").expect("valid step name"),
            0,
            child_id,
            Some(std::time::Duration::from_secs(10 * 60)),
        )
        .await
}
```

`spawn` inserts the child, records its immutable parent, and checkpoints its identifier in one transaction. The step identity supplies the child idempotency key, so replay returns the same task identifier. The child can use another queue and handler version through its `EnqueueRequest`.

`wait_for_result` only accepts a direct child of the current task. This ownership rule prevents result-wait cycles. It returns a checkpoint object containing `state`, `result`, and `error`, then releases the parent lease while the child runs. A database trigger commits the parent checkpoint and wakes its queue when the child succeeds, fails, or is cancelled.

A result timeout returns a checkpoint with `state` set to `timeout`. It cancels the unfinished child and its descendants. When a parent succeeds, fails, or is cancelled, PostgreSQL cancels every unfinished descendant. Retention deletes terminal leaves before their parents, so an active workflow never loses its ownership chain.

For a client that is not inside a task handler, use `Store::task_result` for inspection or `Store::wait_for_task_result` for notification-driven waiting. The latter establishes `LISTEN pgtask_result` before checking task state, so completion cannot be lost between subscription and inspection.

## Write replay-safe handlers

```rust
use pgtask::core::{StepName, TaskId};
use pgtask::worker::{HandlerError, TaskContext};
use serde_json::{Value, json};

async fn charge_card(total: u64, idempotency_key: String) -> Result<Value, HandlerError> {
    Ok(json!({"total": total, "idempotency_key": idempotency_key}))
}

async fn run(task_id: TaskId, context: TaskContext) -> Result<Value, HandlerError> {
    context
        .step(
            &StepName::new("charge-card").expect("valid step name"),
            0,
            || charge_card(4_200, format!("{task_id}:1:charge-card:0")),
        )
        .await
}
```

Assume code outside a completed checkpoint can run again. A database transaction makes pgtask state transitions atomic, but it cannot atomically commit a request to another service. Pass a stable idempotency key to that service. Derive it from the task identifier, handler version, step name, and occurrence.

If the external system has no idempotency facility, the handler remains at-least-once at that boundary. Store enough reconciliation data to detect and repair duplicates. Do not describe such a step as exactly-once.

## Change long-running handlers

Register a new `HandlerVersion` when a release changes step order, step meaning, signal identity, child identity, checkpoint result shape, or retry policy. Keep the old version registered while tasks using it remain nonterminal.

Safe changes inside one version include performance improvements and bug corrections that preserve the same durable protocol. Renaming a Rust function is safe. Renaming its stable `TaskName` or `StepName` is a protocol change.

Do not deploy a new implementation under an old version and assume suspended tasks restart from the top. They resume from stored checkpoints. If you cannot keep the old handler, write an explicit data migration that transforms checkpoints and record the supported source and target versions.

The first worker registration makes the retry policy immutable for the queue, task name, and handler version. Tasks snapshot that policy before their first attempt. Restarting or replacing workers cannot change their remaining retry delays.
