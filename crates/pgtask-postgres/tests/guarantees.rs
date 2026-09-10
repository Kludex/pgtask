//! Deterministic tests for individual documented guarantees.
//!
//! `model.rs` explores randomly, which is good at finding states nobody thought
//! of but bad at guaranteeing a specific boundary is ever reached. These pin the
//! boundaries directly: one guarantee per test, no timing, no concurrency.

use std::time::Duration;

use pgtask_core::{
    EnqueueRequest, HandlerVersion, QueueName, SignalName, StepName, Task, TaskName, TaskState, WorkerId,
};
use pgtask_postgres::{SpawnRequest, Store};
use serde_json::json;
use uuid::Uuid;

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

fn request(task_name: &TaskName, queue_name: &QueueName, max_attempts: u16, priority: i16) -> EnqueueRequest {
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    request.max_attempts = max_attempts;
    request.priority = priority;
    request
}

async fn claim(store: &Store, queue: &QueueName, name: &TaskName, limit: u16) -> Vec<Task> {
    store
        .claim(
            queue,
            WorkerId::new(),
            &[(name.clone(), HandlerVersion::default())],
            limit,
            Duration::from_mins(10),
        )
        .await
        .unwrap()
}

/// A fresh queue and task name, so tests never see each other's rows.
fn names(prefix: &str) -> (QueueName, TaskName) {
    let suffix = Uuid::new_v4();
    (
        QueueName::new(format!("{prefix}-{suffix}")).unwrap(),
        TaskName::new(format!("{prefix}-task-{suffix}")).unwrap(),
    )
}

/// `claim` filters on `attempt < max_attempts`. This constructs that boundary
/// directly rather than through a handler, because the filter has to hold
/// however the row got there.
#[tokio::test]
async fn claim_skips_a_task_that_has_exhausted_its_attempts() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let (queue, task_name) = names("exhausted");

    let task_id = store.enqueue(&request(&task_name, &queue, 2, 0)).await.unwrap().task_id;
    sqlx::query("UPDATE pgtask.tasks SET attempt = max_attempts WHERE id = $1")
        .bind(task_id.as_uuid())
        .execute(store.pool())
        .await
        .unwrap();

    let claimed = claim(&store, &queue, &task_name, 10).await;
    assert!(
        claimed.is_empty(),
        "claim handed out a task that had already used all {} of its attempts",
        2
    );
}

/// Recovery must retire a task whose lease expired on its last attempt, rather
/// than returning it to `pending` where nothing would ever claim it.
#[tokio::test]
async fn recovery_fails_a_task_with_no_attempts_left() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let (queue, task_name) = names("no-attempts");

    store.enqueue(&request(&task_name, &queue, 1, 0)).await.unwrap();
    let task = claim(&store, &queue, &task_name, 1).await.pop().unwrap();
    assert_eq!(task.attempt, 1, "the only attempt");

    sqlx::query("UPDATE pgtask.tasks SET lease_expires_at = statement_timestamp() - interval '1 second' WHERE id = $1")
        .bind(task.id.as_uuid())
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(store.recover_expired(&queue, 10).await.unwrap(), 1);

    let recovered = store.get_task(task.id).await.unwrap().unwrap();
    assert_eq!(
        recovered.state,
        TaskState::Failed,
        "a task out of attempts must be retired, not left pending where no worker can take it"
    );
}

/// Higher priority is claimed first.
#[tokio::test]
async fn higher_priority_is_claimed_first() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let (queue, task_name) = names("priority");

    // Enqueued low first, so claim order cannot be explained by insertion order.
    let low = store.enqueue(&request(&task_name, &queue, 5, 0)).await.unwrap().task_id;
    let high = store.enqueue(&request(&task_name, &queue, 5, 9)).await.unwrap().task_id;

    let first = claim(&store, &queue, &task_name, 1).await.pop().expect("a task");
    assert_eq!(
        first.id, high,
        "claim took the priority 0 task before the priority 9 one"
    );
    assert_ne!(first.id, low);
}

/// A parent reaching a terminal state cancels its unfinished descendants, and
/// leaves the finished ones alone.
#[tokio::test]
async fn a_finished_parent_cancels_only_its_unfinished_children() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let (queue, parent_name) = names("descendants");
    let child_name = TaskName::new(format!("{parent_name}-child")).unwrap();

    store.enqueue(&request(&parent_name, &queue, 5, 0)).await.unwrap();
    let parent = claim(&store, &queue, &parent_name, 1).await.pop().unwrap();

    let mut children = Vec::new();
    for occurrence in 0..2 {
        children.push(
            store
                .spawn_task(SpawnRequest {
                    parent_task_id: parent.id,
                    parent_attempt: parent.attempt,
                    parent_lease_token: parent.lease_token.unwrap(),
                    step_name: &StepName::new("spawn").unwrap(),
                    occurrence,
                    task: &request(&child_name, &queue, 5, 0),
                })
                .await
                .unwrap()
                .unwrap()
                .task_id,
        );
    }

    // Finish one child; leave the other pending.
    let claimed = claim(&store, &queue, &child_name, 2).await;
    let finished = claimed.iter().find(|task| task.id == children[0]).unwrap();
    assert!(
        store
            .complete(
                finished.id,
                finished.attempt,
                finished.lease_token.unwrap(),
                Some(&json!({"ok": true}))
            )
            .await
            .unwrap()
    );

    assert!(
        store
            .complete(parent.id, parent.attempt, parent.lease_token.unwrap(), Some(&json!({})))
            .await
            .unwrap()
    );

    assert_eq!(
        store.get_task(children[0]).await.unwrap().unwrap().state,
        TaskState::Succeeded,
        "a child that had already succeeded must not be rewritten to cancelled"
    );
    assert_eq!(
        store.get_task(children[1]).await.unwrap().unwrap().state,
        TaskState::Cancelled,
        "the unfinished child must be cancelled with its parent"
    );
}

/// A durable sleep is not a failure, so resuming from one must leave the task
/// claimable — including on its last attempt.
#[tokio::test]
#[ignore = "reproduces #28: sleeping on the final attempt strands the task. Un-ignore with the fix."]
async fn a_task_that_sleeps_on_its_final_attempt_is_still_claimable() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let (queue, task_name) = names("strand-sleep");

    store.enqueue(&request(&task_name, &queue, 1, 0)).await.unwrap();
    let task = claim(&store, &queue, &task_name, 1).await.pop().unwrap();
    assert_eq!(task.attempt, 1);

    store
        .sleep_for(
            task.id,
            task.attempt,
            task.lease_token.unwrap(),
            &StepName::new("nap").unwrap(),
            0,
            Duration::ZERO,
        )
        .await
        .unwrap()
        .expect("the sleep is accepted");

    let woken = store.get_task(task.id).await.unwrap().unwrap();
    assert_eq!(woken.state, TaskState::Pending, "the task parks as pending");

    assert!(
        !claim(&store, &queue, &task_name, 10).await.is_empty(),
        "a task that slept on its final attempt is pending and due, but no worker can \
         claim it and no sweep recovers it: it is stranded forever"
    );
}

/// The same, via a signal wait.
#[tokio::test]
#[ignore = "reproduces #28: a signal wake on the final attempt strands the task. Un-ignore with the fix."]
async fn a_task_woken_by_a_signal_on_its_final_attempt_is_still_claimable() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let (queue, task_name) = names("strand-signal");
    let signal_name = SignalName::new("go").unwrap();

    store.enqueue(&request(&task_name, &queue, 1, 0)).await.unwrap();
    let task = claim(&store, &queue, &task_name, 1).await.pop().unwrap();

    store
        .wait_for_signal(pgtask_postgres::SignalWaitRequest {
            task_id: task.id,
            attempt: task.attempt,
            lease_token: task.lease_token.unwrap(),
            step_name: &StepName::new("await").unwrap(),
            occurrence: 0,
            signal_name: &signal_name,
            signal_occurrence: 0,
            timeout: None,
        })
        .await
        .unwrap()
        .expect("the wait is accepted");
    store.emit_signal(task.id, &signal_name, 0, &json!({})).await.unwrap();

    assert_eq!(
        store.get_task(task.id).await.unwrap().unwrap().state,
        TaskState::Pending,
        "the signal wakes the task back to pending"
    );
    assert!(
        !claim(&store, &queue, &task_name, 10).await.is_empty(),
        "a task woken by a signal on its final attempt cannot be claimed again"
    );
}
