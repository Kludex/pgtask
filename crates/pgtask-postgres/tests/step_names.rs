//! Step names carry whatever a composing framework produced, so the charset the other names keep
//! must not reach the three tables that store one.

use std::time::Duration;

use pgtask_core::{
    EnqueueRequest, EnqueueResult, HandlerVersion, QueueName, SignalName, StepName, Task, TaskName, TaskState, WorkerId,
};
use pgtask_postgres::{ResultWait, ResultWaitRequest, SignalWait, SignalWaitRequest, SpawnRequest, Store};
use serde_json::json;
use uuid::Uuid;

/// The shape a framework composes on a caller's behalf: an agent name in angle brackets, a dotted
/// path, a separator, and text outside ASCII. None of it survived the original character set.
const RELAXED: &str = "pydantic_ai__function_toolset__<agent>.call_tool:tool 1/2 ✓";

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

async fn claim_one(store: &Store, queue_name: &QueueName, task_name: &TaskName) -> Task {
    store
        .claim(
            queue_name,
            WorkerId::new(),
            &[(task_name.clone(), HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn spawn_child(store: &Store, parent: &Task, step_name: &StepName, task: &EnqueueRequest) -> EnqueueResult {
    store
        .spawn_task(SpawnRequest {
            parent_task_id: parent.id,
            parent_attempt: parent.attempt,
            parent_lease_token: parent.lease_token.unwrap(),
            step_name,
            occurrence: 0,
            task,
        })
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn relaxed_step_names_checkpoint_spawn_and_await_a_result() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let parent_queue = QueueName::new(format!("relaxed-parent-{suffix}")).unwrap();
    let child_queue = QueueName::new(format!("relaxed-child-{suffix}")).unwrap();
    let parent_name = TaskName::new(format!("relaxed-parent-task-{suffix}")).unwrap();
    let child_name = TaskName::new(format!("relaxed-child-task-{suffix}")).unwrap();
    let mut parent_request = EnqueueRequest::new(parent_name.clone(), json!({}));
    parent_request.queue_name = parent_queue.clone();
    let mut child_request = EnqueueRequest::new(child_name.clone(), json!({}));
    child_request.queue_name = child_queue.clone();

    let parent_id = store.enqueue(&parent_request).await.unwrap().task_id;
    let parent = claim_one(&store, &parent_queue, &parent_name).await;
    let lease_token = parent.lease_token.unwrap();

    let step_name = StepName::new(RELAXED).unwrap();
    let committed = store
        .commit_checkpoint(
            parent_id,
            parent.attempt,
            lease_token,
            &step_name,
            0,
            &json!({"charged": true}),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.step_name, step_name);
    assert_eq!(
        store
            .get_checkpoint(parent_id, HandlerVersion::default(), &step_name, 0)
            .await
            .unwrap()
            .unwrap()
            .value,
        json!({"charged": true})
    );

    let spawn_step = StepName::new(format!("{RELAXED}/spawn")).unwrap();
    let child = spawn_child(&store, &parent, &spawn_step, &child_request).await;
    assert!(child.created);
    let replayed = spawn_child(&store, &parent, &spawn_step, &child_request).await;
    assert!(!replayed.created);
    assert_eq!(replayed.task_id, child.task_id);

    // The child idempotency key joins the step name and occurrence with a colon. Only the trailing
    // occurrence is digits, so a step name that ends in one cannot collide with its neighbour.
    let ambiguous_step = StepName::new(format!("{spawn_step}:0")).unwrap();
    let ambiguous = spawn_child(&store, &parent, &ambiguous_step, &child_request).await;
    assert!(ambiguous.created);
    assert_ne!(ambiguous.task_id, child.task_id);

    let await_step = StepName::new(format!("{RELAXED}/await")).unwrap();
    assert_eq!(
        store
            .wait_for_result(ResultWaitRequest {
                task_id: parent_id,
                attempt: parent.attempt,
                lease_token,
                step_name: &await_step,
                occurrence: 0,
                result_task_id: child.task_id,
                timeout: None,
            })
            .await
            .unwrap(),
        Some(ResultWait::Waiting)
    );
}

#[tokio::test]
async fn relaxed_step_names_register_a_signal_wait() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("relaxed-signal-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("relaxed-signal-task-{suffix}")).unwrap();
    let signal_name = SignalName::new("approval").unwrap();
    let step_name = StepName::new(RELAXED).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();

    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let task = claim_one(&store, &queue_name, &task_name).await;
    assert_eq!(
        store
            .wait_for_signal(SignalWaitRequest {
                task_id,
                attempt: task.attempt,
                lease_token: task.lease_token.unwrap(),
                step_name: &step_name,
                occurrence: 0,
                signal_name: &signal_name,
                signal_occurrence: 0,
                timeout: None,
            })
            .await
            .unwrap(),
        Some(SignalWait::Waiting)
    );

    store
        .emit_signal(task_id, &signal_name, 0, &json!({"approved": true}))
        .await
        .unwrap();
    assert_eq!(
        store.get_task(task_id).await.unwrap().unwrap().state,
        TaskState::Pending
    );
    assert_eq!(
        store
            .get_checkpoint(task_id, HandlerVersion::default(), &step_name, 0)
            .await
            .unwrap()
            .unwrap()
            .value,
        json!({"outcome": "signal", "value": {"approved": true}})
    );
}
