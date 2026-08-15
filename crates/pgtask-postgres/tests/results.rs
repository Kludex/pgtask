use std::time::Duration;

use pgtask_core::{EnqueueRequest, HandlerVersion, QueueName, StepName, TaskId, TaskName, TaskState, WorkerId};
use pgtask_postgres::{PostgresError, ResultWait, ResultWaitRequest, Store, TaskResultWait};
use serde_json::json;
use uuid::Uuid;

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

#[tokio::test]
async fn result_wait_replays_a_result_completed_before_registration() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let child_queue = QueueName::new(format!("result-child-{suffix}")).unwrap();
    let parent_queue = QueueName::new(format!("result-parent-{suffix}")).unwrap();
    let child_name = TaskName::new(format!("result-child-handler-{suffix}")).unwrap();
    let parent_name = TaskName::new(format!("result-parent-handler-{suffix}")).unwrap();
    let step_name = StepName::new("wait-for-child").unwrap();

    let mut child_request = EnqueueRequest::new(child_name.clone(), json!({}));
    child_request.queue_name = child_queue.clone();
    let completed_child_id = store.enqueue(&child_request).await.unwrap().task_id;
    let completed_child = store
        .claim(
            &child_queue,
            WorkerId::new(),
            &[(child_name.clone(), HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    store
        .complete(
            completed_child.id,
            completed_child.attempt,
            completed_child.lease_token.unwrap(),
            Some(&json!({"child": "complete"})),
        )
        .await
        .unwrap();

    let mut parent_request = EnqueueRequest::new(parent_name.clone(), json!({}));
    parent_request.queue_name = parent_queue.clone();
    let parent_id = store.enqueue(&parent_request).await.unwrap().task_id;
    let parent = store
        .claim(
            &parent_queue,
            WorkerId::new(),
            &[(parent_name.clone(), HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        store
            .wait_for_result(ResultWaitRequest {
                task_id: parent_id,
                attempt: parent.attempt,
                lease_token: parent.lease_token.unwrap(),
                step_name: &step_name,
                occurrence: 0,
                result_task_id: completed_child_id,
            })
            .await
            .unwrap(),
        Some(ResultWait::Ready(json!({
            "state": "succeeded",
            "result": {"child": "complete"},
            "error": null
        })))
    );
}

#[tokio::test]
async fn result_completion_resumes_a_registered_wait() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let child_queue = QueueName::new(format!("result-child-{suffix}")).unwrap();
    let parent_queue = QueueName::new(format!("result-parent-{suffix}")).unwrap();
    let child_name = TaskName::new(format!("result-child-handler-{suffix}")).unwrap();
    let parent_name = TaskName::new(format!("result-parent-handler-{suffix}")).unwrap();
    let step_name = StepName::new("wait-for-child").unwrap();
    let mut child_request = EnqueueRequest::new(child_name.clone(), json!({}));
    child_request.queue_name = child_queue.clone();
    let mut parent_request = EnqueueRequest::new(parent_name.clone(), json!({}));
    parent_request.queue_name = parent_queue.clone();

    let waiting_child_id = store.enqueue(&child_request).await.unwrap().task_id;
    let waiting_parent_id = store.enqueue(&parent_request).await.unwrap().task_id;
    let waiting_parent = store
        .claim(
            &parent_queue,
            WorkerId::new(),
            &[(parent_name.clone(), HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(waiting_parent.id, waiting_parent_id);
    assert_eq!(
        store
            .wait_for_result(ResultWaitRequest {
                task_id: waiting_parent_id,
                attempt: waiting_parent.attempt,
                lease_token: waiting_parent.lease_token.unwrap(),
                step_name: &step_name,
                occurrence: 0,
                result_task_id: waiting_child_id,
            })
            .await
            .unwrap(),
        Some(ResultWait::Waiting)
    );
    let waiting_child = store
        .claim(
            &child_queue,
            WorkerId::new(),
            &[(child_name, HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(waiting_child.id, waiting_child_id);
    store
        .complete(
            waiting_child.id,
            waiting_child.attempt,
            waiting_child.lease_token.unwrap(),
            Some(&json!({"child": "woke-parent"})),
        )
        .await
        .unwrap();
    assert_eq!(
        store.get_task(waiting_parent_id).await.unwrap().unwrap().state,
        TaskState::Pending
    );
    let resumed_parent = store
        .claim(
            &parent_queue,
            WorkerId::new(),
            &[(parent_name, HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        store
            .wait_for_result(ResultWaitRequest {
                task_id: waiting_parent_id,
                attempt: resumed_parent.attempt,
                lease_token: resumed_parent.lease_token.unwrap(),
                step_name: &step_name,
                occurrence: 0,
                result_task_id: waiting_child_id,
            })
            .await
            .unwrap(),
        Some(ResultWait::Ready(json!({
            "state": "succeeded",
            "result": {"child": "woke-parent"},
            "error": null
        })))
    );
}

#[tokio::test]
async fn external_result_wait_uses_notifications() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("external-result-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("external-result-handler-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let waiting_store = store.clone();
    let waiter = tokio::spawn(async move {
        waiting_store
            .wait_for_task_result(task_id, Some(Duration::from_secs(1)))
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    sqlx::query("SELECT pg_notify('pgtask_result', $1)")
        .bind(task_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let task = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(task_name.clone(), HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    store
        .complete(
            task.id,
            task.attempt,
            task.lease_token.unwrap(),
            Some(&json!({"done": true})),
        )
        .await
        .unwrap();
    let TaskResultWait::Ready(result) = waiter.await.unwrap() else {
        panic!("task result was not delivered");
    };
    assert_eq!(result.state, TaskState::Succeeded);
    assert_eq!(result.result, Some(json!({"done": true})));
}

#[tokio::test]
async fn external_result_wait_can_wait_without_a_deadline() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("unbounded-result-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("unbounded-result-handler-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();

    let unbounded_id = store.enqueue(&request).await.unwrap().task_id;
    let waiting_store = store.clone();
    let unbounded_waiter =
        tokio::spawn(async move { waiting_store.wait_for_task_result(unbounded_id, None).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let unbounded = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(task_name.clone(), HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    store
        .complete(unbounded.id, unbounded.attempt, unbounded.lease_token.unwrap(), None)
        .await
        .unwrap();
    assert!(matches!(
        unbounded_waiter.await.unwrap(),
        TaskResultWait::Ready(result) if result.state == TaskState::Succeeded
    ));
}

#[tokio::test]
async fn external_result_wait_reports_timeout_and_absence() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let task_name = TaskName::new(format!("absent-result-handler-{suffix}")).unwrap();
    let request = EnqueueRequest::new(task_name, json!({}));

    let disappeared_id = store.enqueue(&request).await.unwrap().task_id;
    let waiting_store = store.clone();
    let disappeared_waiter = tokio::spawn(async move {
        waiting_store
            .wait_for_task_result(disappeared_id, Some(Duration::from_secs(1)))
            .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    sqlx::query("DELETE FROM pgtask.tasks WHERE id = $1")
        .bind(disappeared_id.as_uuid())
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("SELECT pg_notify('pgtask_result', $1)")
        .bind(disappeared_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        disappeared_waiter.await.unwrap(),
        Err(PostgresError::InvalidTask(message)) if message.contains("disappeared")
    ));

    let pending_id = store.enqueue(&request).await.unwrap().task_id;
    assert_eq!(
        store
            .wait_for_task_result(pending_id, Some(Duration::from_millis(1)))
            .await
            .unwrap(),
        TaskResultWait::TimedOut
    );
    assert_eq!(
        store
            .wait_for_task_result(TaskId::new(), Some(Duration::from_millis(1)))
            .await
            .unwrap(),
        TaskResultWait::NotFound
    );
}
