use std::{str::FromStr, time::Duration};

use pgtask_core::{
    EnqueueRequest, HandlerVersion, QueueConfig, QueueName, StepName, Task, TaskId, TaskName, TaskState, WorkerId,
};
use pgtask_postgres::{PostgresError, ResultWait, ResultWaitRequest, SpawnRequest, Store, TaskResultWait};
use serde_json::json;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::time::timeout;
use uuid::Uuid;

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

async fn spawn_child(store: &Store, parent: &Task, step_name: &str, request: &EnqueueRequest) -> TaskId {
    store
        .spawn_task(SpawnRequest {
            parent_task_id: parent.id,
            parent_attempt: parent.attempt,
            parent_lease_token: parent.lease_token.unwrap(),
            step_name: &StepName::new(step_name).unwrap(),
            occurrence: 0,
            task: request,
        })
        .await
        .unwrap()
        .unwrap()
        .task_id
}

#[tokio::test]
async fn result_waiters_share_one_listener_connection() {
    let Some(database_url) = database_url() else {
        return;
    };
    let query_pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    let listener_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let store = Store::from_pools(query_pool, listener_pool);
    store.migrate().await.unwrap();

    let first = store
        .enqueue(&EnqueueRequest::new(
            TaskName::new("result-multiplex-first").unwrap(),
            json!({}),
        ))
        .await
        .unwrap()
        .task_id;
    let second = store
        .enqueue(&EnqueueRequest::new(
            TaskName::new("result-multiplex-second").unwrap(),
            json!({}),
        ))
        .await
        .unwrap()
        .task_id;
    let mut first_listener = store.result_listener(first).await.unwrap();
    let mut second_listener = timeout(Duration::from_secs(1), store.result_listener(second))
        .await
        .expect("a shared listener does not need a second database connection")
        .unwrap();

    sqlx::query("UPDATE pgtask.tasks SET state = 'cancelled', completed_at = statement_timestamp() WHERE id = ANY($1)")
        .bind([first.as_uuid(), second.as_uuid()])
        .execute(store.pool())
        .await
        .unwrap();
    let first_notification = timeout(Duration::from_secs(1), first_listener.recv())
        .await
        .unwrap()
        .unwrap();
    let second_notification = timeout(Duration::from_secs(1), second_listener.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_notification.payload(), first.to_string());
    assert_eq!(second_notification.payload(), second.to_string());
    assert!(first_notification.channel().starts_with("pgtask_result_"));
    assert!(second_notification.channel().starts_with("pgtask_result_"));
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
    let completed_child_id = store
        .spawn_task(SpawnRequest {
            parent_task_id: parent_id,
            parent_attempt: parent.attempt,
            parent_lease_token: parent.lease_token.unwrap(),
            step_name: &StepName::new("spawn-child").unwrap(),
            occurrence: 0,
            task: &child_request,
        })
        .await
        .unwrap()
        .unwrap()
        .task_id;
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
    assert_eq!(completed_child.parent_task_id, Some(parent_id));
    store
        .complete(
            completed_child.id,
            completed_child.attempt,
            completed_child.lease_token.unwrap(),
            Some(&json!({"child": "complete"})),
        )
        .await
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
                timeout: None,
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

    let waiting_parent_id = store.enqueue(&parent_request).await.unwrap().task_id;
    let waiting_parent = claim_one(&store, &parent_queue, &parent_name).await;
    assert_eq!(waiting_parent.id, waiting_parent_id);
    let waiting_child_id = spawn_child(&store, &waiting_parent, "spawn-child", &child_request).await;
    assert_eq!(
        store
            .wait_for_result(ResultWaitRequest {
                task_id: waiting_parent_id,
                attempt: waiting_parent.attempt,
                lease_token: waiting_parent.lease_token.unwrap(),
                step_name: &step_name,
                occurrence: 0,
                result_task_id: waiting_child_id,
                timeout: None,
            })
            .await
            .unwrap(),
        Some(ResultWait::Waiting)
    );
    let waiting_child = claim_one(&store, &child_queue, &child_name).await;
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
    let resumed_parent = claim_one(&store, &parent_queue, &parent_name).await;
    assert_eq!(
        store
            .wait_for_result(ResultWaitRequest {
                task_id: waiting_parent_id,
                attempt: resumed_parent.attempt,
                lease_token: resumed_parent.lease_token.unwrap(),
                step_name: &step_name,
                occurrence: 0,
                result_task_id: waiting_child_id,
                timeout: None,
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
    let application_name = format!("result-listener-{}", Uuid::new_v4().simple());
    let options = PgConnectOptions::from_str(&database_url)
        .unwrap()
        .application_name(&application_name);
    let store = Store::from_pool(PgPoolOptions::new().connect_with(options).await.unwrap());
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
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let listening: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name = $1 AND query LIKE 'LISTEN%')",
            )
            .bind(&application_name)
            .fetch_one(store.pool())
            .await
            .unwrap();
            if listening {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    sqlx::query("SELECT pg_notify('pgtask_result', $1)")
        .bind(TaskId::new().to_string())
        .execute(store.pool())
        .await
        .unwrap();
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
async fn external_result_wait_returns_an_already_completed_task() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("completed-result-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("completed-result-handler-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let task = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(task_name, HandlerVersion::default())],
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
            Some(&json!({"ready": true})),
        )
        .await
        .unwrap();

    assert!(matches!(
        store.wait_for_task_result(task_id, None).await.unwrap(),
        TaskResultWait::Ready(result) if result.result == Some(json!({"ready": true}))
    ));
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
    sqlx::query("SELECT pg_notify(pgtask.result_channel($1), $1::text)")
        .bind(disappeared_id.as_uuid())
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

#[tokio::test]
async fn result_wait_rejects_tasks_outside_the_owned_workflow() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("result-ownership-{suffix}")).unwrap();
    let parent_name = TaskName::new(format!("result-parent-{suffix}")).unwrap();
    let unrelated_name = TaskName::new(format!("result-unrelated-{suffix}")).unwrap();
    let mut parent_request = EnqueueRequest::new(parent_name.clone(), json!({}));
    parent_request.queue_name = queue_name.clone();
    let mut unrelated_request = EnqueueRequest::new(unrelated_name, json!({}));
    unrelated_request.queue_name = queue_name.clone();
    let parent_id = store.enqueue(&parent_request).await.unwrap().task_id;
    let unrelated_id = store.enqueue(&unrelated_request).await.unwrap().task_id;
    let parent = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(parent_name, HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

    let error = store
        .wait_for_result(ResultWaitRequest {
            task_id: parent_id,
            attempt: parent.attempt,
            lease_token: parent.lease_token.unwrap(),
            step_name: &StepName::new("unrelated").unwrap(),
            occurrence: 0,
            result_task_id: unrelated_id,
            timeout: None,
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not a direct child"));
    assert!(matches!(
        store
            .wait_for_result(ResultWaitRequest {
                task_id: parent_id,
                attempt: parent.attempt,
                lease_token: parent.lease_token.unwrap(),
                step_name: &StepName::new("zero-timeout").unwrap(),
                occurrence: 0,
                result_task_id: unrelated_id,
                timeout: Some(Duration::ZERO),
            })
            .await,
        Err(PostgresError::InvalidResultWaitTimeout)
    ));
}

#[tokio::test]
async fn result_wait_timeout_wakes_the_parent_and_cancels_the_child_tree() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("result-timeout-{suffix}")).unwrap();
    let parent_name = TaskName::new(format!("timeout-parent-{suffix}")).unwrap();
    let child_name = TaskName::new(format!("timeout-child-{suffix}")).unwrap();
    let grandchild_name = TaskName::new(format!("timeout-grandchild-{suffix}")).unwrap();
    let mut parent_request = EnqueueRequest::new(parent_name.clone(), json!({}));
    parent_request.queue_name = queue_name.clone();
    let mut child_request = EnqueueRequest::new(child_name.clone(), json!({}));
    child_request.queue_name = queue_name.clone();
    let mut grandchild_request = EnqueueRequest::new(grandchild_name.clone(), json!({}));
    grandchild_request.queue_name = queue_name.clone();

    let parent_id = store.enqueue(&parent_request).await.unwrap().task_id;
    let parent = claim_one(&store, &queue_name, &parent_name).await;
    let child_id = spawn_child(&store, &parent, "spawn-child", &child_request).await;
    let child = claim_one(&store, &queue_name, &child_name).await;
    let grandchild_id = spawn_child(&store, &child, "spawn-grandchild", &grandchild_request).await;
    let grandchild = claim_one(&store, &queue_name, &grandchild_name).await;

    assert_eq!(
        store
            .wait_for_result(ResultWaitRequest {
                task_id: parent_id,
                attempt: parent.attempt,
                lease_token: parent.lease_token.unwrap(),
                step_name: &StepName::new("wait-child").unwrap(),
                occurrence: 0,
                result_task_id: child_id,
                timeout: Some(Duration::from_millis(1)),
            })
            .await
            .unwrap(),
        Some(ResultWait::Waiting)
    );
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(store.recover_result_wait_timeouts(10).await.unwrap(), 1);
    assert_eq!(
        store.get_task(parent_id).await.unwrap().unwrap().state,
        TaskState::Pending
    );
    assert_eq!(
        store.get_task(child_id).await.unwrap().unwrap().state,
        TaskState::Cancelled
    );
    assert_eq!(
        store.get_task(grandchild_id).await.unwrap().unwrap().state,
        TaskState::Cancelled
    );
    let cancelled_attempts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pgtask.attempt_view WHERE task_id = ANY($1) AND state = 'cancelled'")
            .bind([child_id.as_uuid(), grandchild.id.as_uuid()])
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(cancelled_attempts, 2);

    let resumed_parent = claim_one(&store, &queue_name, &parent_name).await;
    assert_eq!(
        store
            .wait_for_result(ResultWaitRequest {
                task_id: parent_id,
                attempt: resumed_parent.attempt,
                lease_token: resumed_parent.lease_token.unwrap(),
                step_name: &StepName::new("wait-child").unwrap(),
                occurrence: 0,
                result_task_id: child_id,
                timeout: Some(Duration::from_secs(1)),
            })
            .await
            .unwrap(),
        Some(ResultWait::Ready(
            json!({"state": "timeout", "result": null, "error": null})
        ))
    );
}

#[tokio::test]
async fn terminal_parent_cancels_descendants_and_retention_deletes_leaves_first() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("result-retention-{suffix}")).unwrap();
    let mut queue = QueueConfig::new(queue_name.clone());
    queue.terminal_retention = Duration::ZERO;
    store.put_queue(&queue).await.unwrap();
    let parent_name = TaskName::new(format!("retention-parent-{suffix}")).unwrap();
    let child_name = TaskName::new(format!("retention-child-{suffix}")).unwrap();
    let mut parent_request = EnqueueRequest::new(parent_name.clone(), json!({}));
    parent_request.queue_name = queue_name.clone();
    let mut child_request = EnqueueRequest::new(child_name.clone(), json!({}));
    child_request.queue_name = queue_name.clone();
    let parent_id = store.enqueue(&parent_request).await.unwrap().task_id;
    let parent = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(parent_name, HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let child_id = store
        .spawn_task(SpawnRequest {
            parent_task_id: parent_id,
            parent_attempt: parent.attempt,
            parent_lease_token: parent.lease_token.unwrap(),
            step_name: &StepName::new("spawn-child").unwrap(),
            occurrence: 0,
            task: &child_request,
        })
        .await
        .unwrap()
        .unwrap()
        .task_id;
    let child = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(child_name, HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();

    assert!(
        store
            .complete(parent.id, parent.attempt, parent.lease_token.unwrap(), None)
            .await
            .unwrap()
    );
    assert_eq!(
        store.get_task(child_id).await.unwrap().unwrap().state,
        TaskState::Cancelled
    );
    let child_attempt_state: String =
        sqlx::query_scalar("SELECT state FROM pgtask.attempt_view WHERE task_id = $1 AND attempt = $2")
            .bind(child_id.as_uuid())
            .bind(i32::from(child.attempt))
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(child_attempt_state, "cancelled");

    assert_eq!(store.delete_expired_terminal(&queue_name, 10).await.unwrap(), 1);
    assert!(store.get_task(child_id).await.unwrap().is_none());
    assert!(store.get_task(parent_id).await.unwrap().is_some());
    assert_eq!(store.delete_expired_terminal(&queue_name, 10).await.unwrap(), 1);
    assert!(store.get_task(parent_id).await.unwrap().is_none());
}
