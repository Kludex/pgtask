use std::time::Duration;

use pgtask_core::{EnqueueRequest, HandlerVersion, QueueName, SignalName, StepName, TaskName, TaskState, WorkerId};
use pgtask_postgres::{SignalWait, SignalWaitRequest, Store};
use serde_json::json;
use uuid::Uuid;

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

#[tokio::test]
async fn signals_are_immutable_and_close_both_wait_registration_races() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("signal-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("signal-task-{suffix}")).unwrap();
    let signal_name = SignalName::new("approval").unwrap();
    let step_name = StepName::new("wait-for-approval").unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;

    let first = store
        .emit_signal(task_id, &signal_name, 0, &json!({"approved": true}))
        .await
        .unwrap();
    let replayed = store
        .emit_signal(task_id, &signal_name, 0, &json!({"approved": false}))
        .await
        .unwrap();
    assert_eq!(replayed.value, first.value);
    let running = store
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
    let ready = store
        .wait_for_signal(SignalWaitRequest {
            task_id,
            attempt: running.attempt,
            lease_token: running.lease_token.unwrap(),
            step_name: &step_name,
            occurrence: 0,
            signal_name: &signal_name,
            signal_occurrence: 0,
            timeout: None,
        })
        .await
        .unwrap();
    assert_eq!(
        ready,
        Some(SignalWait::Ready(
            json!({"outcome": "signal", "value": {"approved": true}})
        ))
    );

    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let waiting_id = store.enqueue(&request).await.unwrap().task_id;
    let waiting = store
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
    assert_eq!(
        store
            .wait_for_signal(SignalWaitRequest {
                task_id: waiting_id,
                attempt: waiting.attempt,
                lease_token: waiting.lease_token.unwrap(),
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
    assert_eq!(
        store.get_task(waiting_id).await.unwrap().unwrap().state,
        TaskState::Waiting
    );
    store
        .emit_signal(waiting_id, &signal_name, 0, &json!({"approved": true}))
        .await
        .unwrap();
    assert_eq!(
        store.get_task(waiting_id).await.unwrap().unwrap().state,
        TaskState::Pending
    );
}

#[tokio::test]
async fn signal_wait_timeout_resumes_with_a_durable_timeout_checkpoint() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("signal-timeout-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("signal-timeout-task-{suffix}")).unwrap();
    let signal_name = SignalName::new("never-emitted").unwrap();
    let step_name = StepName::new("wait-with-timeout").unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let running = store
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
    assert_eq!(
        store
            .wait_for_signal(SignalWaitRequest {
                task_id,
                attempt: running.attempt,
                lease_token: running.lease_token.unwrap(),
                step_name: &step_name,
                occurrence: 0,
                signal_name: &signal_name,
                signal_occurrence: 0,
                timeout: Some(Duration::from_millis(1)),
            })
            .await
            .unwrap(),
        Some(SignalWait::Waiting)
    );
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(store.recover_wait_timeouts(10).await.unwrap(), 1);
    let resumed = store
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
    assert_eq!(resumed.attempt, 2);
    assert_eq!(
        store
            .wait_for_signal(SignalWaitRequest {
                task_id,
                attempt: resumed.attempt,
                lease_token: resumed.lease_token.unwrap(),
                step_name: &step_name,
                occurrence: 0,
                signal_name: &signal_name,
                signal_occurrence: 0,
                timeout: Some(Duration::from_millis(1)),
            })
            .await
            .unwrap(),
        Some(SignalWait::Ready(json!({"outcome": "timeout"})))
    );
}
