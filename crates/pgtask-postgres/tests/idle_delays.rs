//! The `next_*_delay` functions tell an idle worker how long it may sleep. They report `None` when no
//! deadline exists, a positive delay for a future deadline, and zero once a deadline has passed.
//!
//! The functions read every row in the database, so each test runs against its own database.

use std::{str::FromStr, time::Duration};

use chrono::{TimeDelta, Utc};
use pgtask_core::{
    EnqueueRequest, HandlerVersion, QueueName, ScheduleConfig, ScheduleDefinition, ScheduleName, SignalName, StepName,
    TaskName, WorkerId,
};
use pgtask_postgres::{SignalWaitRequest, Store};
use serde_json::json;
use sqlx::{PgPool, postgres::PgConnectOptions};
use uuid::Uuid;

const FUTURE: Duration = Duration::from_hours(1);

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

async fn isolated_store(database_url: &str) -> (Store, PgPool, String) {
    let database_name = format!("pgtask_idle_delays_{}", Uuid::new_v4().simple());
    let options = PgConnectOptions::from_str(database_url).unwrap();
    let maintenance = PgPool::connect_with(options.clone().database("postgres"))
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database_name}")))
        .execute(&maintenance)
        .await
        .unwrap();
    let store = Store::from_pool(PgPool::connect_with(options.database(&database_name)).await.unwrap());
    store.migrate().await.unwrap();
    (store, maintenance, database_name)
}

async fn drop_isolated_store(store: Store, maintenance: &PgPool, database_name: &str) {
    drop(store);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE {database_name} WITH (FORCE)"
    )))
    .execute(maintenance)
    .await
    .unwrap();
}

fn assert_future_delay(delay: Option<Duration>) {
    let delay = delay.expect("a future deadline reports a delay");
    assert!(
        delay > Duration::ZERO && delay <= FUTURE,
        "expected a delay within {FUTURE:?}, got {delay:?}"
    );
}

#[tokio::test]
async fn schedule_delay_is_absent_until_an_active_schedule_exists() {
    let Some(database_url) = database_url() else {
        return;
    };
    let (store, maintenance, database_name) = isolated_store(&database_url).await;
    assert_eq!(store.next_schedule_delay().await.unwrap(), None);

    let task = EnqueueRequest::new(TaskName::new("idle-schedule-task").unwrap(), json!({}));
    let definition = ScheduleDefinition::interval(Duration::from_mins(1)).unwrap();
    let mut future = ScheduleConfig::new(ScheduleName::new("future").unwrap(), definition.clone(), task.clone());
    future.start_at = Some(Utc::now() + TimeDelta::from_std(FUTURE).unwrap());
    let future = store.put_schedule(&future).await.unwrap();
    assert_future_delay(store.next_schedule_delay().await.unwrap());

    store.set_schedule_paused(future.config.id, true).await.unwrap();
    assert_eq!(store.next_schedule_delay().await.unwrap(), None);

    let mut overdue = ScheduleConfig::new(ScheduleName::new("overdue").unwrap(), definition, task);
    overdue.start_at = Some(Utc::now() - TimeDelta::minutes(1));
    store.put_schedule(&overdue).await.unwrap();
    assert_eq!(store.next_schedule_delay().await.unwrap(), Some(Duration::ZERO));

    drop_isolated_store(store, &maintenance, &database_name).await;
}

#[tokio::test]
async fn task_delay_is_absent_until_a_pending_task_exists() {
    let Some(database_url) = database_url() else {
        return;
    };
    let (store, maintenance, database_name) = isolated_store(&database_url).await;
    let queue_name = QueueName::new("idle-task-queue").unwrap();
    let task_name = TaskName::new("idle-task").unwrap();
    let capabilities = [(task_name.clone(), HandlerVersion::default())];
    assert_eq!(store.next_task_delay(&queue_name, &capabilities).await.unwrap(), None);

    let mut future = EnqueueRequest::new(task_name.clone(), json!({}));
    future.queue_name = queue_name.clone();
    future.run_at = Some(Utc::now() + TimeDelta::from_std(FUTURE).unwrap());
    store.enqueue(&future).await.unwrap();
    assert_future_delay(store.next_task_delay(&queue_name, &capabilities).await.unwrap());

    let other_name = TaskName::new("idle-task-without-a-handler").unwrap();
    let other = [(other_name, HandlerVersion::default())];
    assert_eq!(store.next_task_delay(&queue_name, &other).await.unwrap(), None);

    let mut due = EnqueueRequest::new(task_name, json!({}));
    due.queue_name = queue_name.clone();
    store.enqueue(&due).await.unwrap();
    assert_eq!(
        store.next_task_delay(&queue_name, &capabilities).await.unwrap(),
        Some(Duration::ZERO)
    );

    drop_isolated_store(store, &maintenance, &database_name).await;
}

#[tokio::test]
async fn wait_delay_is_absent_until_a_wait_has_a_timeout() {
    let Some(database_url) = database_url() else {
        return;
    };
    let (store, maintenance, database_name) = isolated_store(&database_url).await;
    assert_eq!(store.next_wait_delay().await.unwrap(), None);

    let queue_name = QueueName::new("idle-wait-queue").unwrap();
    let task_name = TaskName::new("idle-wait-task").unwrap();
    let signal_name = SignalName::new("approval").unwrap();
    let step_name = StepName::new("wait-for-approval").unwrap();
    let wait = async |timeout: Option<Duration>| {
        let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
        request.queue_name = queue_name.clone();
        store.enqueue(&request).await.unwrap();
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
            .wait_for_signal(SignalWaitRequest {
                task_id: task.id,
                attempt: task.attempt,
                lease_token: task.lease_token.unwrap(),
                step_name: &step_name,
                occurrence: 0,
                signal_name: &signal_name,
                signal_occurrence: 0,
                timeout,
            })
            .await
            .unwrap();
    };

    wait(None).await;
    assert_eq!(store.next_wait_delay().await.unwrap(), None);

    wait(Some(FUTURE)).await;
    assert_future_delay(store.next_wait_delay().await.unwrap());

    wait(Some(Duration::from_millis(1))).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(store.next_wait_delay().await.unwrap(), Some(Duration::ZERO));

    drop_isolated_store(store, &maintenance, &database_name).await;
}
