use std::{str::FromStr, time::Duration};

use chrono::{TimeDelta, Utc};
use pgtask_core::{
    EnqueueRequest, LeaseToken, ScheduleConfig, ScheduleDefinition, ScheduleName, SignalName, StepName, TaskId,
    TaskName,
};
use pgtask_postgres::{PostgresError, ResultWaitRequest, SignalWaitRequest, Store};
use serde_json::json;
use sqlx::{PgPool, postgres::PgConnectOptions};
use uuid::Uuid;

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

async fn isolated_store(database_url: &str) -> (Store, PgPool, String) {
    let database_name = format!("pgtask_corruption_{}", Uuid::new_v4().simple());
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

#[tokio::test]
async fn rejects_corrupted_wait_protocol_rows() {
    let Some(database_url) = database_url() else {
        return;
    };
    let (store, maintenance, database_name) = isolated_store(&database_url).await;

    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION pgtask.wait_for_signal(
            p_task_id uuid,
            p_attempt integer,
            p_lease_token uuid,
            p_step_name text,
            p_occurrence integer,
            p_signal_name text,
            p_signal_occurrence integer,
            p_timeout_milliseconds bigint
        )
        RETURNS TABLE(status text, checkpoint jsonb)
        LANGUAGE sql
        AS $$ SELECT 'invalid'::text, NULL::jsonb $$
        ",
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        store
            .wait_for_signal(SignalWaitRequest {
                task_id: TaskId::new(),
                attempt: 1,
                lease_token: LeaseToken::new(),
                step_name: &StepName::new("corrupt-signal").unwrap(),
                occurrence: 0,
                signal_name: &SignalName::new("signal").unwrap(),
                signal_occurrence: 0,
                timeout: None,
            })
            .await,
        Err(PostgresError::InvalidTask(message)) if message.contains("signal wait")
    ));

    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION pgtask.wait_for_result(
            p_task_id uuid,
            p_attempt integer,
            p_lease_token uuid,
            p_step_name text,
            p_occurrence integer,
            p_result_task_id uuid,
            p_timeout_milliseconds bigint
        )
        RETURNS TABLE(status text, checkpoint jsonb)
        LANGUAGE sql
        AS $$ SELECT 'invalid'::text, NULL::jsonb $$
        ",
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        store
            .wait_for_result(ResultWaitRequest {
                task_id: TaskId::new(),
                attempt: 1,
                lease_token: LeaseToken::new(),
                step_name: &StepName::new("corrupt-result").unwrap(),
                occurrence: 0,
                result_task_id: TaskId::new(),
                timeout: None,
            })
            .await,
        Err(PostgresError::InvalidTask(message)) if message.contains("result wait")
    ));

    drop_isolated_store(store, &maintenance, &database_name).await;
}

#[tokio::test]
async fn rejects_corrupted_storage_protocol_ranges() {
    let Some(database_url) = database_url() else {
        return;
    };
    let (store, maintenance, database_name) = isolated_store(&database_url).await;

    sqlx::query(
        "CREATE OR REPLACE FUNCTION pgtask.storage_protocol_range() \
         RETURNS TABLE(minimum integer, maximum integer) LANGUAGE sql AS $$ SELECT -1, 1 $$",
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        store.storage_protocol_range().await,
        Err(PostgresError::InvalidStorageProtocolRange {
            minimum: -1,
            maximum: 1
        })
    ));

    sqlx::query(
        "CREATE OR REPLACE FUNCTION pgtask.storage_protocol_range() \
         RETURNS TABLE(minimum integer, maximum integer) LANGUAGE sql AS $$ SELECT 1, -1 $$",
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        store.storage_protocol_range().await,
        Err(PostgresError::InvalidStorageProtocolRange {
            minimum: 1,
            maximum: -1
        })
    ));

    drop_isolated_store(store, &maintenance, &database_name).await;
}

#[tokio::test]
async fn rejects_corrupted_schedule_protocol_rows() {
    let Some(database_url) = database_url() else {
        return;
    };
    let (store, maintenance, database_name) = isolated_store(&database_url).await;

    let suffix = Uuid::new_v4();
    let mut schedule = ScheduleConfig::new(
        ScheduleName::new(format!("corrupt-{suffix}")).unwrap(),
        ScheduleDefinition::interval(Duration::from_secs(1)).unwrap(),
        EnqueueRequest::new(TaskName::new(format!("corrupt-task-{suffix}")).unwrap(), json!({})),
    );
    schedule.start_at = Some(Utc::now() + TimeDelta::hours(1));
    let schedule_id = store.put_schedule(&schedule).await.unwrap().config.id;
    sqlx::query("ALTER TABLE pgtask.schedules DROP CONSTRAINT schedules_kind_check, DROP CONSTRAINT schedules_check")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE pgtask.schedules SET kind = 'invalid' WHERE id = $1")
        .bind(schedule_id.as_uuid())
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.get_schedule(schedule_id).await,
        Err(PostgresError::InvalidTask(message)) if message.contains("schedule definition")
    ));

    sqlx::query(
        "ALTER TABLE pgtask.schedules DROP CONSTRAINT schedules_misfire_policy_check, \
         DROP CONSTRAINT schedules_check1",
    )
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE pgtask.schedules SET kind = 'interval', misfire_policy = 'invalid', catch_up_limit = NULL \
         WHERE id = $1",
    )
    .bind(schedule_id.as_uuid())
    .execute(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        store.get_schedule(schedule_id).await,
        Err(PostgresError::InvalidTask(message)) if message.contains("misfire policy")
    ));

    sqlx::query("ALTER TABLE pgtask.schedules DROP CONSTRAINT schedules_interval_milliseconds_check")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE pgtask.schedules SET misfire_policy = 'latest', interval_milliseconds = -1 WHERE id = $1")
        .bind(schedule_id.as_uuid())
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.get_schedule(schedule_id).await,
        Err(PostgresError::InvalidTask(_))
    ));
    sqlx::query("UPDATE pgtask.schedules SET interval_milliseconds = 0 WHERE id = $1")
        .bind(schedule_id.as_uuid())
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.get_schedule(schedule_id).await,
        Err(PostgresError::Schedule(_))
    ));

    drop_isolated_store(store, &maintenance, &database_name).await;
}

#[tokio::test]
async fn rejects_corrupted_task_state() {
    let Some(database_url) = database_url() else {
        return;
    };
    let (store, maintenance, database_name) = isolated_store(&database_url).await;

    let suffix = Uuid::new_v4();
    let task_id = store
        .enqueue(&EnqueueRequest::new(
            TaskName::new(format!("corrupt-state-{suffix}")).unwrap(),
            json!({}),
        ))
        .await
        .unwrap()
        .task_id;
    sqlx::query("ALTER TABLE pgtask.tasks DROP CONSTRAINT tasks_state_check")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE pgtask.tasks SET state = 'invalid' WHERE id = $1")
        .bind(task_id.as_uuid())
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.task_result(task_id).await,
        Err(PostgresError::InvalidTask(message)) if message.contains("unknown state")
    ));

    drop_isolated_store(store, &maintenance, &database_name).await;
}
