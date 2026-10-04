//! A schedule that can never be materialized must not make workers spin or starve other schedules.

use std::{str::FromStr, time::Duration};

use chrono::{TimeDelta, Utc};
use pgtask_core::{
    EnqueueRequest, HandlerVersion, QueueName, RetryPolicy, ScheduleConfig, ScheduleDefinition, ScheduleName,
    SignalName, StepName, TaskName, WorkerId,
};
use pgtask_postgres::{SignalWait, SignalWaitRequest, Store};
use pgtask_worker::{HandlerRegistry, Worker, WorkerConfig};
use serde_json::json;
use sqlx::{PgPool, postgres::PgConnectOptions};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const QUEUE: &str = "schedules";
const OBSERVED: Duration = Duration::from_millis(3_500);

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

/// A database of its own, with `claim_due_schedules` counting its calls in a sequence. A sequence
/// keeps counting when the sweep's transaction rolls back, which is what a failing sweep does.
async fn counting_store(database_url: &str) -> (Store, PgPool, String) {
    let database_name = format!("pgtask_schedule_spin_{}", Uuid::new_v4().simple());
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
    for statement in [
        "CREATE SEQUENCE public.schedule_claims",
        "ALTER FUNCTION pgtask.claim_due_schedules(integer) RENAME TO claim_due_schedules_uncounted",
        r"
        CREATE FUNCTION pgtask.claim_due_schedules(p_limit integer)
        RETURNS SETOF pgtask.schedules
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, pgtask
        AS $$
        BEGIN
            PERFORM nextval('public.schedule_claims');
            RETURN QUERY SELECT * FROM pgtask.claim_due_schedules_uncounted(p_limit);
        END;
        $$
        ",
    ] {
        sqlx::query(statement).execute(store.pool()).await.unwrap();
    }
    (store, maintenance, database_name)
}

/// Parks a task on a signal wait an hour away, so the wait deadline never asks the loop to run.
async fn park_a_waiting_task(store: &Store, queue_name: &QueueName) {
    let task_name = TaskName::new("parked").unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let task = store
        .claim(
            queue_name,
            WorkerId::new(),
            &[(task_name, HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let waiting = store
        .wait_for_signal(SignalWaitRequest {
            task_id,
            attempt: task.attempt,
            lease_token: task.lease_token.unwrap(),
            step_name: &StepName::new("park").unwrap(),
            occurrence: 0,
            signal_name: &SignalName::new("never").unwrap(),
            signal_occurrence: 0,
            timeout: Some(Duration::from_hours(1)),
        })
        .await
        .unwrap();
    assert_eq!(waiting, Some(SignalWait::Waiting));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unmaterializable_schedule_neither_spins_workers_nor_blocks_other_schedules() {
    let Some(database_url) = database_url() else {
        return;
    };
    let (store, maintenance, database_name) = counting_store(&database_url).await;
    let queue_name = QueueName::new(QUEUE).unwrap();
    park_a_waiting_task(&store, &queue_name).await;

    // February 30 never comes. It sorts first, so it is in every claimed batch.
    sqlx::query(
        r"
        SELECT pgtask.put_schedule(
            gen_random_uuid(), 'february-30', 'cron', NULL, '0 0 0 30 2 *', 'latest', NULL,
            $1, 'scheduled', 1, '{}'::jsonb, '{}'::jsonb, 0::smallint, 5,
            statement_timestamp() - interval '1 minute'
        )
        ",
    )
    .bind(QUEUE)
    .execute(store.pool())
    .await
    .unwrap();
    let mut healthy = ScheduleConfig::new(
        ScheduleName::new("every-second").unwrap(),
        ScheduleDefinition::interval(Duration::from_secs(1)).unwrap(),
        EnqueueRequest::new(TaskName::new("scheduled").unwrap(), json!({})),
    );
    healthy.task.queue_name = queue_name.clone();
    healthy.start_at = Some(Utc::now() - TimeDelta::milliseconds(10));
    let healthy = store.put_schedule(&healthy).await.unwrap();

    let mut registry = HandlerRegistry::new();
    registry.register(
        TaskName::new("scheduled").unwrap(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { Ok(json!(null)) },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.poll_interval = Duration::from_secs(30);
    config.retention_enabled = false;
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    tokio::time::sleep(OBSERVED).await;
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();

    let claims: i64 = sqlx::query_scalar("SELECT last_value FROM public.schedule_claims")
        .fetch_one(store.pool())
        .await
        .unwrap();
    let materialized: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtask.tasks WHERE schedule_id = $1")
        .bind(healthy.config.id.as_uuid())
        .fetch_one(store.pool())
        .await
        .unwrap();
    println!("{claims} schedule sweeps and {materialized} healthy occurrences in {OBSERVED:?}");
    // One sweep per healthy occurrence, plus slack for wake-ups. Before the fix: thousands.
    assert!(claims <= 20, "{claims} schedule sweeps in {OBSERVED:?}");
    // Occurrences at roughly 0 s, 1 s, 2 s and 3 s.
    assert!(
        materialized >= 3,
        "the healthy schedule materialized {materialized} times"
    );

    store.pool().close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE {database_name} WITH (FORCE)"
    )))
    .execute(&maintenance)
    .await
    .unwrap();
}
