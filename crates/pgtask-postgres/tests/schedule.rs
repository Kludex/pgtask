use std::{
    num::{NonZeroU16, NonZeroU64},
    sync::OnceLock,
    time::Duration,
};

use chrono::{DateTime, TimeDelta, Utc};
use pgtask_core::{
    EnqueueRequest, HandlerVersion, MisfirePolicy, QueueConfig, QueueName, ScheduleConfig, ScheduleDefinition,
    ScheduleError, ScheduleId, ScheduleName, Task, TaskName, WorkerId,
};
use pgtask_postgres::{PostgresError, Store};
use serde_json::json;
use uuid::Uuid;

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

async fn schedule_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static GUARD: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    GUARD.get_or_init(|| tokio::sync::Mutex::new(())).lock().await
}

/// Drains the schedule sweep until this schedule's own task is claimable,
/// instead of assuming a fixed number of calls is enough.
///
/// `pgtask.claim_due_schedules` (which backs `materialize_due_schedules`) has
/// no `queue_name` filter: it orders every due schedule in the database and
/// takes the first `p_limit`. In a suite where several files share one
/// database, enough of *their* due schedules can starve this one's out of
/// every fixed-size batch, which is exactly what made this test flake -- see
/// #39. A large batch and a bounded retry loop drain whatever backlog exists
/// rather than gambling that two calls of ten were always going to be enough.
async fn claim_after_materializing(
    store: &Store,
    queue_name: &QueueName,
    task_name: &TaskName,
    limit: u16,
) -> Vec<Task> {
    for _ in 0..20 {
        let claimed = store
            .claim(
                queue_name,
                WorkerId::new(),
                &[(task_name.clone(), HandlerVersion::default())],
                limit,
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        if !claimed.is_empty() {
            return claimed;
        }
        store.materialize_due_schedules(1_000).await.unwrap();
    }
    Vec::new()
}

async fn materialize_two_intervals(store: &Store, schedule_id: ScheduleId, expected: DateTime<Utc>) -> i64 {
    sqlx::query_scalar("SELECT pgtask.materialize_schedule($1, $2, $3, $4)")
        .bind(schedule_id.as_uuid())
        .bind(expected)
        .bind([expected, expected + TimeDelta::seconds(10)])
        .bind(expected + TimeDelta::seconds(20))
        .fetch_one(store.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn interval_schedule_reconciles_materializes_and_supports_dynamic_crud() {
    let _guard = schedule_test_guard().await;
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let task_name = TaskName::new(format!("scheduled-task-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({"source": "interval"}));
    request.priority = 7;
    let mut config = ScheduleConfig::new(
        ScheduleName::new(format!("interval-{suffix}")).unwrap(),
        ScheduleDefinition::interval(Duration::from_secs(10)).unwrap(),
        request,
    );
    config.misfire_policy = MisfirePolicy::CatchUp {
        limit: NonZeroU16::new(2).unwrap(),
    };
    config.start_at = Some(Utc::now() + TimeDelta::milliseconds(100));

    let created = store.put_schedule(&config).await.unwrap();
    let reconciled = store.put_schedule(&config).await.unwrap();
    assert_eq!(reconciled.config.id, created.config.id);
    assert_eq!(reconciled.updated_at, created.updated_at);
    let paused = store
        .set_schedule_paused(created.config.id, true)
        .await
        .unwrap()
        .unwrap();
    assert!(paused.paused_at.is_some());
    tokio::time::sleep(Duration::from_millis(150)).await;
    store.materialize_due_schedules(10).await.unwrap();
    let paused_claim = store
        .claim(
            &config.task.queue_name,
            WorkerId::new(),
            &[(task_name.clone(), HandlerVersion::default())],
            10,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    assert!(paused_claim.is_empty());
    let resumed = store
        .set_schedule_paused(created.config.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(resumed.paused_at.is_none());

    let claimed = claim_after_materializing(&store, &config.task.queue_name, &task_name, 10).await;
    assert_eq!(claimed.len(), 1);
    assert!(claimed.iter().all(|task| task.priority == 7));
    assert!(store.get_schedule(created.config.id).await.unwrap().is_some());
    assert!(store.delete_schedule(created.config.id).await.unwrap());
    assert!(store.get_schedule(created.config.id).await.unwrap().is_none());
}

#[tokio::test]
async fn skip_misfire_policy_round_trips() {
    let _guard = schedule_test_guard().await;
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let request = EnqueueRequest::new(TaskName::new(format!("skip-task-{suffix}")).unwrap(), json!({}));
    let mut config = ScheduleConfig::new(
        ScheduleName::new(format!("skip-{suffix}")).unwrap(),
        ScheduleDefinition::interval(Duration::from_secs(10)).unwrap(),
        request,
    );
    config.misfire_policy = MisfirePolicy::Skip;
    config.start_at = Some(Utc::now() + TimeDelta::hours(1));

    let schedule = store.put_schedule(&config).await.unwrap();
    assert_eq!(schedule.config.misfire_policy, MisfirePolicy::Skip);
    assert!(store.delete_schedule(schedule.config.id).await.unwrap());
}

#[tokio::test]
async fn schedule_backpressure_preserves_occurrences_at_queue_capacity() {
    let _guard = schedule_test_guard().await;
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("schedule-capacity-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("schedule-capacity-{suffix}")).unwrap();
    let mut queue = QueueConfig::new(queue_name.clone());
    queue.max_outstanding_tasks = NonZeroU64::new(1);
    store.put_queue(&queue).await.unwrap();

    let mut blocker = EnqueueRequest::new(task_name.clone(), json!({"blocker": true}));
    blocker.queue_name = queue_name.clone();
    let blocker_id = store.enqueue(&blocker).await.unwrap().task_id;
    let mut scheduled_request = EnqueueRequest::new(task_name.clone(), json!({"scheduled": true}));
    scheduled_request.queue_name = queue_name.clone();
    let mut config = ScheduleConfig::new(
        ScheduleName::new(format!("schedule-capacity-{suffix}")).unwrap(),
        ScheduleDefinition::interval(Duration::from_secs(10)).unwrap(),
        scheduled_request,
    );
    config.start_at = Some(Utc::now() - TimeDelta::seconds(20));
    config.misfire_policy = MisfirePolicy::CatchUp {
        limit: NonZeroU16::new(2).unwrap(),
    };
    let schedule = store.put_schedule(&config).await.unwrap();

    assert_eq!(
        materialize_two_intervals(&store, schedule.config.id, schedule.next_run_at).await,
        0
    );
    assert_eq!(
        store
            .get_schedule(schedule.config.id)
            .await
            .unwrap()
            .unwrap()
            .next_run_at,
        schedule.next_run_at
    );
    assert!(store.cancel(blocker_id).await.unwrap());
    assert_eq!(
        materialize_two_intervals(&store, schedule.config.id, schedule.next_run_at).await,
        1
    );
    let deferred = store.get_schedule(schedule.config.id).await.unwrap().unwrap();
    assert_eq!(deferred.next_run_at, schedule.next_run_at + TimeDelta::seconds(10));
    assert_eq!(
        materialize_two_intervals(&store, schedule.config.id, deferred.next_run_at).await,
        0
    );
    assert_eq!(
        store
            .get_schedule(schedule.config.id)
            .await
            .unwrap()
            .unwrap()
            .next_run_at,
        deferred.next_run_at
    );

    let claimed = store
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
    assert!(store.cancel(claimed.id).await.unwrap());
    assert_eq!(
        materialize_two_intervals(&store, schedule.config.id, deferred.next_run_at).await,
        1
    );
    assert!(store.delete_schedule(schedule.config.id).await.unwrap());
}

#[tokio::test]
async fn concurrent_schedulers_materialize_one_cron_occurrence() {
    let _guard = schedule_test_guard().await;
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let task_name = TaskName::new(format!("cron-task-{suffix}")).unwrap();
    let request = EnqueueRequest::new(task_name.clone(), json!({"source": "cron"}));
    let mut config = ScheduleConfig::new(
        ScheduleName::new(format!("cron-{suffix}")).unwrap(),
        ScheduleDefinition::cron("0 0 0 * * *").unwrap(),
        request,
    );
    config.start_at = Some(Utc::now().date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc());
    let schedule = store.put_schedule(&config).await.unwrap();

    let (left, right) = tokio::join!(store.materialize_due_schedules(10), store.materialize_due_schedules(10));
    left.unwrap();
    right.unwrap();
    let claimed = store
        .claim(
            &config.task.queue_name,
            WorkerId::new(),
            &[(task_name, HandlerVersion::default())],
            10,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert!(store.delete_schedule(schedule.config.id).await.unwrap());
}

#[tokio::test]
async fn missed_occurrence_materialization_is_idempotent_across_restarts() {
    let _guard = schedule_test_guard().await;
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let task_name = TaskName::new(format!("restart-task-{suffix}")).unwrap();
    let request = EnqueueRequest::new(task_name.clone(), json!({}));
    let mut config = ScheduleConfig::new(
        ScheduleName::new(format!("restart-{suffix}")).unwrap(),
        ScheduleDefinition::interval(Duration::from_secs(10)).unwrap(),
        request,
    );
    config.start_at = Some(Utc::now() - TimeDelta::hours(24));
    let schedule = store.put_schedule(&config).await.unwrap();
    drop(store);

    let store = Store::connect(&database_url).await.unwrap();
    store.materialize_due_schedules(10).await.unwrap();
    drop(store);
    let store = Store::connect(&database_url).await.unwrap();
    store.materialize_due_schedules(10).await.unwrap();
    let claimed = store
        .claim(
            &config.task.queue_name,
            WorkerId::new(),
            &[(task_name, HandlerVersion::default())],
            10,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert!(store.delete_schedule(schedule.config.id).await.unwrap());
}

/// A database of its own: `claim_due_schedules` has no queue filter, so a schedule left due here
/// would reach every other test sharing the database.
async fn isolated_schedule_store(database_url: &str) -> (Store, sqlx::PgPool, String) {
    use std::str::FromStr;

    let database_name = format!("pgtask_schedule_{}", Uuid::new_v4().simple());
    let options = sqlx::postgres::PgConnectOptions::from_str(database_url).unwrap();
    let maintenance = sqlx::PgPool::connect_with(options.clone().database("postgres"))
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database_name}")))
        .execute(&maintenance)
        .await
        .unwrap();
    let store = Store::from_pool(
        sqlx::PgPool::connect_with(options.database(&database_name))
            .await
            .unwrap(),
    );
    store.migrate().await.unwrap();
    (store, maintenance, database_name)
}

#[tokio::test]
async fn put_schedule_rejects_a_definition_with_no_future_occurrence_even_with_a_start() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let mut config = ScheduleConfig::new(
        ScheduleName::new(format!("february-30-{}", Uuid::new_v4())).unwrap(),
        ScheduleDefinition::cron("0 0 0 30 2 *").unwrap(),
        EnqueueRequest::new(TaskName::new("never").unwrap(), json!({})),
    );
    config.start_at = Some(Utc::now());
    assert!(matches!(
        store.put_schedule(&config).await,
        Err(PostgresError::Schedule(ScheduleError::NoFutureOccurrence))
    ));
    config.start_at = None;
    assert!(matches!(
        store.put_schedule(&config).await,
        Err(PostgresError::Schedule(ScheduleError::NoFutureOccurrence))
    ));
}

#[tokio::test]
async fn an_unmaterializable_schedule_is_deferred_and_the_rest_of_the_batch_commits() {
    let Some(database_url) = database_url() else {
        return;
    };
    let (store, maintenance, database_name) = isolated_schedule_store(&database_url).await;

    // The SQL protocol stores a cron expression without evaluating it, so this row can exist.
    let broken: Uuid = sqlx::query_scalar(
        r"
        SELECT id FROM pgtask.put_schedule(
            gen_random_uuid(), 'february-30', 'cron', NULL, '0 0 0 30 2 *', 'latest', NULL,
            'schedules', 'broken', 1, '{}'::jsonb, '{}'::jsonb, 0::smallint, 5,
            statement_timestamp() - interval '1 minute'
        )
        ",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    let mut healthy = ScheduleConfig::new(
        ScheduleName::new("healthy").unwrap(),
        ScheduleDefinition::interval(Duration::from_secs(5)).unwrap(),
        EnqueueRequest::new(TaskName::new("healthy").unwrap(), json!({})),
    );
    healthy.task.queue_name = QueueName::new("schedules").unwrap();
    healthy.start_at = Some(Utc::now() - TimeDelta::seconds(1));
    let healthy = store.put_schedule(&healthy).await.unwrap();

    let before = Utc::now();
    assert_eq!(store.materialize_due_schedules(10).await.unwrap(), 1);
    let materialized: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtask.tasks WHERE schedule_id = $1")
        .bind(healthy.config.id.as_uuid())
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(materialized, 1);
    let deferred_until: DateTime<Utc> = sqlx::query_scalar("SELECT next_run_at FROM pgtask.schedules WHERE id = $1")
        .bind(broken)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert!(deferred_until >= before + TimeDelta::seconds(55));
    assert!(store.next_schedule_delay().await.unwrap() > Some(Duration::ZERO));

    store.pool().close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE {database_name} WITH (FORCE)"
    )))
    .execute(&maintenance)
    .await
    .unwrap();
}
