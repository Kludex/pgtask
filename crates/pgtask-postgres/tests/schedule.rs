use std::{
    num::{NonZeroU16, NonZeroU64},
    sync::OnceLock,
    time::Duration,
};

use chrono::{DateTime, TimeDelta, Utc};
use pgtask_core::{
    EnqueueRequest, HandlerVersion, MisfirePolicy, QueueConfig, QueueName, ScheduleConfig, ScheduleDefinition,
    ScheduleId, ScheduleName, TaskName, WorkerId,
};
use pgtask_postgres::Store;
use serde_json::json;
use uuid::Uuid;

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

async fn schedule_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static GUARD: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    GUARD.get_or_init(|| tokio::sync::Mutex::new(())).lock().await
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

    store.materialize_due_schedules(10).await.unwrap();
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
