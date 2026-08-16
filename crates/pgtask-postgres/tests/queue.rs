use std::time::Duration;
use std::{
    collections::HashSet,
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
};

use chrono::{Duration as ChronoDuration, Utc};
use pgtask_core::{
    EnqueueRequest, EnqueueResult, HandlerVersion, LeaseRenewal, QueueConfig, QueueName, RetryPolicy,
    STORAGE_PROTOCOL_RANGE, STORAGE_PROTOCOL_VERSION, StepName, TaskName, TaskState, WorkerId,
};
use pgtask_postgres::{PostgresError, Store, StoreConfig};
use serde_json::json;
use sqlx::{Acquire, PgConnection, postgres::PgPoolOptions};
use tokio::sync::Barrier;
use uuid::Uuid;

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

async fn enqueue_concurrently(store: &Store, request: &EnqueueRequest) -> Vec<EnqueueResult> {
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let store = store.clone();
        let request = request.clone();
        tasks.spawn(async move { store.enqueue(&request).await.unwrap() });
    }
    let mut results = Vec::new();
    while let Some(result) = tasks.join_next().await {
        results.push(result.unwrap());
    }
    results
}

async fn configure_test_roles(store: &Store) {
    sqlx::query(
        r"
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgtask_test_producer') THEN
                CREATE ROLE pgtask_test_producer;
                CREATE ROLE pgtask_test_worker;
                CREATE ROLE pgtask_test_observer;
                CREATE ROLE pgtask_test_administrator;
            END IF;
        END
        $$
        ",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let owner: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(store.pool())
        .await
        .unwrap();
    store
        .configure_grants(
            &owner,
            "pgtask_test_producer",
            "pgtask_test_worker",
            "pgtask_test_observer",
            "pgtask_test_administrator",
        )
        .await
        .unwrap();
}

async fn assert_worker_protocol_grants(connection: &mut PgConnection, queue_name: &str) {
    let storage_protocol: i32 = sqlx::query_scalar("SELECT pgtask.storage_protocol_version()")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    assert_eq!(storage_protocol, i32::try_from(STORAGE_PROTOCOL_VERSION).unwrap());
    let storage_protocol_range: (i32, i32) =
        sqlx::query_as("SELECT minimum, maximum FROM pgtask.storage_protocol_range()")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
    assert_eq!(storage_protocol_range, (1, 1));
    let ready_channel: String = sqlx::query_scalar("SELECT pgtask.ready_channel($1)")
        .bind(queue_name)
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    assert!(ready_channel.starts_with("pgtask_ready_"));
    let capable_tasks: i64 =
        sqlx::query_scalar("SELECT capable_tasks FROM pgtask.queue_demand($1, ARRAY['role-task'], ARRAY[1])")
            .bind(queue_name)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
    assert_eq!(capable_tasks, 1);
    let maintenance_grants: (bool, bool, bool, bool, bool, bool) = sqlx::query_as(
        r"
        SELECT
            has_function_privilege(current_user, 'pgtask.put_schedule(uuid, text, text, bigint, text, text, integer, text, text, integer, jsonb, jsonb, smallint, integer, timestamptz)', 'EXECUTE'),
            has_function_privilege(current_user, 'pgtask.delete_expired_terminal(text, integer)', 'EXECUTE'),
            has_function_privilege(current_user, 'pgtask.wait_for_result(uuid, integer, uuid, text, integer, uuid, bigint)', 'EXECUTE'),
            has_function_privilege(current_user, 'pgtask.recover_result_wait_timeouts(integer)', 'EXECUTE'),
            has_function_privilege(current_user, 'pgtask.register_worker(uuid, text, text, text[], integer[], text[], bigint[], integer[], bigint[], bigint)', 'EXECUTE'),
            has_function_privilege(current_user, 'pgtask.delete_expired_idempotency_keys(text, integer)', 'EXECUTE')
        ",
    )
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    assert_eq!(maintenance_grants, (true, true, true, true, true, true));
}

async fn can_execute(connection: &mut PgConnection, function: &str) -> bool {
    sqlx::query_scalar("SELECT has_function_privilege(current_user, $1, 'EXECUTE')")
        .bind(function)
        .fetch_one(connection)
        .await
        .unwrap()
}

#[tokio::test]
async fn reports_the_supported_storage_protocol() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    Store::from_pool(store.pool().clone()).health().await.unwrap();

    assert_eq!(
        store.storage_protocol_version().await.unwrap(),
        STORAGE_PROTOCOL_VERSION
    );
    assert_eq!(store.storage_protocol_range().await.unwrap(), STORAGE_PROTOCOL_RANGE);
    assert_eq!(
        store.ensure_storage_protocol(STORAGE_PROTOCOL_RANGE).await.unwrap(),
        STORAGE_PROTOCOL_RANGE
    );
}

#[tokio::test]
async fn query_and_listener_connections_have_independent_endpoints_and_budgets() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect_with_config(
        &StoreConfig::new(&database_url)
            .with_query_connections(NonZeroU32::MIN)
            .with_listener_connections(NonZeroU32::MIN),
    )
    .await
    .unwrap();
    assert_eq!(store.pool().options().get_max_connections(), 1);
    store.health().await.unwrap();

    assert!(
        Store::connect_with_config(&StoreConfig::new(&database_url).with_listener_url("://"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn queue_subscriptions_share_one_listener_and_filter_payloads() {
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
    let suffix = Uuid::new_v4();
    let first_queue = QueueName::new(format!("ready-first-{suffix}")).unwrap();
    let second_queue = QueueName::new(format!("ready-second-{suffix}")).unwrap();
    let mut first_listener = store.ready_listener(&first_queue).await.unwrap();
    let mut second_listener = tokio::time::timeout(Duration::from_secs(1), store.ready_listener(&second_queue))
        .await
        .expect("a shared listener does not need a second database connection")
        .unwrap();

    let mut first_request = EnqueueRequest::new(TaskName::new("ready-first").unwrap(), json!({}));
    first_request.queue_name = first_queue.clone();
    let mut second_request = EnqueueRequest::new(TaskName::new("ready-second").unwrap(), json!({}));
    second_request.queue_name = second_queue.clone();
    store.enqueue(&second_request).await.unwrap();
    store.enqueue(&first_request).await.unwrap();

    let first_notification = tokio::time::timeout(Duration::from_secs(1), first_listener.recv())
        .await
        .unwrap()
        .unwrap();
    let second_notification = tokio::time::timeout(Duration::from_secs(1), second_listener.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_notification.payload(), first_queue.as_str());
    assert_eq!(second_notification.payload(), second_queue.as_str());
}

#[tokio::test]
async fn invalid_runtime_limits_fail_before_mutating_storage() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let queue_name = QueueName::new(format!("invalid-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("invalid-task").unwrap();
    let worker_id = WorkerId::new();

    assert!(matches!(
        store
            .register_worker(worker_id, &queue_name, "test", &[], Duration::from_secs(1))
            .await,
        Err(PostgresError::MissingCapabilities)
    ));
    assert!(matches!(
        store
            .register_worker(
                worker_id,
                &queue_name,
                "test",
                &[(task_name.clone(), HandlerVersion::default())],
                Duration::ZERO,
            )
            .await,
        Err(PostgresError::InvalidLeaseDuration)
    ));
    assert!(matches!(
        store.heartbeat_worker(worker_id, Duration::ZERO, false).await,
        Err(PostgresError::InvalidLeaseDuration)
    ));
    assert!(matches!(
        store.next_task_delay(&queue_name, &[]).await,
        Err(PostgresError::MissingCapabilities)
    ));
    assert!(matches!(
        store.queue_demand(&queue_name, &[]).await,
        Err(PostgresError::MissingCapabilities)
    ));
    assert!(matches!(
        store.delete_expired_terminal(&queue_name, 0).await,
        Err(PostgresError::InvalidRetentionLimit)
    ));
    assert!(matches!(
        store.materialize_due_schedules(0).await,
        Err(PostgresError::InvalidScheduleLimit)
    ));
    assert!(matches!(
        store.recover_wait_timeouts(0).await,
        Err(PostgresError::InvalidWaitLimit)
    ));
    assert!(matches!(
        store.recover_result_wait_timeouts(0).await,
        Err(PostgresError::InvalidWaitLimit)
    ));
    assert!(matches!(
        store.recover_expired(&queue_name, 0).await,
        Err(PostgresError::InvalidClaimLimit)
    ));
}

#[tokio::test]
async fn queue_demand_separates_capable_and_unroutable_ready_tasks() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("demand-{suffix}")).unwrap();
    let supported = TaskName::new(format!("supported-{suffix}")).unwrap();
    let unsupported = TaskName::new(format!("unsupported-{suffix}")).unwrap();
    let capabilities = [(supported.clone(), HandlerVersion::default())];
    let mut requests = Vec::new();
    for task_name in [supported.clone(), unsupported] {
        let mut request = EnqueueRequest::new(task_name, json!({}));
        request.queue_name = queue_name.clone();
        requests.push(request);
    }
    let mut delayed = EnqueueRequest::new(supported, json!({}));
    delayed.queue_name = queue_name.clone();
    delayed.run_at = Some(Utc::now() + ChronoDuration::hours(1));
    requests.push(delayed);
    store.enqueue_many(&requests).await.unwrap();
    let worker_id = WorkerId::new();
    store
        .register_worker(worker_id, &queue_name, "test", &capabilities, Duration::from_secs(30))
        .await
        .unwrap();

    assert_eq!(
        store.queue_demand(&queue_name, &capabilities).await.unwrap(),
        pgtask_postgres::QueueDemand {
            ready_tasks: 2,
            capable_tasks: 1,
            unroutable_tasks: 1,
        }
    );
    let overview: (i64, i64, i64) = sqlx::query_as(
        "SELECT ready_count, routable_count, unroutable_count FROM pgtask.queue_overview WHERE name = $1",
    )
    .bind(queue_name.as_str())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(overview, (2, 1, 1));

    assert!(
        store
            .heartbeat_worker(worker_id, Duration::from_secs(30), true)
            .await
            .unwrap()
    );
    let demand = store.queue_demand(&queue_name, &capabilities).await.unwrap();
    assert_eq!(demand.capable_tasks, 1);
    assert_eq!(demand.unroutable_tasks, 2);
    store.set_queue_paused(&queue_name, true).await.unwrap();
    assert_eq!(
        store
            .queue_demand(&queue_name, &capabilities)
            .await
            .unwrap()
            .ready_tasks,
        0
    );
}

#[tokio::test]
async fn invalid_task_and_lease_values_fail_before_mutating_storage() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let queue_name = QueueName::new(format!("invalid-task-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("invalid-task").unwrap();
    let worker_id = WorkerId::new();

    let mut invalid_enqueue = EnqueueRequest::new(task_name.clone(), json!({}));
    invalid_enqueue.max_attempts = 0;
    assert!(matches!(
        store.enqueue(&invalid_enqueue).await,
        Err(PostgresError::InvalidMaxAttempts)
    ));

    let capability = [(task_name.clone(), HandlerVersion::default())];
    assert!(matches!(
        store
            .claim(&queue_name, worker_id, &capability, 0, Duration::from_secs(1))
            .await,
        Err(PostgresError::InvalidClaimLimit)
    ));
    assert!(matches!(
        store
            .claim(&queue_name, worker_id, &capability, 1, Duration::ZERO)
            .await,
        Err(PostgresError::InvalidLeaseDuration)
    ));
    assert!(matches!(
        store
            .claim(&queue_name, worker_id, &[], 1, Duration::from_secs(1))
            .await,
        Err(PostgresError::MissingCapabilities)
    ));
    let unsupported_version = HandlerVersion::new(NonZeroU32::new(u32::try_from(i32::MAX).unwrap() + 1).unwrap());
    assert!(matches!(
        store
            .claim(
                &queue_name,
                worker_id,
                &[(task_name, unsupported_version)],
                1,
                Duration::from_secs(1),
            )
            .await,
        Err(PostgresError::InvalidHandlerVersion)
    ));

    assert!(matches!(
        store.renew_leases(&[], Duration::ZERO).await,
        Err(PostgresError::InvalidLeaseDuration)
    ));
    assert!(
        store
            .renew_leases(&[], Duration::from_secs(1))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store
            .sleep_for(
                pgtask_core::TaskId::new(),
                1,
                pgtask_core::LeaseToken::new(),
                &StepName::new("overflow").unwrap(),
                0,
                Duration::MAX,
            )
            .await,
        Err(PostgresError::InvalidSleepDuration)
    ));

    let mut queue = QueueConfig::new(queue_name);
    queue.terminal_retention = Duration::from_secs(u64::MAX);
    assert!(matches!(
        store.put_queue(&queue).await,
        Err(PostgresError::InvalidTask(_))
    ));
    queue.terminal_retention = Duration::ZERO;
    queue.idempotency_retention = Duration::from_secs(u64::MAX);
    assert!(matches!(
        store.put_queue(&queue).await,
        Err(PostgresError::InvalidTask(_))
    ));
    queue.idempotency_retention = Duration::ZERO;
    queue.max_outstanding_tasks = NonZeroU64::new(u64::MAX);
    assert!(matches!(
        store.put_queue(&queue).await,
        Err(PostgresError::InvalidTask(_))
    ));
    queue.max_outstanding_tasks = None;
    queue.starvation_timeout = Duration::from_secs(u64::MAX);
    assert!(matches!(
        store.put_queue(&queue).await,
        Err(PostgresError::InvalidTask(_))
    ));
}

#[tokio::test]
async fn transactional_enqueue_rolls_back_with_its_caller() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("transaction-{suffix}")).unwrap();
    let task_name = TaskName::new("transactional-task").unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();

    let mut connection = store.pool().acquire().await.unwrap();
    let mut transaction = connection.begin().await.unwrap();
    Store::enqueue_on(&mut transaction, &request).await.unwrap();
    transaction.rollback().await.unwrap();
    drop(connection);

    let claimed = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(task_name, HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    assert!(claimed.is_empty());

    let second = request;
    let mut connection = store.pool().acquire().await.unwrap();
    let mut transaction = connection.begin().await.unwrap();
    let enqueued = Store::enqueue_many_on(&mut transaction, &[second]).await.unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(enqueued.len(), 1);

    let malformed = EnqueueRequest::new(TaskName::new("malformed-batch-result").unwrap(), json!({}));
    let mut transaction = connection.begin().await.unwrap();
    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION pgtask.enqueue_many(p_tasks jsonb)
        RETURNS TABLE(request_index bigint, task_id uuid, created boolean)
        LANGUAGE sql
        AS $$
            SELECT 99::bigint, '00000000-0000-0000-0000-000000000001'::uuid, true
        $$
        ",
    )
    .execute(&mut *transaction)
    .await
    .unwrap();
    assert!(matches!(
        Store::enqueue_many_on(&mut transaction, &[malformed]).await,
        Err(PostgresError::InvalidTask(_))
    ));
    transaction.rollback().await.unwrap();
}

#[tokio::test]
async fn fenced_transitions_and_expired_lease_recovery() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("transitions-{suffix}")).unwrap();
    let task_name = TaskName::new("transition-task").unwrap();
    let capability = [(task_name.clone(), HandlerVersion::default())];
    let mut request = EnqueueRequest::new(task_name, json!({}));
    request.queue_name = queue_name.clone();
    request.max_attempts = 2;
    let task_id = store.enqueue(&request).await.unwrap().task_id;

    let first = store
        .claim(&queue_name, WorkerId::new(), &capability, 1, Duration::from_secs(30))
        .await
        .unwrap()
        .pop()
        .unwrap();
    let first_token = first.lease_token.unwrap();
    assert!(
        store
            .renew_lease(task_id, first.attempt, first_token, Duration::from_secs(30))
            .await
            .unwrap()
    );
    assert!(
        !store
            .renew_lease(
                task_id,
                first.attempt,
                pgtask_core::LeaseToken::new(),
                Duration::from_secs(30),
            )
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .fail(
                task_id,
                first.attempt,
                first_token,
                &json!({"type": "temporary"}),
                Some(Duration::ZERO)
            )
            .await
            .unwrap(),
        Some(TaskState::Pending)
    );

    let second = store
        .claim(&queue_name, WorkerId::new(), &capability, 1, Duration::from_millis(1))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(second.attempt, 2);
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(store.recover_expired(&queue_name, 10).await.unwrap(), 1);
    assert!(
        !store
            .complete(
                task_id,
                second.attempt,
                second.lease_token.unwrap(),
                Some(&json!({"late": true}))
            )
            .await
            .unwrap()
    );

    let mut completed_request = EnqueueRequest::new(capability[0].0.clone(), json!({}));
    completed_request.queue_name = queue_name.clone();
    let completed_id = store.enqueue(&completed_request).await.unwrap().task_id;
    let completed_task = store
        .claim(&queue_name, WorkerId::new(), &capability, 1, Duration::from_secs(30))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(completed_task.id, completed_id);
    let completed_token = completed_task.lease_token.unwrap();
    assert!(
        store
            .complete(
                completed_id,
                completed_task.attempt,
                completed_token,
                Some(&json!({"ok": true}))
            )
            .await
            .unwrap()
    );
    assert!(
        !store
            .complete(completed_id, completed_task.attempt, completed_token, None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn enqueue_deduplicate_and_claim_supported_due_tasks() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("test-{suffix}")).unwrap();
    let task_name = TaskName::new("known-task").unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({"message": "hello"}));
    request.queue_name = queue_name.clone();
    request.idempotency_key = Some(format!("deduplicate-{suffix}"));

    let first = store.enqueue(&request).await.unwrap();
    let duplicate = store.enqueue(&request).await.unwrap();
    assert!(first.created);
    assert!(!duplicate.created);
    assert_eq!(duplicate.task_id, first.task_id);
    assert_eq!(
        store
            .task_count_by_state(&queue_name, TaskState::Pending)
            .await
            .unwrap(),
        1
    );

    let unknown_name = TaskName::new("unknown-task").unwrap();
    let mut unknown = EnqueueRequest::new(unknown_name, json!({}));
    unknown.queue_name = queue_name.clone();
    store.enqueue(&unknown).await.unwrap();

    let mut delayed = EnqueueRequest::new(task_name.clone(), json!({"message": "later"}));
    delayed.queue_name = queue_name.clone();
    delayed.run_at = Some(Utc::now() + ChronoDuration::hours(1));
    store.enqueue(&delayed).await.unwrap();

    let claimed = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(task_name, HandlerVersion::default())],
            10,
            Duration::from_secs(30),
        )
        .await
        .unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, first.task_id);
    assert_eq!(claimed[0].state, TaskState::Running);
    assert_eq!(claimed[0].attempt, 1);
    assert!(claimed[0].lease_token.is_some());
    assert!(claimed[0].lease_expires_at.is_some());
}

#[tokio::test]
async fn idempotency_retention_is_independent_from_task_history() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("idempotency-retention-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("idempotency-retention-{suffix}")).unwrap();
    let mut queue = QueueConfig::new(queue_name.clone());
    queue.terminal_retention = Duration::ZERO;
    queue.idempotency_retention = Duration::from_hours(1);
    store.put_queue(&queue).await.unwrap();

    let mut retained_request = EnqueueRequest::new(task_name.clone(), json!({}));
    retained_request.queue_name = queue_name.clone();
    retained_request.idempotency_key = Some("retained".to_owned());
    let retained_id = store.enqueue(&retained_request).await.unwrap().task_id;
    let retained = store
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
    assert!(
        store
            .complete(retained.id, retained.attempt, retained.lease_token.unwrap(), None)
            .await
            .unwrap()
    );
    assert_eq!(store.delete_expired_terminal(&queue_name, 1).await.unwrap(), 1);
    assert!(store.get_task(retained_id).await.unwrap().is_none());
    let duplicate = store.enqueue(&retained_request).await.unwrap();
    assert!(!duplicate.created);
    assert_eq!(duplicate.task_id, retained_id);

    queue.idempotency_retention = Duration::ZERO;
    store.put_queue(&queue).await.unwrap();
    let mut reusable_request = retained_request.clone();
    reusable_request.idempotency_key = Some("reusable".to_owned());
    let reusable_id = store.enqueue(&reusable_request).await.unwrap().task_id;
    let reusable = store
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
    assert_eq!(
        store
            .fail(
                reusable.id,
                reusable.attempt,
                reusable.lease_token.unwrap(),
                &json!({"type": "terminal"}),
                None,
            )
            .await
            .unwrap(),
        Some(TaskState::Failed)
    );
    let actor = format!("idempotency-{suffix}");
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pgtask.admin_retry_task($1, $2)")
            .bind(reusable_id.as_uuid())
            .bind(&actor)
            .fetch_one(store.pool())
            .await
            .unwrap()
    );
    let retried_duplicate = store.enqueue(&reusable_request).await.unwrap();
    assert!(!retried_duplicate.created);
    assert_eq!(retried_duplicate.task_id, reusable_id);
    assert!(store.cancel(reusable_id).await.unwrap());
    assert_eq!(store.delete_expired_terminal(&queue_name, 1).await.unwrap(), 1);
    let audited_task_id: Uuid =
        sqlx::query_scalar("SELECT task_id FROM pgtask.administrator_audit_view WHERE actor = $1")
            .bind(actor)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(audited_task_id, reusable_id.as_uuid());
    assert_eq!(store.delete_expired_idempotency_keys(&queue_name, 1).await.unwrap(), 1);
    let reused = store.enqueue(&reusable_request).await.unwrap();
    assert!(reused.created);
    assert_ne!(reused.task_id, reusable_id);
    assert!(matches!(
        store.delete_expired_idempotency_keys(&queue_name, 0).await,
        Err(PostgresError::InvalidRetentionLimit)
    ));
}

#[tokio::test]
async fn concurrent_producers_create_one_task_per_idempotency_window() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("concurrent-idempotency-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("concurrent-idempotency-{suffix}")).unwrap();
    let mut queue = QueueConfig::new(queue_name.clone());
    queue.idempotency_retention = Duration::ZERO;
    store.put_queue(&queue).await.unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    request.idempotency_key = Some("one-window".to_owned());

    let first_window = enqueue_concurrently(&store, &request).await;
    assert_eq!(first_window.iter().filter(|result| result.created).count(), 1);
    let first_id = first_window[0].task_id;
    assert!(first_window.iter().all(|result| result.task_id == first_id));
    let first = store
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
    assert!(
        store
            .complete(first.id, first.attempt, first.lease_token.unwrap(), None)
            .await
            .unwrap()
    );

    let second_window = enqueue_concurrently(&store, &request).await;
    assert_eq!(second_window.iter().filter(|result| result.created).count(), 1);
    let second_id = second_window[0].task_id;
    assert_ne!(second_id, first_id);
    assert!(second_window.iter().all(|result| result.task_id == second_id));
}

#[tokio::test]
async fn claim_orders_due_tasks_by_priority_then_run_time() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("priority-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("priority-task").unwrap();
    let now = Utc::now() - ChronoDuration::minutes(1);
    let requests: Vec<_> = [("low", -1, 0), ("new-high", 10, 20), ("old-high", 10, 10)]
        .into_iter()
        .map(|(name, priority, seconds)| {
            let mut request = EnqueueRequest::new(task_name.clone(), json!({"name": name}));
            request.queue_name = queue_name.clone();
            request.priority = priority;
            request.run_at = Some(now + ChronoDuration::seconds(seconds));
            request
        })
        .collect();
    store.enqueue_many(&requests).await.unwrap();

    let claimed = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(task_name, HandlerVersion::default())],
            3,
            Duration::from_secs(30),
        )
        .await
        .unwrap();

    assert_eq!(
        claimed
            .iter()
            .map(|task| task.payload["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["old-high", "new-high", "low"]
    );
}

#[tokio::test]
async fn starvation_timeout_rescues_the_oldest_due_task() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("starvation-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("starvation-task").unwrap();
    let mut queue = QueueConfig::new(queue_name.clone());
    queue.starvation_timeout = Duration::ZERO;
    store.put_queue(&queue).await.unwrap();
    let now = Utc::now() - ChronoDuration::minutes(1);
    let requests: Vec<_> = [("old-low", -10, 0), ("new-high", 10, 30)]
        .into_iter()
        .map(|(name, priority, seconds)| {
            let mut request = EnqueueRequest::new(task_name.clone(), json!({"name": name}));
            request.queue_name = queue_name.clone();
            request.priority = priority;
            request.run_at = Some(now + ChronoDuration::seconds(seconds));
            request
        })
        .collect();
    store.enqueue_many(&requests).await.unwrap();

    let claimed = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(task_name, HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap();

    assert_eq!(claimed[0].payload["name"], "old-low");
}

#[tokio::test]
async fn queue_capacity_is_atomic_under_concurrent_producers() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("capacity-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("capacity-task").unwrap();
    let mut queue = QueueConfig::new(queue_name.clone());
    queue.max_outstanding_tasks = NonZeroU64::new(4);
    store.put_queue(&queue).await.unwrap();

    let oversized_batch: Vec<_> = (0..5)
        .map(|sequence| {
            let mut request = EnqueueRequest::new(task_name.clone(), json!({"batch": sequence}));
            request.queue_name = queue_name.clone();
            request
        })
        .collect();
    let error = store.enqueue_many(&oversized_batch).await.unwrap_err();
    let PostgresError::Database(sqlx::Error::Database(error)) = error else {
        panic!("unexpected batch enqueue error: {error}");
    };
    assert_eq!(error.code().as_deref(), Some("PT001"));
    assert_eq!(
        store
            .task_count_by_state(&queue_name, TaskState::Pending)
            .await
            .unwrap(),
        0
    );

    let mut producers = tokio::task::JoinSet::new();
    for sequence in 0..16 {
        let store = store.clone();
        let queue_name = queue_name.clone();
        let task_name = task_name.clone();
        producers.spawn(async move {
            let mut request = EnqueueRequest::new(task_name, json!({"sequence": sequence}));
            request.queue_name = queue_name;
            store.enqueue(&request).await
        });
    }
    let mut created = Vec::new();
    let mut rejected = 0;
    while let Some(result) = producers.join_next().await {
        match result.unwrap() {
            Ok(result) => created.push(result.task_id),
            Err(PostgresError::Database(sqlx::Error::Database(error))) => {
                assert_eq!(error.code().as_deref(), Some("PT001"));
                rejected += 1;
            }
            Err(error) => panic!("unexpected enqueue error: {error}"),
        }
    }
    assert_eq!(created.len(), 4);
    assert_eq!(rejected, 12);
    let overview: (i64, Option<i64>, i64) = sqlx::query_as(
        "SELECT outstanding_count, max_outstanding_tasks, starvation_timeout_seconds \
         FROM pgtask.queue_overview WHERE name = $1",
    )
    .bind(queue_name.as_str())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(overview, (4, Some(4), 300));

    let duplicate_key = format!("capacity-{}", Uuid::new_v4());
    assert!(store.cancel(created[0]).await.unwrap());
    let mut keyed = EnqueueRequest::new(task_name.clone(), json!({}));
    keyed.queue_name = queue_name.clone();
    keyed.idempotency_key = Some(duplicate_key);
    let first = store.enqueue(&keyed).await.unwrap();
    assert!(!store.enqueue(&keyed).await.unwrap().created);
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
    assert!(store.cancel(running.id).await.unwrap());
    assert_ne!(first.task_id, running.id);
    store
        .enqueue(&EnqueueRequest {
            queue_name,
            ..EnqueueRequest::new(task_name, json!({"replacement": true}))
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn queue_configuration_pauses_and_resumes_claiming() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("configured-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("configured-task").unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    store.enqueue(&request).await.unwrap();

    let implicit = store.get_queue(&queue_name).await.unwrap().unwrap();
    assert_eq!(implicit.terminal_retention, Duration::from_hours(7 * 24));
    assert_eq!(implicit.idempotency_retention, Duration::from_hours(30 * 24));
    assert_eq!(implicit.max_outstanding_tasks, None);
    assert_eq!(implicit.starvation_timeout, Duration::from_mins(5));
    let mut config = QueueConfig::new(queue_name.clone());
    config.terminal_retention = Duration::from_mins(1);
    config.idempotency_retention = Duration::from_hours(1);
    config.max_outstanding_tasks = NonZeroU64::new(100);
    config.starvation_timeout = Duration::from_secs(45);
    let configured = store.put_queue(&config).await.unwrap();
    assert_eq!(configured.terminal_retention, Duration::from_mins(1));
    assert_eq!(configured.idempotency_retention, Duration::from_hours(1));
    assert_eq!(configured.max_outstanding_tasks.unwrap().get(), 100);
    assert_eq!(configured.starvation_timeout, Duration::from_secs(45));
    assert!(
        store
            .set_queue_paused(&queue_name, true)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_some()
    );

    let capability = [(task_name, HandlerVersion::default())];
    assert!(
        store
            .claim(&queue_name, WorkerId::new(), &capability, 1, Duration::from_secs(30))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .set_queue_paused(&queue_name, false)
            .await
            .unwrap()
            .unwrap()
            .paused_at
            .is_none()
    );
    assert_eq!(
        store
            .claim(&queue_name, WorkerId::new(), &capability, 1, Duration::from_secs(30))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn terminal_retention_deletes_bounded_batches() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("retention-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("retained-task").unwrap();
    let mut config = QueueConfig::new(queue_name.clone());
    config.terminal_retention = Duration::ZERO;
    store.put_queue(&config).await.unwrap();
    let mut requests = Vec::new();
    for _ in 0..2 {
        let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
        request.queue_name = queue_name.clone();
        requests.push(request);
    }
    let enqueued = store.enqueue_many(&requests).await.unwrap();
    let claimed = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(task_name, HandlerVersion::default())],
            2,
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    for task in claimed {
        assert!(
            store
                .complete(task.id, task.attempt, task.lease_token.unwrap(), None)
                .await
                .unwrap()
        );
    }

    assert_eq!(store.delete_expired_terminal(&queue_name, 1).await.unwrap(), 1);
    let mut retained = 0;
    for task in &enqueued {
        retained += usize::from(store.get_task(task.task_id).await.unwrap().is_some());
    }
    assert_eq!(retained, 1);
    assert_eq!(store.delete_expired_terminal(&queue_name, 10).await.unwrap(), 1);
    assert!(store.get_task(enqueued[0].task_id).await.unwrap().is_none());
    assert!(store.get_task(enqueued[1].task_id).await.unwrap().is_none());
}

#[tokio::test]
async fn renews_multiple_fenced_leases_in_one_operation() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("batch-renewal-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("batch-renewal-task").unwrap();
    let requests: Vec<_> = (0..2)
        .map(|_| {
            let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
            request.queue_name = queue_name.clone();
            request
        })
        .collect();
    store.enqueue_many(&requests).await.unwrap();
    let tasks = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(task_name, HandlerVersion::default())],
            2,
            Duration::from_millis(30),
        )
        .await
        .unwrap();
    let previous_expiry = tasks[0].lease_expires_at.unwrap();
    let leases: Vec<_> = tasks
        .iter()
        .map(|task| LeaseRenewal {
            task_id: task.id,
            attempt: task.attempt,
            lease_token: task.lease_token.unwrap(),
        })
        .collect();

    let renewed = store.renew_leases(&leases, Duration::from_secs(1)).await.unwrap();

    assert_eq!(renewed.len(), 2);
    assert!(
        store
            .get_task(tasks[0].id)
            .await
            .unwrap()
            .unwrap()
            .lease_expires_at
            .unwrap()
            > previous_expiry
    );
}

#[tokio::test]
async fn cancellation_is_terminal_and_fences_running_handlers() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("cancel-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("cancel-task").unwrap();
    let mut requests: Vec<_> = (0..2)
        .map(|_| {
            let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
            request.queue_name = queue_name.clone();
            request
        })
        .collect();
    requests[0].idempotency_key = Some(format!("pending-{}", Uuid::new_v4()));
    requests[1].idempotency_key = Some(format!("running-{}", Uuid::new_v4()));
    let enqueued = store.enqueue_many(&requests).await.unwrap();
    assert!(store.cancel(enqueued[0].task_id).await.unwrap());
    assert!(!store.cancel(enqueued[0].task_id).await.unwrap());

    let running = store
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
    assert_eq!(running.id, enqueued[1].task_id);
    assert!(store.cancel(running.id).await.unwrap());
    assert!(
        !store
            .complete(running.id, running.attempt, running.lease_token.unwrap(), None)
            .await
            .unwrap()
    );
    assert_eq!(
        store.get_task(running.id).await.unwrap().unwrap().state,
        TaskState::Cancelled
    );
}

#[tokio::test]
async fn worker_registration_reports_versioned_capabilities_and_expiry() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("registration-{}", Uuid::new_v4())).unwrap();
    store.put_queue(&QueueConfig::new(queue_name.clone())).await.unwrap();
    let worker_id = WorkerId::new();
    assert!(store.get_worker(worker_id).await.unwrap().is_none());
    let capabilities = [
        (TaskName::new("alpha").unwrap(), HandlerVersion::default()),
        (
            TaskName::new("beta").unwrap(),
            HandlerVersion::new(std::num::NonZeroU32::new(2).unwrap()),
        ),
    ];
    store
        .register_worker(
            worker_id,
            &queue_name,
            "test-version",
            &capabilities,
            Duration::from_secs(30),
        )
        .await
        .unwrap();

    let registered = store.get_worker(worker_id).await.unwrap().unwrap();
    assert_eq!(registered.queue_name, queue_name);
    assert_eq!(registered.version, "test-version");
    assert_eq!(registered.capabilities, capabilities);
    assert!(!registered.draining);
    assert!(registered.expires_at > registered.heartbeat_at);

    assert!(
        store
            .heartbeat_worker(worker_id, Duration::from_millis(1), true)
            .await
            .unwrap()
    );
    let stopped = store.get_worker(worker_id).await.unwrap().unwrap();
    assert!(stopped.draining);
    assert!(stopped.heartbeat_at >= registered.heartbeat_at);
}

#[tokio::test]
async fn retry_policies_are_immutable_per_handler_version_and_snapshotted_on_tasks() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("retry-policy-{suffix}")).unwrap();
    let fixed_name = TaskName::new(format!("fixed-{suffix}")).unwrap();
    let exponential_name = TaskName::new(format!("exponential-{suffix}")).unwrap();
    let fixed = RetryPolicy::Fixed {
        delay: Duration::from_secs(3),
    };
    store
        .register_worker_with_policies(
            WorkerId::new(),
            &queue_name,
            "fixed",
            &[(fixed_name.clone(), HandlerVersion::default(), fixed)],
            Duration::from_secs(30),
        )
        .await
        .unwrap();

    let mut fixed_request = EnqueueRequest::new(fixed_name.clone(), json!({}));
    fixed_request.queue_name = queue_name.clone();
    let fixed_id = store.enqueue(&fixed_request).await.unwrap().task_id;
    assert_eq!(
        store.get_task(fixed_id).await.unwrap().unwrap().retry_policy,
        Some(fixed)
    );

    let mut exponential_request = EnqueueRequest::new(exponential_name.clone(), json!({}));
    exponential_request.queue_name = queue_name.clone();
    let exponential_id = store.enqueue(&exponential_request).await.unwrap().task_id;
    assert_eq!(
        store.get_task(exponential_id).await.unwrap().unwrap().retry_policy,
        None
    );
    let exponential = RetryPolicy::Exponential {
        base_delay: Duration::from_secs(2),
        factor: 3,
        max_delay: Duration::from_secs(20),
    };
    store
        .register_worker_with_policies(
            WorkerId::new(),
            &queue_name,
            "exponential",
            &[(exponential_name.clone(), HandlerVersion::default(), exponential)],
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let claimed = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(exponential_name, HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.id, exponential_id);
    assert_eq!(claimed.retry_policy, Some(exponential));

    let conflict = store
        .register_worker_with_policies(
            WorkerId::new(),
            &queue_name,
            "conflict",
            &[(fixed_name, HandlerVersion::default(), RetryPolicy::Never)],
            Duration::from_secs(30),
        )
        .await
        .unwrap_err();
    assert!(conflict.to_string().contains("retry policy is immutable"));
}

#[tokio::test]
async fn rejects_oversized_task_values_without_partial_transitions() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("limits-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("limited-task").unwrap();
    let mut oversized = EnqueueRequest::new(task_name.clone(), json!("x".repeat(1_048_576)));
    oversized.queue_name = queue_name.clone();
    assert!(store.enqueue(&oversized).await.is_err());

    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let running = store
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
    assert!(
        store
            .complete(
                task_id,
                running.attempt,
                running.lease_token.unwrap(),
                Some(&json!("x".repeat(1_048_576))),
            )
            .await
            .is_err()
    );
    assert_eq!(
        store.get_task(task_id).await.unwrap().unwrap().state,
        TaskState::Running
    );
}

#[tokio::test]
async fn concurrent_claimers_receive_each_task_once() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("contention-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("contended-task").unwrap();
    for sequence in 0..40 {
        let mut request = EnqueueRequest::new(task_name.clone(), json!({"sequence": sequence}));
        request.queue_name = queue_name.clone();
        store.enqueue(&request).await.unwrap();
    }

    let barrier = Arc::new(Barrier::new(8));
    let mut claimers = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let store = store.clone();
        let queue_name = queue_name.clone();
        let task_name = task_name.clone();
        let barrier = Arc::clone(&barrier);
        claimers.spawn(async move {
            barrier.wait().await;
            store
                .claim(
                    &queue_name,
                    WorkerId::new(),
                    &[(task_name, HandlerVersion::default())],
                    10,
                    Duration::from_secs(30),
                )
                .await
                .unwrap()
        });
    }

    let mut claimed_ids = HashSet::new();
    while let Some(tasks) = claimers.join_next().await {
        for task in tasks.unwrap() {
            assert!(claimed_ids.insert(task.id));
        }
    }
    assert_eq!(claimed_ids.len(), 40);
}

#[tokio::test]
async fn batch_enqueue_is_ordered_and_atomic() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("batch-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("batch-task").unwrap();
    let requests: Vec<_> = (0..5)
        .map(|sequence| {
            let mut request = EnqueueRequest::new(task_name.clone(), json!({"sequence": sequence}));
            request.queue_name = queue_name.clone();
            request.idempotency_key = Some(format!("{queue_name}-{sequence}"));
            request
        })
        .collect();

    let inserted = store.enqueue_many(&requests).await.unwrap();
    assert_eq!(inserted.len(), requests.len());
    assert!(inserted.iter().all(|result| result.created));
    let duplicate = store.enqueue_many(&requests).await.unwrap();
    assert_eq!(
        duplicate.iter().map(|result| result.task_id).collect::<Vec<_>>(),
        inserted.iter().map(|result| result.task_id).collect::<Vec<_>>()
    );
    assert!(duplicate.iter().all(|result| !result.created));

    let mut invalid = requests.clone();
    invalid[2].max_attempts = 0;
    assert!(store.enqueue_many(&invalid).await.is_err());
}

#[tokio::test]
async fn committed_transitions_survive_client_restarts() {
    let Some(database_url) = database_url() else {
        return;
    };
    let mut store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("crash-paths-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("crash-path-task").unwrap();
    let capability = [(task_name.clone(), HandlerVersion::default())];
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    request.max_attempts = 4;
    let task_id = store.enqueue(&request).await.unwrap().task_id;

    drop(store);
    store = Store::connect(&database_url).await.unwrap();
    assert_eq!(
        store.get_task(task_id).await.unwrap().unwrap().state,
        TaskState::Pending
    );

    let claimed = store
        .claim(&queue_name, WorkerId::new(), &capability, 1, Duration::from_millis(50))
        .await
        .unwrap()
        .pop()
        .unwrap();
    let lease_token = claimed.lease_token.unwrap();
    drop(store);
    store = Store::connect(&database_url).await.unwrap();
    assert_eq!(store.get_task(task_id).await.unwrap().unwrap().attempt, 1);

    let previous_expiry = claimed.lease_expires_at.unwrap();
    assert!(
        store
            .renew_lease(task_id, claimed.attempt, lease_token, Duration::from_secs(30))
            .await
            .unwrap()
    );
    drop(store);
    store = Store::connect(&database_url).await.unwrap();
    assert!(
        store
            .get_task(task_id)
            .await
            .unwrap()
            .unwrap()
            .lease_expires_at
            .unwrap()
            > previous_expiry
    );

    assert_eq!(
        store
            .fail(
                task_id,
                claimed.attempt,
                lease_token,
                &json!({"type": "transient"}),
                Some(Duration::ZERO),
            )
            .await
            .unwrap(),
        Some(TaskState::Pending)
    );
    drop(store);
    store = Store::connect(&database_url).await.unwrap();
    assert_eq!(
        store.get_task(task_id).await.unwrap().unwrap().state,
        TaskState::Pending
    );

    let terminal = store
        .claim(&queue_name, WorkerId::new(), &capability, 1, Duration::from_secs(30))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        store
            .fail(
                task_id,
                terminal.attempt,
                terminal.lease_token.unwrap(),
                &json!({"type": "terminal"}),
                None,
            )
            .await
            .unwrap(),
        Some(TaskState::Failed)
    );
    drop(store);
    store = Store::connect(&database_url).await.unwrap();
    assert_eq!(store.get_task(task_id).await.unwrap().unwrap().state, TaskState::Failed);
}

#[tokio::test]
async fn committed_completion_and_recovery_survive_client_restarts() {
    let Some(database_url) = database_url() else {
        return;
    };
    let mut store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("crash-terminal-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("crash-terminal-task").unwrap();
    let capability = [(task_name.clone(), HandlerVersion::default())];
    let mut request = EnqueueRequest::new(task_name, json!({}));
    request.queue_name = queue_name.clone();
    request.max_attempts = 4;

    request.idempotency_key = Some(format!("complete-{}", Uuid::new_v4()));
    let completed_id = store.enqueue(&request).await.unwrap().task_id;
    let completed = store
        .claim(&queue_name, WorkerId::new(), &capability, 1, Duration::from_secs(30))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(
        store
            .complete(
                completed_id,
                completed.attempt,
                completed.lease_token.unwrap(),
                Some(&json!({"durable": true})),
            )
            .await
            .unwrap()
    );
    drop(store);
    store = Store::connect(&database_url).await.unwrap();
    let completed = store.get_task(completed_id).await.unwrap().unwrap();
    assert_eq!(completed.state, TaskState::Succeeded);
    assert_eq!(completed.result, Some(json!({"durable": true})));

    request.idempotency_key = Some(format!("recover-{}", Uuid::new_v4()));
    let recovered_id = store.enqueue(&request).await.unwrap().task_id;
    store
        .claim(&queue_name, WorkerId::new(), &capability, 1, Duration::from_millis(1))
        .await
        .unwrap();
    drop(store);
    tokio::time::sleep(Duration::from_millis(5)).await;
    store = Store::connect(&database_url).await.unwrap();
    assert_eq!(store.recover_expired(&queue_name, 1).await.unwrap(), 1);
    drop(store);
    store = Store::connect(&database_url).await.unwrap();
    assert_eq!(
        store.get_task(recovered_id).await.unwrap().unwrap().state,
        TaskState::Pending
    );
}

#[tokio::test]
async fn checkpoints_replay_the_first_fenced_value_and_enforce_size_limits() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("checkpoint-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("checkpoint-task").unwrap();
    let step_name = StepName::new("fetch-user").unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let running = store
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
    let lease_token = running.lease_token.unwrap();

    let first = store
        .commit_checkpoint(
            task_id,
            running.attempt,
            lease_token,
            &step_name,
            0,
            &json!({"user": 1}),
        )
        .await
        .unwrap()
        .unwrap();
    let replayed = store
        .commit_checkpoint(
            task_id,
            running.attempt,
            lease_token,
            &step_name,
            0,
            &json!({"user": 2}),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replayed.value, first.value);
    assert_eq!(
        store
            .get_checkpoint(task_id, running.handler_version, &step_name, 0)
            .await
            .unwrap()
            .unwrap()
            .value,
        json!({"user": 1})
    );
    assert!(
        store
            .commit_checkpoint(
                task_id,
                running.attempt,
                pgtask_core::LeaseToken::new(),
                &step_name,
                1,
                &json!({}),
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .commit_checkpoint(
                task_id,
                running.attempt,
                lease_token,
                &step_name,
                2,
                &json!("x".repeat(1_048_576)),
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn runtime_roles_only_receive_their_protocol_capabilities() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    configure_test_roles(&store).await;

    let queue_name = format!("roles-{}", Uuid::new_v4());
    let mut producer = store.pool().acquire().await.unwrap();
    sqlx::query("SET ROLE pgtask_test_producer")
        .execute(&mut *producer)
        .await
        .unwrap();
    let producer_protocol: (i32, i32) = sqlx::query_as("SELECT minimum, maximum FROM pgtask.storage_protocol_range()")
        .fetch_one(&mut *producer)
        .await
        .unwrap();
    assert_eq!(producer_protocol, (1, 1));
    assert!(!can_execute(&mut producer, "pgtask.put_queue(text, bigint, bigint, bigint, bigint)").await);
    let task_id: Uuid =
        sqlx::query_scalar("SELECT task_id FROM pgtask.enqueue('role-task', '{}'::jsonb, $1) WHERE created")
            .bind(&queue_name)
            .fetch_one(&mut *producer)
            .await
            .unwrap();
    let result_channel: String = sqlx::query_scalar("SELECT pgtask.result_channel($1)")
        .bind(task_id)
        .fetch_one(&mut *producer)
        .await
        .unwrap();
    assert!(result_channel.starts_with("pgtask_result_"));
    let error = sqlx::query("SELECT count(*) FROM pgtask.tasks")
        .execute(&mut *producer)
        .await
        .unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("42501"));
    sqlx::query("RESET ROLE").execute(&mut *producer).await.unwrap();
    drop(producer);

    let mut worker = store.pool().acquire().await.unwrap();
    sqlx::query("SET ROLE pgtask_test_worker")
        .execute(&mut *worker)
        .await
        .unwrap();
    assert_worker_protocol_grants(&mut worker, &queue_name).await;
    let claimed_id: Uuid = sqlx::query_scalar(
        "SELECT id FROM pgtask.claim($1, gen_random_uuid(), ARRAY['role-task'], ARRAY[1], 1, 30000)",
    )
    .bind(&queue_name)
    .fetch_one(&mut *worker)
    .await
    .unwrap();
    assert_eq!(claimed_id, task_id);
    let error = sqlx::query("SELECT * FROM pgtask.cancel_task($1)")
        .bind(task_id)
        .execute(&mut *worker)
        .await
        .unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("42501"));
    sqlx::query("RESET ROLE").execute(&mut *worker).await.unwrap();
    drop(worker);

    let mut observer = store.pool().acquire().await.unwrap();
    sqlx::query("SET ROLE pgtask_test_observer")
        .execute(&mut *observer)
        .await
        .unwrap();
    let visible_tasks: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtask.task_view WHERE id = $1")
        .bind(task_id)
        .fetch_one(&mut *observer)
        .await
        .unwrap();
    assert_eq!(visible_tasks, 1);
    let error = sqlx::query("SELECT count(*) FROM pgtask.tasks")
        .execute(&mut *observer)
        .await
        .unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().as_deref(), Some("42501"));
    sqlx::query("RESET ROLE").execute(&mut *observer).await.unwrap();
    drop(observer);

    let mut administrator = store.pool().acquire().await.unwrap();
    sqlx::query("SET ROLE pgtask_test_administrator")
        .execute(&mut *administrator)
        .await
        .unwrap();
    assert!(
        can_execute(
            &mut administrator,
            "pgtask.put_queue(text, bigint, bigint, bigint, bigint)"
        )
        .await
    );
    let cancelled: Option<String> = sqlx::query_scalar("SELECT task_name FROM pgtask.cancel_task($1)")
        .bind(task_id)
        .fetch_optional(&mut *administrator)
        .await
        .unwrap();
    assert_eq!(cancelled.as_deref(), Some("role-task"));
    sqlx::query("RESET ROLE").execute(&mut *administrator).await.unwrap();
    drop(administrator);
}
