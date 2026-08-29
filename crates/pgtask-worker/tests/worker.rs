use std::{
    net::TcpListener as StdTcpListener,
    num::NonZeroU16,
    str::FromStr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::{TimeDelta, Utc};
use pgtask_core::{
    EnqueueRequest, HandlerVersion, QueueConfig, QueueName, RetryPolicy, ScheduleConfig, ScheduleDefinition,
    ScheduleName, SignalName, StepName, TaskName, TaskState,
};
use pgtask_postgres::{PostgresError, Store};
use pgtask_worker::{HandlerError, HandlerRegistry, Worker, WorkerConfig, WorkerError};
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, MutexGuard, Notify, Semaphore},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

async fn database_fault_guard() -> MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD.get_or_init(|| Mutex::new(())).lock().await
}

async fn drop_runtime_role(admin: &Store, role: &str) {
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1")
        .bind(role)
        .execute(admin.pool())
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP OWNED BY {role}")))
        .execute(admin.pool())
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP ROLE {role}")))
        .execute(admin.pool())
        .await
        .unwrap();
}

struct DropNotification(Arc<Notify>);

impl Drop for DropNotification {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

async fn health_status(address: std::net::SocketAddr, path: &str) -> u16 {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response.split_ascii_whitespace().nth(1).unwrap().parse().unwrap()
}

fn successful_registry(task_name: &TaskName) -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { Ok(json!(null)) },
    );
    registry
}

async fn complete_ready_child(store: &Store, queue_name: &QueueName, task_name: &TaskName) -> Result<(), HandlerError> {
    let child = store
        .claim(
            queue_name,
            pgtask_core::WorkerId::new(),
            &[(task_name.clone(), HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .map_err(|error| HandlerError::terminal(error.to_string()))?
        .pop()
        .unwrap();
    store
        .complete(
            child.id,
            child.attempt,
            child.lease_token.unwrap(),
            Some(&json!({"ready": true})),
        )
        .await
        .map_err(|error| HandlerError::terminal(error.to_string()))?;
    Ok(())
}

struct DatabaseFaultWorker {
    admin: Store,
    fault_queue: QueueName,
    owner: String,
    release: Arc<Semaphore>,
    role: String,
    shutdown: CancellationToken,
    worker_task: tokio::task::JoinHandle<Result<(), WorkerError>>,
}

impl DatabaseFaultWorker {
    async fn start(database_url: &str) -> Self {
        let admin = Store::connect(database_url).await.unwrap();
        admin.migrate().await.unwrap();
        let suffix = Uuid::new_v4().simple();
        let role = format!("pgtask_fault_{suffix}");
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {role} LOGIN PASSWORD 'fault-test'"
        )))
        .execute(admin.pool())
        .await
        .unwrap();
        let owner: String = sqlx::query_scalar("SELECT current_user")
            .fetch_one(admin.pool())
            .await
            .unwrap();
        admin
            .configure_grants(&owner, &role, &role, &role, &role)
            .await
            .unwrap();

        let options = PgConnectOptions::from_str(database_url)
            .unwrap()
            .username(&role)
            .password("fault-test")
            .application_name(&role);
        let worker_store = Store::from_pool(
            PgPoolOptions::new()
                .acquire_timeout(Duration::from_secs(1))
                .connect_with(options)
                .await
                .unwrap(),
        );
        let fault_queue = QueueName::new(format!("database-fault-{suffix}")).unwrap();
        let task_name = TaskName::new(format!("database-fault-task-{suffix}")).unwrap();
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let handler_started = Arc::clone(&started);
        let handler_release = Arc::clone(&release);
        let mut registry = HandlerRegistry::new();
        registry.register(
            task_name.clone(),
            HandlerVersion::default(),
            RetryPolicy::Never,
            move |task| {
                let started = Arc::clone(&handler_started);
                let release = Arc::clone(&handler_release);
                async move {
                    started.add_permits(1);
                    if task.payload == json!("release") {
                        release.acquire().await.unwrap().forget();
                        Ok(json!(null))
                    } else {
                        std::future::pending().await
                    }
                }
            },
        );
        for payload in [json!("release"), json!("block"), json!("block")] {
            let mut request = EnqueueRequest::new(task_name.clone(), payload);
            request.queue_name = fault_queue.clone();
            request.max_attempts = 1;
            admin.enqueue(&request).await.unwrap();
        }
        let mut config = WorkerConfig::new(fault_queue.clone());
        config.concurrency = NonZeroU16::new(3).unwrap();
        config.claim_batch_size = NonZeroU16::new(3).unwrap();
        config.lease_duration = Duration::from_millis(90);
        config.poll_interval = Duration::from_millis(20);
        config.worker_heartbeat_interval = Duration::from_millis(20);
        config.worker_ttl = Duration::from_millis(200);
        config.schedule_reconciliation_interval = Duration::from_millis(20);
        // Retention must tick inside the fault windows below, or its failure path is never exercised.
        config.retention_interval = Duration::from_millis(20);
        config.supervisor_interval = Duration::from_millis(20);
        config.shutdown_grace = Duration::from_millis(20);
        let worker = Worker::new(worker_store, registry, config).unwrap();
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
        started.acquire_many(3).await.unwrap().forget();
        Self {
            admin,
            fault_queue,
            owner,
            release,
            role,
            shutdown,
            worker_task,
        }
    }

    async fn restore_grants(&self) {
        self.admin
            .configure_grants(&self.owner, &self.role, &self.role, &self.role, &self.role)
            .await
            .unwrap();
    }

    async fn stop(self) {
        self.shutdown.cancel();
        self.worker_task.await.unwrap().unwrap();
        drop_runtime_role(&self.admin, &self.role).await;
    }
}

#[test]
fn handler_registry_and_errors_expose_explicit_public_values() {
    let task_name = TaskName::new("public-handler").unwrap();
    let mut registry = HandlerRegistry::new();
    assert!(registry.register(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async { Ok(json!(null)) },
    ));
    assert!(!registry.register(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async { Ok(json!(null)) },
    ));
    assert_eq!(registry.capabilities(), vec![(task_name, HandlerVersion::default())]);

    let retryable = HandlerError::retryable("again");
    assert!(retryable.retryable);
    assert_eq!(retryable.error["message"], "again");
    assert!(!retryable.is_suspended());
    let terminal = HandlerError::terminal("stop");
    assert!(!terminal.retryable);
    assert_eq!(terminal.error["message"], "stop");
    let suspended = HandlerError::suspended();
    assert!(suspended.is_suspended());
}

#[tokio::test]
async fn worker_configuration_rejects_every_invalid_invariant() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    let queue_name = QueueName::new(format!("worker-validation-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("worker-validation").unwrap();

    assert!(matches!(
        Worker::new(
            store.clone(),
            HandlerRegistry::new(),
            WorkerConfig::new(queue_name.clone())
        ),
        Err(WorkerError::MissingHandlers)
    ));

    assert!(matches!(
        Worker::new(
            store.clone(),
            successful_registry(&task_name),
            WorkerConfig::with_queues(Vec::new())
        ),
        Err(WorkerError::MissingQueues)
    ));

    assert!(matches!(
        Worker::new(
            store.clone(),
            successful_registry(&task_name),
            WorkerConfig::with_queues(vec![queue_name.clone(), queue_name.clone()])
        ),
        Err(WorkerError::DuplicateQueues)
    ));

    let mut config = WorkerConfig::new(queue_name.clone());
    config.lease_duration = Duration::from_millis(2);
    assert!(matches!(
        Worker::new(store.clone(), successful_registry(&task_name), config),
        Err(WorkerError::InvalidLeaseDuration)
    ));

    let mut config = WorkerConfig::new(queue_name.clone());
    config.poll_interval = Duration::ZERO;
    assert!(matches!(
        Worker::new(store.clone(), successful_registry(&task_name), config),
        Err(WorkerError::InvalidPollInterval)
    ));

    for heartbeat in [Duration::ZERO, Duration::from_micros(500), Duration::from_secs(30)] {
        let mut config = WorkerConfig::new(queue_name.clone());
        config.worker_heartbeat_interval = heartbeat;
        assert!(matches!(
            Worker::new(store.clone(), successful_registry(&task_name), config),
            Err(WorkerError::InvalidWorkerHeartbeat)
        ));
    }

    let mut config = WorkerConfig::new(queue_name.clone());
    config.schedule_reconciliation_interval = Duration::ZERO;
    assert!(matches!(
        Worker::new(store.clone(), successful_registry(&task_name), config),
        Err(WorkerError::InvalidScheduleReconciliationInterval)
    ));

    let mut config = WorkerConfig::new(queue_name.clone());
    config.retention_interval = Duration::ZERO;
    assert!(matches!(
        Worker::new(store.clone(), successful_registry(&task_name), config),
        Err(WorkerError::InvalidRetentionInterval)
    ));

    let mut config = WorkerConfig::new(queue_name.clone());
    config.supervisor_interval = Duration::ZERO;
    assert!(matches!(
        Worker::new(store.clone(), successful_registry(&task_name), config),
        Err(WorkerError::InvalidSupervisorInterval)
    ));

    let mut config = WorkerConfig::new(queue_name.clone());
    config.concurrency = NonZeroU16::new(1).unwrap();
    config.overload_protection.minimum_concurrency = NonZeroU16::new(2).unwrap();
    assert!(matches!(
        Worker::new(store.clone(), successful_registry(&task_name), config),
        Err(WorkerError::InvalidMinimumConcurrency)
    ));

    let mut config = WorkerConfig::new(queue_name);
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = QueueName::new("another-queue").unwrap();
    config.declared_schedules.push(ScheduleConfig::new(
        ScheduleName::new("invalid-worker-schedule").unwrap(),
        ScheduleDefinition::interval(Duration::from_secs(1)).unwrap(),
        request,
    ));
    assert!(matches!(
        Worker::new(store, successful_registry(&task_name), config),
        Err(WorkerError::InvalidDeclaredSchedule(_))
    ));
}

#[tokio::test]
async fn worker_rejects_an_incompatible_storage_protocol() {
    let Some(database_url) = database_url() else {
        return;
    };
    let database_name = format!("pgtask_protocol_{}", Uuid::new_v4().simple());
    let options = PgConnectOptions::from_str(&database_url).unwrap();
    let maintenance = PgPool::connect_with(options.clone().database("postgres"))
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database_name}")))
        .execute(&maintenance)
        .await
        .unwrap();
    let store = Store::from_pool(PgPool::connect_with(options.database(&database_name)).await.unwrap());
    store.migrate().await.unwrap();
    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION pgtask.storage_protocol_range()
        RETURNS TABLE(minimum integer, maximum integer)
        LANGUAGE sql
        IMMUTABLE
        AS $$ SELECT 2, 3 $$
        ",
    )
    .execute(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        store.ensure_storage_protocol(pgtask_core::STORAGE_PROTOCOL_RANGE).await,
        Err(PostgresError::IncompatibleStorageProtocol {
            database_minimum: 2,
            database_maximum: 3,
            client_minimum: pgtask_core::STORAGE_PROTOCOL_MIN_VERSION,
            client_maximum: pgtask_core::STORAGE_PROTOCOL_MAX_VERSION,
        })
    ));
    let queue_name = QueueName::new(format!("incompatible-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("incompatible-task").unwrap();
    let worker = Worker::new(store, successful_registry(&task_name), WorkerConfig::new(queue_name)).unwrap();

    assert!(matches!(
        worker.run(CancellationToken::new()).await,
        Err(WorkerError::IncompatibleStorageProtocol {
            database_minimum: 2,
            database_maximum: 3,
            worker_minimum: pgtask_core::STORAGE_PROTOCOL_MIN_VERSION,
            worker_maximum: pgtask_core::STORAGE_PROTOCOL_MAX_VERSION,
        })
    ));
    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION pgtask.storage_protocol_range()
        RETURNS TABLE(minimum integer, maximum integer)
        LANGUAGE sql
        IMMUTABLE
        AS $$ SELECT 1, 2 $$
        ",
    )
    .execute(
        &PgPool::connect_with(
            PgConnectOptions::from_str(&database_url)
                .unwrap()
                .database(&database_name),
        )
        .await
        .unwrap(),
    )
    .await
    .unwrap();
    let normal_store = Store::from_pool(
        PgPool::connect_with(
            PgConnectOptions::from_str(&database_url)
                .unwrap()
                .database(&database_name),
        )
        .await
        .unwrap(),
    );
    let queue_name = QueueName::new(format!("compatible-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("compatible-task").unwrap();
    let mut config = WorkerConfig::new(queue_name);
    config.poll_interval = Duration::from_millis(20);
    config.schedule_reconciliation_interval = Duration::from_millis(20);
    config.supervisor_interval = Duration::from_millis(20);
    config.overload_protection.enabled = false;
    let worker = Worker::new(normal_store, successful_registry(&task_name), config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    tokio::time::sleep(Duration::from_millis(60)).await;
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE {database_name} WITH (FORCE)"
    )))
    .execute(&maintenance)
    .await
    .unwrap();
}

#[tokio::test]
async fn worker_executes_registered_task_and_shuts_down() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("worker-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("echo").unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({"value": 42}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;

    let mut registry = HandlerRegistry::new();
    assert!(registry.register(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |task| async move { Ok(json!({"echo": task.payload})) }
    ));

    let mut config = WorkerConfig::new(queue_name);
    config.concurrency = NonZeroU16::new(1).unwrap();
    config.claim_batch_size = NonZeroU16::new(1).unwrap();
    config.lease_duration = Duration::from_millis(30);
    config.poll_interval = Duration::from_millis(5);
    config.retention_enabled = false;
    config.shutdown_grace = Duration::from_secs(1);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let task = store.get_task(task_id).await.unwrap().unwrap();
            if task.state == TaskState::Succeeded {
                assert_eq!(task.result, Some(json!({"echo": {"value": 42}})));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn worker_drains_earlier_queues_before_later_ones() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let high = QueueName::new(format!("multi-high-{}", Uuid::new_v4())).unwrap();
    let low = QueueName::new(format!("multi-low-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("multi-queue-order").unwrap();

    let mut task_ids = Vec::new();
    for queue_name in [&low, &high, &low, &high] {
        let mut request = EnqueueRequest::new(task_name.clone(), json!(queue_name.as_str()));
        request.queue_name = queue_name.clone();
        task_ids.push(store.enqueue(&request).await.unwrap().task_id);
    }

    let executed = Arc::new(Mutex::new(Vec::new()));
    let mut registry = HandlerRegistry::new();
    let recorded = Arc::clone(&executed);
    registry.register(task_name, HandlerVersion::default(), RetryPolicy::Never, move |task| {
        let recorded = Arc::clone(&recorded);
        async move {
            recorded.lock().await.push(task.queue_name.clone());
            Ok(json!(null))
        }
    });

    let mut config = WorkerConfig::with_queues(vec![high.clone(), low.clone()]);
    config.concurrency = NonZeroU16::new(1).unwrap();
    config.claim_batch_size = NonZeroU16::new(1).unwrap();
    // Long enough that a slow runner cannot expire a lease mid-handler and run a task twice, which
    // would say nothing about the order queues are drained in.
    config.lease_duration = TEST_TIMEOUT;
    config.poll_interval = Duration::from_millis(5);
    config.retention_enabled = false;
    config.shutdown_grace = Duration::from_secs(1);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut done = true;
            for task_id in &task_ids {
                done &= store.get_task(*task_id).await.unwrap().unwrap().state == TaskState::Succeeded;
            }
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    shutdown.cancel();
    worker_task.await.unwrap().unwrap();

    let order = executed.lock().await.clone();
    assert_eq!(order, vec![high.clone(), high, low.clone(), low]);
}

#[tokio::test]
async fn worker_records_handler_panics_without_crashing() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("panic-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("panic-task").unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;

    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { panic!("synthetic handler panic") },
    );

    let mut config = WorkerConfig::new(queue_name);
    config.lease_duration = Duration::from_millis(30);
    config.poll_interval = Duration::from_millis(5);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let task = store.get_task(task_id).await.unwrap().unwrap();
            if task.state == TaskState::Failed {
                assert_eq!(task.error, Some(json!({"type": "handler_panic"})));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn stale_success_and_panic_results_do_not_overwrite_cancellation() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("stale-results-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("stale-results-handler-{suffix}")).unwrap();
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let finished = Arc::new(Semaphore::new(0));
    let handler_started = Arc::clone(&started);
    let handler_release = Arc::clone(&release);
    let handler_finished = Arc::clone(&finished);
    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |task| {
            let started = Arc::clone(&handler_started);
            let release = Arc::clone(&handler_release);
            let finished = Arc::clone(&handler_finished);
            async move {
                started.add_permits(1);
                release.acquire().await.unwrap().forget();
                finished.add_permits(1);
                assert_ne!(task.payload, json!("panic"), "expected stale panic");
                Ok(json!({"late": true}))
            }
        },
    );
    let mut task_ids = Vec::new();
    for payload in [json!("success"), json!("panic")] {
        let mut request = EnqueueRequest::new(task_name.clone(), payload);
        request.queue_name = queue_name.clone();
        request.max_attempts = 1;
        task_ids.push(store.enqueue(&request).await.unwrap().task_id);
    }
    let mut config = WorkerConfig::new(queue_name);
    config.concurrency = NonZeroU16::new(2).unwrap();
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    started.acquire_many(2).await.unwrap().forget();
    for task_id in &task_ids {
        assert!(store.cancel(*task_id).await.unwrap());
    }
    release.add_permits(2);
    finished.acquire_many(2).await.unwrap().forget();
    tokio::time::sleep(Duration::from_millis(20)).await;
    for task_id in task_ids {
        assert_eq!(
            store.get_task(task_id).await.unwrap().unwrap().state,
            TaskState::Cancelled
        );
    }
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn lease_renewal_propagates_database_cancellation_to_the_handler() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("renewal-cancellation-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("renewal-cancellation-handler-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let handler_started = Arc::clone(&started);
    let handler_dropped = Arc::clone(&dropped);
    let mut registry = HandlerRegistry::new();
    registry.register(task_name, HandlerVersion::default(), RetryPolicy::Never, move |_task| {
        let started = Arc::clone(&handler_started);
        let dropped = DropNotification(Arc::clone(&handler_dropped));
        async move {
            started.notify_one();
            let _dropped = dropped;
            std::future::pending::<Result<serde_json::Value, HandlerError>>().await
        }
    });
    let mut config = WorkerConfig::new(queue_name);
    config.lease_duration = Duration::from_millis(90);
    config.poll_interval = Duration::from_millis(5);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, started.notified()).await.unwrap();
    assert!(store.cancel(task_id).await.unwrap());
    tokio::time::timeout(TEST_TIMEOUT, dropped.notified()).await.unwrap();
    assert_eq!(
        store.get_task(task_id).await.unwrap().unwrap().state,
        TaskState::Cancelled
    );
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn another_worker_recovers_a_task_after_runtime_termination() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("worker-crash-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("crashing-worker-task").unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;

    let started = std::sync::Arc::new(Notify::new());
    let mut first_registry = HandlerRegistry::new();
    first_registry.register(task_name.clone(), HandlerVersion::default(), RetryPolicy::Never, {
        let started = std::sync::Arc::clone(&started);
        move |_| {
            let started = std::sync::Arc::clone(&started);
            async move {
                started.notify_one();
                std::future::pending().await
            }
        }
    });
    let mut config = WorkerConfig::new(queue_name.clone());
    config.concurrency = NonZeroU16::new(1).unwrap();
    config.claim_batch_size = NonZeroU16::new(1).unwrap();
    config.lease_duration = Duration::from_secs(1);
    config.poll_interval = Duration::from_millis(5);
    let first_worker = Worker::new(store.clone(), first_registry, config.clone()).unwrap();
    let first_worker_task = tokio::spawn(async move { first_worker.run(CancellationToken::new()).await });

    tokio::time::timeout(TEST_TIMEOUT, started.notified()).await.unwrap();
    first_worker_task.abort();
    assert!(first_worker_task.await.unwrap_err().is_cancelled());

    let mut second_registry = HandlerRegistry::new();
    second_registry.register(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { Ok(json!({"recovered": true})) },
    );
    let second_worker = Worker::new(store.clone(), second_registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let second_worker_task = tokio::spawn(async move { second_worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let task = store.get_task(task_id).await.unwrap().unwrap();
            if task.state == TaskState::Succeeded {
                assert_eq!(task.attempt, 2);
                assert_eq!(task.result, Some(json!({"recovered": true})));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    shutdown.cancel();
    second_worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn workers_publish_shared_queue_demand_samples() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("shared-demand-{suffix}")).unwrap();
    let supported_name = TaskName::new(format!("shared-demand-supported-{suffix}")).unwrap();
    let missing_name = TaskName::new(format!("shared-demand-missing-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(missing_name, json!({}));
    request.queue_name = queue_name.clone();
    store.enqueue(&request).await.unwrap();
    let mut config = WorkerConfig::new(queue_name.clone());
    config.worker_heartbeat_interval = Duration::from_millis(50);
    config.worker_ttl = Duration::from_secs(1);
    let first = Worker::new(store.clone(), successful_registry(&supported_name), config.clone()).unwrap();
    let second = Worker::new(store.clone(), successful_registry(&supported_name), config).unwrap();
    let shutdown = CancellationToken::new();
    let first_shutdown = shutdown.clone();
    let second_shutdown = shutdown.clone();
    let first_task = tokio::spawn(async move { first.run(first_shutdown).await });
    let second_task = tokio::spawn(async move { second.run(second_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let sample: (bool, i64, i64) = sqlx::query_as(
                "SELECT demand_sampled_at IS NOT NULL, demand_ready_tasks, demand_unroutable_tasks \
                 FROM pgtask.queues WHERE name = $1",
            )
            .bind(queue_name.as_str())
            .fetch_one(store.pool())
            .await
            .unwrap();
            if sample == (true, 0, 1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    first_task.await.unwrap().unwrap();
    second_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn queue_runtimes_have_independent_concurrency() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let blocked_queue = QueueName::new(format!("blocked-{suffix}")).unwrap();
    let ready_queue = QueueName::new(format!("ready-{suffix}")).unwrap();
    let blocked_name = TaskName::new("blocked-task").unwrap();
    let ready_name = TaskName::new("ready-task").unwrap();
    let mut blocked_request = EnqueueRequest::new(blocked_name.clone(), json!({}));
    blocked_request.queue_name = blocked_queue.clone();
    store.enqueue(&blocked_request).await.unwrap();
    let mut ready_request = EnqueueRequest::new(ready_name.clone(), json!({}));
    ready_request.queue_name = ready_queue.clone();
    let ready_id = store.enqueue(&ready_request).await.unwrap().task_id;

    let blocked_started = std::sync::Arc::new(Notify::new());
    let mut blocked_registry = HandlerRegistry::new();
    blocked_registry.register(blocked_name, HandlerVersion::default(), RetryPolicy::Never, {
        let blocked_started = std::sync::Arc::clone(&blocked_started);
        move |_| {
            let blocked_started = std::sync::Arc::clone(&blocked_started);
            async move {
                blocked_started.notify_one();
                std::future::pending().await
            }
        }
    });
    let mut ready_registry = HandlerRegistry::new();
    ready_registry.register(
        ready_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { Ok(json!({"ran": true})) },
    );
    let mut blocked_config = WorkerConfig::new(blocked_queue);
    blocked_config.concurrency = NonZeroU16::new(1).unwrap();
    blocked_config.claim_batch_size = NonZeroU16::new(1).unwrap();
    blocked_config.lease_duration = Duration::from_millis(100);
    blocked_config.poll_interval = Duration::from_millis(5);
    let mut ready_config = WorkerConfig::new(ready_queue);
    ready_config.concurrency = NonZeroU16::new(1).unwrap();
    ready_config.claim_batch_size = NonZeroU16::new(1).unwrap();
    ready_config.lease_duration = Duration::from_millis(100);
    ready_config.poll_interval = Duration::from_millis(5);

    let blocked_worker = Worker::new(store.clone(), blocked_registry, blocked_config).unwrap();
    let blocked_task = tokio::spawn(async move { blocked_worker.run(CancellationToken::new()).await });
    let ready_worker = Worker::new(store.clone(), ready_registry, ready_config).unwrap();
    let ready_shutdown = CancellationToken::new();
    let worker_shutdown = ready_shutdown.clone();
    let ready_task = tokio::spawn(async move { ready_worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, blocked_started.notified())
        .await
        .unwrap();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if store.get_task(ready_id).await.unwrap().unwrap().state == TaskState::Succeeded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    blocked_task.abort();
    assert!(blocked_task.await.unwrap_err().is_cancelled());
    ready_shutdown.cancel();
    ready_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn worker_renews_a_long_running_handler_automatically() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("renewal-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("long-task").unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(json!({"done": true}))
        },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.concurrency = NonZeroU16::new(4).unwrap();
    config.claim_batch_size = NonZeroU16::new(4).unwrap();
    config.lease_duration = Duration::from_secs(1);
    config.poll_interval = Duration::from_millis(5);
    config.supervisor_interval = Duration::from_millis(10);
    config.overload_protection.event_loop_lag_threshold = Duration::ZERO;
    config.overload_protection.sustained_samples = NonZeroU16::MIN;
    config.overload_protection.enforce = true;
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let task = store.get_task(task_id).await.unwrap().unwrap();
            if task.state == TaskState::Succeeded {
                assert_eq!(task.attempt, 1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn effective_concurrency_stops_new_claims_without_cancelling_handlers() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("admission-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("admission-task").unwrap();
    let mut task_ids = Vec::new();
    for sequence in 0..3 {
        let mut request = EnqueueRequest::new(task_name.clone(), json!({"sequence": sequence}));
        request.queue_name = queue_name.clone();
        task_ids.push(store.enqueue(&request).await.unwrap().task_id);
    }
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let permits = Arc::new(Semaphore::new(0));
    let mut registry = HandlerRegistry::new();
    registry.register(task_name, HandlerVersion::default(), RetryPolicy::Never, {
        let active = Arc::clone(&active);
        let maximum = Arc::clone(&maximum);
        let started = Arc::clone(&started);
        let permits = Arc::clone(&permits);
        move |_| {
            let active = Arc::clone(&active);
            let maximum = Arc::clone(&maximum);
            let started = Arc::clone(&started);
            let permits = Arc::clone(&permits);
            async move {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(current, Ordering::SeqCst);
                started.fetch_add(1, Ordering::SeqCst);
                permits.acquire().await.unwrap().forget();
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(json!({"done": true}))
            }
        }
    });
    let mut config = WorkerConfig::new(queue_name);
    config.concurrency = NonZeroU16::new(2).unwrap();
    config.claim_batch_size = NonZeroU16::new(2).unwrap();
    config.lease_duration = Duration::from_secs(10);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let control = worker.control();
    assert_eq!(control.configured_concurrency().get(), 2);
    assert_eq!(control.effective_concurrency().get(), 2);
    assert!(control.set_effective_concurrency(NonZeroU16::new(3).unwrap()).is_err());
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        while started.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    control.set_effective_concurrency(NonZeroU16::new(1).unwrap()).unwrap();
    permits.add_permits(1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(started.load(Ordering::SeqCst), 2);
    assert_eq!(active.load(Ordering::SeqCst), 1);

    permits.add_permits(1);
    tokio::time::timeout(TEST_TIMEOUT, async {
        while started.load(Ordering::SeqCst) < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    permits.add_permits(1);
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut complete = true;
            for task_id in &task_ids {
                complete &= store.get_task(*task_id).await.unwrap().unwrap().state == TaskState::Succeeded;
            }
            if complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(maximum.load(Ordering::SeqCst), 2);
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn dedicated_supervisor_serves_worker_liveness_and_readiness() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let queue_name = QueueName::new(format!("health-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("health-task").unwrap();
    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { Ok(json!(null)) },
    );
    let socket = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let mut config = WorkerConfig::new(queue_name);
    config.health_address = Some(address);
    config.supervisor_interval = Duration::from_millis(10);
    config.overload_protection.enabled = false;
    let worker = Worker::new(store, registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if TcpStream::connect(address).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(health_status(address, "/livez").await, 200);
    tokio::time::timeout(TEST_TIMEOUT, async {
        while health_status(address, "/readyz").await != 200 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn supervisor_binding_failure_is_reported() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let queue_name = QueueName::new(format!("health-conflict-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("health-conflict-task").unwrap();
    let socket = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let mut config = WorkerConfig::new(queue_name);
    config.health_address = Some(socket.local_addr().unwrap());
    let worker = Worker::new(store, successful_registry(&task_name), config).unwrap();

    assert!(matches!(
        worker.run(CancellationToken::new()).await,
        Err(WorkerError::Supervisor(_))
    ));
}

#[tokio::test]
async fn worker_recovers_from_revoked_database_protocols() {
    let Some(database_url) = database_url() else {
        return;
    };
    let _guard = database_fault_guard().await;
    let fixture = DatabaseFaultWorker::start(&database_url).await;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "REVOKE EXECUTE ON FUNCTION pgtask.complete_task(uuid, integer, uuid, jsonb) FROM {}",
        fixture.role
    )))
    .execute(fixture.admin.pool())
    .await
    .unwrap();
    fixture.release.add_permits(1);
    tokio::time::sleep(Duration::from_millis(80)).await;
    fixture.restore_grants().await;

    let revoke_background = format!(
        "REVOKE EXECUTE ON FUNCTION \
         pgtask.renew_leases(uuid[], integer[], uuid[], bigint), \
         pgtask.heartbeat_worker(uuid, bigint, boolean), \
         pgtask.heartbeat_worker_with_sampling(uuid, bigint, boolean, bigint), \
         pgtask.claim_due_schedules(integer), \
         pgtask.recover_wait_timeouts(integer) FROM {}",
        fixture.role
    );
    sqlx::query(sqlx::AssertSqlSafe(revoke_background))
        .execute(fixture.admin.pool())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    fixture.restore_grants().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    fixture.admin.recover_expired(&fixture.fault_queue, 10).await.unwrap();

    for (function, delay) in [
        ("pgtask.delete_expired_terminal(text, integer)", 150),
        ("pgtask.delete_expired_idempotency_keys(text, integer)", 150),
        ("pgtask.recover_expired(text, integer)", 150),
        ("pgtask.claim(text, uuid, text[], integer[], integer, bigint)", 150),
        ("pgtask.next_task_delay_milliseconds(text, text[], integer[])", 150),
        ("pgtask.next_schedule_delay_milliseconds()", 100),
        ("pgtask.next_wait_delay_milliseconds()", 100),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "REVOKE EXECUTE ON FUNCTION {function} FROM {}",
            fixture.role
        )))
        .execute(fixture.admin.pool())
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(delay)).await;
        fixture.restore_grants().await;
    }

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "REVOKE EXECUTE ON FUNCTION pgtask.heartbeat_worker(uuid, bigint, boolean) FROM {}",
        fixture.role
    )))
    .execute(fixture.admin.pool())
    .await
    .unwrap();
    fixture.stop().await;
}

#[tokio::test]
async fn worker_refuses_a_schema_without_demand_sampling() {
    let Some(database_url) = database_url() else {
        return;
    };
    let database_name = format!("pgtask_outdated_{}", Uuid::new_v4().simple());
    let options = PgConnectOptions::from_str(&database_url).unwrap();
    let maintenance = PgPool::connect_with(options.clone().database("postgres"))
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database_name}")))
        .execute(&maintenance)
        .await
        .unwrap();
    let store = Store::from_pool(PgPool::connect_with(options.database(&database_name)).await.unwrap());
    store.migrate().await.unwrap();
    sqlx::query("DROP FUNCTION pgtask.heartbeat_worker_with_sampling(uuid, bigint, boolean, bigint)")
        .execute(store.pool())
        .await
        .unwrap();
    let queue_name = QueueName::new(format!("outdated-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("outdated-task").unwrap();
    let worker = Worker::new(store, successful_registry(&task_name), WorkerConfig::new(queue_name)).unwrap();
    assert!(matches!(
        worker.run(CancellationToken::new()).await,
        Err(WorkerError::OutdatedStorageSchema)
    ));
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE {database_name} WITH (FORCE)"
    )))
    .execute(&maintenance)
    .await
    .unwrap();
}

#[tokio::test]
async fn worker_recovers_from_database_disconnects_and_registration_loss() {
    let Some(database_url) = database_url() else {
        return;
    };
    let _guard = database_fault_guard().await;
    let fixture = DatabaseFaultWorker::start(&database_url).await;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER ROLE {} CONNECTION LIMIT 0",
        fixture.role
    )))
    .execute(fixture.admin.pool())
    .await
    .unwrap();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1")
        .bind(&fixture.role)
        .execute(fixture.admin.pool())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER ROLE {} CONNECTION LIMIT -1",
        fixture.role
    )))
    .execute(fixture.admin.pool())
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    sqlx::query("DELETE FROM pgtask.workers WHERE queue_name = $1")
        .bind(fixture.fault_queue.as_str())
        .execute(fixture.admin.pool())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    fixture.stop().await;
}

#[tokio::test]
async fn notification_listener_reports_failure_and_recovers() {
    let Some(database_url) = database_url() else {
        return;
    };
    let _guard = database_fault_guard().await;
    let admin = Store::connect(&database_url).await.unwrap();
    admin.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple();
    let role = format!("pgtask_listener_{suffix}");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'listener-test'"
    )))
    .execute(admin.pool())
    .await
    .unwrap();
    let owner: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(admin.pool())
        .await
        .unwrap();
    admin
        .configure_grants(&owner, &role, &role, &role, &role)
        .await
        .unwrap();
    let options = PgConnectOptions::from_str(&database_url)
        .unwrap()
        .username(&role)
        .password("listener-test")
        .application_name(&role);
    let store = Store::from_pool(
        PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(1))
            .connect_with(options)
            .await
            .unwrap(),
    );
    let queue_name = QueueName::new(format!("listener-recovery-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("listener-recovery-handler-{suffix}")).unwrap();
    let socket = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let mut config = WorkerConfig::new(queue_name);
    config.health_address = Some(address);
    config.poll_interval = Duration::from_secs(5);
    config.supervisor_interval = Duration::from_millis(10);
    let worker = Worker::new(store, successful_registry(&task_name), config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        while TcpStream::connect(address).await.is_err() {}
    })
    .await
    .unwrap();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while health_status(address, "/readyz").await != 200 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    sqlx::query("SELECT pg_notify('pgtask_ready', 'another-queue')")
        .execute(admin.pool())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    sqlx::query(sqlx::AssertSqlSafe(format!("ALTER ROLE {role} CONNECTION LIMIT 0")))
        .execute(admin.pool())
        .await
        .unwrap();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1")
        .bind(&role)
        .execute(admin.pool())
        .await
        .unwrap();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while health_status(address, "/readyz").await != 503 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    sqlx::query(sqlx::AssertSqlSafe(format!("ALTER ROLE {role} CONNECTION LIMIT -1")))
        .execute(admin.pool())
        .await
        .unwrap();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while health_status(address, "/readyz").await != 200 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
    drop_runtime_role(&admin, &role).await;
}

#[tokio::test]
async fn idle_worker_recovers_from_claim_and_deadline_protocol_failures() {
    let Some(database_url) = database_url() else {
        return;
    };
    let _guard = database_fault_guard().await;
    let admin = Store::connect(&database_url).await.unwrap();
    admin.migrate().await.unwrap();
    let suffix = Uuid::new_v4().simple();
    let role = format!("pgtask_fault_{suffix}");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'fault-test'"
    )))
    .execute(admin.pool())
    .await
    .unwrap();
    let owner: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(admin.pool())
        .await
        .unwrap();
    admin
        .configure_grants(&owner, &role, &role, &role, &role)
        .await
        .unwrap();
    let options = PgConnectOptions::from_str(&database_url)
        .unwrap()
        .username(&role)
        .password("fault-test")
        .application_name(&role);
    let store = Store::from_pool(PgPool::connect_with(options).await.unwrap());
    let queue_name = QueueName::new(format!("idle-database-fault-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("idle-database-fault-task-{suffix}")).unwrap();
    let socket = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let mut config = WorkerConfig::new(queue_name.clone());
    config.health_address = Some(address);
    config.poll_interval = Duration::from_millis(20);
    config.supervisor_interval = Duration::from_millis(2);
    config.overload_protection.enabled = false;
    let worker = Worker::new(store, successful_registry(&task_name), config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    tokio::time::timeout(TEST_TIMEOUT, async {
        while TcpStream::connect(address).await.is_err() {}
    })
    .await
    .unwrap();

    for function in [
        "pgtask.next_task_delay_milliseconds(text, text[], integer[])",
        "pgtask.claim(text, uuid, text[], integer[], integer, bigint)",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "REVOKE EXECUTE ON FUNCTION {function} FROM {role}"
        )))
        .execute(admin.pool())
        .await
        .unwrap();
        let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
        request.queue_name = queue_name.clone();
        admin.enqueue(&request).await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        admin
            .configure_grants(&owner, &role, &role, &role, &role)
            .await
            .unwrap();
        tokio::time::timeout(TEST_TIMEOUT, async {
            while health_status(address, "/readyz").await != 200 {}
        })
        .await
        .unwrap();
    }

    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP OWNED BY {role}")))
        .execute(admin.pool())
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP ROLE {role}")))
        .execute(admin.pool())
        .await
        .unwrap();
}

#[tokio::test]
async fn supervisor_proposes_overload_reduction_without_enforcing_it() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let queue_name = QueueName::new(format!("observe-overload-{}", Uuid::new_v4())).unwrap();
    let mut registry = HandlerRegistry::new();
    registry.register(
        TaskName::new("observe-overload-task").unwrap(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { Ok(json!(null)) },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.concurrency = NonZeroU16::new(4).unwrap();
    config.supervisor_interval = Duration::from_millis(10);
    config.overload_protection.event_loop_lag_threshold = Duration::ZERO;
    config.overload_protection.sustained_samples = NonZeroU16::MIN;
    let worker = Worker::new(store, registry, config).unwrap();
    let control = worker.control();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        while control.proposed_concurrency().get() != 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(control.effective_concurrency().get(), 4);
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn optional_overload_enforcement_reduces_the_effective_limit() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let queue_name = QueueName::new(format!("enforce-overload-{}", Uuid::new_v4())).unwrap();
    let mut registry = HandlerRegistry::new();
    registry.register(
        TaskName::new("enforce-overload-task").unwrap(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { Ok(json!(null)) },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.concurrency = NonZeroU16::new(4).unwrap();
    config.supervisor_interval = Duration::from_millis(10);
    config.overload_protection.event_loop_lag_threshold = Duration::ZERO;
    config.overload_protection.sustained_samples = NonZeroU16::MIN;
    config.overload_protection.enforce = true;
    let worker = Worker::new(store, registry, config).unwrap();
    let control = worker.control();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        while control.effective_concurrency().get() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(control.proposed_concurrency().get(), 1);
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn overload_enforcement_recovers_additively_to_the_configured_limit() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let queue_name = QueueName::new(format!("recover-overload-{}", Uuid::new_v4())).unwrap();
    let mut registry = HandlerRegistry::new();
    registry.register(
        TaskName::new("recover-overload-task").unwrap(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { Ok(json!(null)) },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.concurrency = NonZeroU16::new(4).unwrap();
    config.supervisor_interval = Duration::from_millis(10);
    config.overload_protection.event_loop_lag_threshold = Duration::MAX;
    config.overload_protection.recovery_samples = NonZeroU16::new(3).unwrap();
    config.overload_protection.enforce = true;
    let worker = Worker::new(store, registry, config).unwrap();
    let control = worker.control();
    control.set_effective_concurrency(NonZeroU16::MIN).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(control.effective_concurrency().get(), 1);

    tokio::time::timeout(TEST_TIMEOUT, async {
        while control.effective_concurrency().get() != 4 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(control.proposed_concurrency().get(), 4);
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn notification_and_database_deadline_wake_a_delayed_task() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("notify-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("notified-task").unwrap();
    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { Ok(json!({})) },
    );
    let mut config = WorkerConfig::new(queue_name.clone());
    config.poll_interval = Duration::from_secs(5);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut request = EnqueueRequest::new(task_name, json!({}));
    request.queue_name = queue_name;
    request.run_at = Some(Utc::now() + TimeDelta::milliseconds(150));
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if store.get_task(task_id).await.unwrap().unwrap().state == TaskState::Succeeded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn schedule_notifications_reset_the_database_deadline_timer() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("schedule-notify-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("scheduled-handler-{suffix}")).unwrap();
    let executed = std::sync::Arc::new(Notify::new());
    let handler_executed = std::sync::Arc::clone(&executed);
    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |_| {
            let handler_executed = std::sync::Arc::clone(&handler_executed);
            async move {
                handler_executed.notify_one();
                Ok(json!({}))
            }
        },
    );
    let mut worker_config = WorkerConfig::new(queue_name.clone());
    worker_config.poll_interval = Duration::from_secs(5);
    worker_config.schedule_reconciliation_interval = Duration::from_secs(5);
    let worker = Worker::new(store.clone(), registry, worker_config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut request = EnqueueRequest::new(task_name, json!({}));
    request.queue_name = queue_name;
    let mut schedule_config = ScheduleConfig::new(
        ScheduleName::new(format!("schedule-notify-{suffix}")).unwrap(),
        ScheduleDefinition::interval(Duration::from_hours(1)).unwrap(),
        request,
    );
    schedule_config.start_at = Some(Utc::now() + TimeDelta::milliseconds(150));
    let schedule = store.put_schedule(&schedule_config).await.unwrap();

    tokio::time::timeout(TEST_TIMEOUT, executed.notified()).await.unwrap();
    assert!(store.delete_schedule(schedule.config.id).await.unwrap());
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn worker_reconciles_code_declared_schedules_before_starting() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("declared-schedule-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("declared-handler-{suffix}")).unwrap();
    let executed = std::sync::Arc::new(Notify::new());
    let handler_executed = std::sync::Arc::clone(&executed);
    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |_| {
            let handler_executed = std::sync::Arc::clone(&handler_executed);
            async move {
                handler_executed.notify_one();
                Ok(json!({}))
            }
        },
    );
    let mut request = EnqueueRequest::new(task_name, json!({}));
    request.queue_name = queue_name.clone();
    let mut schedule_config = ScheduleConfig::new(
        ScheduleName::new(format!("declared-schedule-{suffix}")).unwrap(),
        ScheduleDefinition::interval(Duration::from_hours(1)).unwrap(),
        request,
    );
    schedule_config.start_at = Some(Utc::now() + TimeDelta::milliseconds(100));
    let schedule_id = schedule_config.id;
    let mut worker_config = WorkerConfig::new(queue_name);
    worker_config.declared_schedules.push(schedule_config);
    let worker = Worker::new(store.clone(), registry, worker_config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, executed.notified()).await.unwrap();
    assert!(store.get_schedule(schedule_id).await.unwrap().is_some());
    assert!(store.delete_schedule(schedule_id).await.unwrap());
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn replicated_workers_delete_expired_terminal_tasks_in_bounded_batches() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("worker-retention-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("worker-retention-{suffix}")).unwrap();
    let mut queue = QueueConfig::new(queue_name.clone());
    queue.terminal_retention = Duration::ZERO;
    queue.idempotency_retention = Duration::ZERO;
    store.put_queue(&queue).await.unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    request.idempotency_key = Some(format!("worker-retention-{suffix}"));
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let task = store
        .claim(
            &queue_name,
            pgtask_core::WorkerId::new(),
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
            .complete(task_id, task.attempt, task.lease_token.unwrap(), None)
            .await
            .unwrap()
    );

    let mut config = WorkerConfig::new(queue_name);
    config.retention_batch_size = NonZeroU16::MIN;
    config.retention_interval = Duration::from_millis(10);
    let worker = Worker::new(store.clone(), successful_registry(&task_name), config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let task_exists = store.get_task(task_id).await.unwrap().is_some();
            let key_exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pgtask.idempotency_keys WHERE task_id = $1)")
                    .bind(task_id.as_uuid())
                    .fetch_one(store.pool())
                    .await
                    .unwrap();
            if !task_exists && !key_exists {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn durable_handler_replays_a_checkpoint_after_retry() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("durable-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("durable-handler-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let operations = std::sync::Arc::new(AtomicUsize::new(0));
    let handler_operations = std::sync::Arc::clone(&operations);
    let mut registry = HandlerRegistry::new();
    registry.register_durable(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Fixed {
            delay: Duration::from_millis(1),
        },
        move |task, context| {
            let handler_operations = std::sync::Arc::clone(&handler_operations);
            async move {
                assert!(!context.cancellation_token().is_cancelled());
                let value = context
                    .step(&StepName::new("load-value").unwrap(), 0, || async move {
                        handler_operations.fetch_add(1, Ordering::SeqCst);
                        Ok(json!({"value": 42}))
                    })
                    .await?;
                if task.attempt == 1 {
                    return Err(pgtask_worker::HandlerError::retryable("retry after checkpoint"));
                }
                Ok(value)
            }
        },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.poll_interval = Duration::from_millis(50);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let task = store.get_task(task_id).await.unwrap().unwrap();
            if task.state == TaskState::Succeeded {
                assert_eq!(task.attempt, 2);
                assert_eq!(task.result, Some(json!({"value": 42})));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(operations.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn durable_context_rejects_cancelled_and_malformed_checkpoints() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("durable-validation-{suffix}")).unwrap();
    let cancelled_name = TaskName::new(format!("durable-cancelled-{suffix}")).unwrap();
    let malformed_name = TaskName::new(format!("durable-malformed-{suffix}")).unwrap();

    let mut registry = HandlerRegistry::new();
    registry.register_durable(
        cancelled_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_task, context| async move {
            context.cancellation_token().cancel();
            context
                .step(&StepName::new("cancelled-step").unwrap(), 0, || async {
                    Ok(json!(null))
                })
                .await
        },
    );
    registry.register_durable(
        malformed_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        |task, context| async move {
            let step_name = StepName::new("malformed-signal").unwrap();
            context.step(&step_name, 0, || async { Ok(task.payload) }).await?;
            context
                .wait_for_signal(&step_name, 0, &SignalName::new("unused").unwrap(), 0, None)
                .await
                .map(|_| json!(null))
        },
    );

    let mut task_ids = Vec::new();
    for (task_name, payload) in [
        (cancelled_name, json!(null)),
        (malformed_name.clone(), json!("invalid")),
        (malformed_name, json!({"outcome": "invalid"})),
    ] {
        let mut request = EnqueueRequest::new(task_name, payload);
        request.queue_name = queue_name.clone();
        request.max_attempts = 1;
        task_ids.push(store.enqueue(&request).await.unwrap().task_id);
    }

    let mut config = WorkerConfig::new(queue_name);
    config.concurrency = NonZeroU16::new(3).unwrap();
    config.lease_duration = Duration::from_millis(30);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let mut terminal = true;
            for task_id in &task_ids {
                terminal &= store.get_task(*task_id).await.unwrap().unwrap().state.is_terminal();
            }
            if terminal {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn external_side_effect_before_checkpoint_commit_is_at_least_once() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("durable-side-effect-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("durable-side-effect-handler-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let operations = std::sync::Arc::new(AtomicUsize::new(0));
    let handler_operations = std::sync::Arc::clone(&operations);
    let mut registry = HandlerRegistry::new();
    registry.register_durable(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Fixed {
            delay: Duration::from_millis(1),
        },
        move |task, context| {
            let handler_operations = std::sync::Arc::clone(&handler_operations);
            async move {
                context
                    .step(&StepName::new("external-effect").unwrap(), 0, || async move {
                        handler_operations.fetch_add(1, Ordering::SeqCst);
                        if task.attempt == 1 {
                            return Err(pgtask_worker::HandlerError::retryable("lost before checkpoint commit"));
                        }
                        Ok(json!({"committed": true}))
                    })
                    .await
            }
        },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.poll_interval = Duration::from_secs(5);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if store.get_task(task_id).await.unwrap().unwrap().state == TaskState::Succeeded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(operations.load(Ordering::SeqCst), 2);
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn durable_sleep_variants_release_the_worker_and_resume_once() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("durable-sleep-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("durable-sleep-handler-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let mut registry = HandlerRegistry::new();
    registry.register_durable(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_task, context| async move {
            assert!(!context.cancellation_token().is_cancelled());
            context
                .sleep_for(&StepName::new("short-sleep").unwrap(), 0, Duration::from_millis(50))
                .await?;
            context
                .sleep_until(
                    &StepName::new("short-sleep-until").unwrap(),
                    0,
                    Utc::now() + TimeDelta::milliseconds(50),
                )
                .await?;
            Ok(json!({"resumed": true}))
        },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.poll_interval = Duration::from_secs(5);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let started_at = std::time::Instant::now();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let task = store.get_task(task_id).await.unwrap().unwrap();
            if task.state == TaskState::Succeeded {
                assert_eq!(task.attempt, 3);
                assert_eq!(task.result, Some(json!({"resumed": true})));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(started_at.elapsed() >= Duration::from_millis(80));
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn durable_signal_wait_closes_the_lost_wakeup_race() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("durable-signal-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("durable-signal-handler-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let mut registry = HandlerRegistry::new();
    registry.register_durable(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_task, context| async move {
            let signal = context
                .wait_for_signal(
                    &StepName::new("approval-wait").unwrap(),
                    0,
                    &SignalName::new("approval").unwrap(),
                    0,
                    None,
                )
                .await?;
            Ok(json!({"signal": signal}))
        },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.poll_interval = Duration::from_secs(5);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if store.get_task(task_id).await.unwrap().unwrap().state == TaskState::Waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    store
        .emit_signal(
            task_id,
            &SignalName::new("approval").unwrap(),
            0,
            &json!({"approved": true}),
        )
        .await
        .unwrap();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let task = store.get_task(task_id).await.unwrap().unwrap();
            if task.state == TaskState::Succeeded {
                assert_eq!(task.attempt, 2);
                assert_eq!(task.result, Some(json!({"signal": {"approved": true}})));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn durable_signal_available_before_execution_completes_without_suspending() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("durable-ready-signal-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("durable-ready-signal-handler-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    store
        .emit_signal(
            task_id,
            &SignalName::new("approval").unwrap(),
            0,
            &json!({"approved": true}),
        )
        .await
        .unwrap();

    let mut registry = HandlerRegistry::new();
    registry.register_durable(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_task, context| async move {
            context
                .wait_for_signal(
                    &StepName::new("ready-approval").unwrap(),
                    0,
                    &SignalName::new("approval").unwrap(),
                    0,
                    None,
                )
                .await
                .map(|signal| signal.unwrap_or(json!(null)))
        },
    );
    let worker = Worker::new(store.clone(), registry, WorkerConfig::new(queue_name)).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let task = store.get_task(task_id).await.unwrap().unwrap();
            if task.state == TaskState::Succeeded {
                assert_eq!(task.attempt, 1);
                assert_eq!(task.result, Some(json!({"approved": true})));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn worker_accepts_shutdown_before_runtime_loops_start() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("pre-cancelled-worker-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("pre-cancelled-handler-{suffix}")).unwrap();
    let worker = Worker::new(store, successful_registry(&task_name), WorkerConfig::new(queue_name)).unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    worker.run(shutdown).await.unwrap();
}

#[tokio::test]
async fn durable_signal_timeout_uses_the_database_deadline() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("durable-signal-timeout-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("durable-signal-timeout-handler-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let mut registry = HandlerRegistry::new();
    registry.register_durable(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_task, context| async move {
            let signal = context
                .wait_for_signal(
                    &StepName::new("timeout-wait").unwrap(),
                    0,
                    &SignalName::new("never").unwrap(),
                    0,
                    Some(Duration::from_millis(50)),
                )
                .await?;
            Ok(json!({"signal": signal}))
        },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.poll_interval = Duration::from_secs(5);
    config.schedule_reconciliation_interval = Duration::from_secs(5);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let task = store.get_task(task_id).await.unwrap().unwrap();
            if task.state == TaskState::Succeeded {
                assert_eq!(task.attempt, 2);
                assert_eq!(task.result, Some(json!({"signal": null})));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn durable_signal_wait_rejects_a_stale_parent_lease() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("durable-stale-signal-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("durable-stale-signal-handler-{suffix}")).unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let handler_started = Arc::clone(&started);
    let handler_release = Arc::clone(&release);
    let mut registry = HandlerRegistry::new();
    registry.register_durable(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |_task, context| {
            let started = Arc::clone(&handler_started);
            let release = Arc::clone(&handler_release);
            async move {
                started.notify_one();
                release.acquire().await.unwrap().forget();
                context
                    .wait_for_signal(
                        &StepName::new("stale-signal").unwrap(),
                        0,
                        &SignalName::new("unused").unwrap(),
                        0,
                        None,
                    )
                    .await
                    .map(|_| json!(null))
            }
        },
    );
    let mut request = EnqueueRequest::new(task_name, json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let worker = Worker::new(store.clone(), registry, WorkerConfig::new(queue_name)).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, started.notified()).await.unwrap();
    assert!(store.cancel(task_id).await.unwrap());
    release.add_permits(1);
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if store.get_task(task_id).await.unwrap().unwrap().state == TaskState::Cancelled {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn shutdown_aborts_handlers_after_the_grace_period() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("shutdown-grace-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("shutdown-grace-handler-{suffix}")).unwrap();
    let started = Arc::new(Notify::new());
    let handler_started = Arc::clone(&started);
    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |_task| {
            let started = Arc::clone(&handler_started);
            async move {
                started.notify_one();
                std::future::pending().await
            }
        },
    );
    let mut request = EnqueueRequest::new(task_name, json!({}));
    request.queue_name = queue_name.clone();
    request.max_attempts = 1;
    store.enqueue(&request).await.unwrap();
    let mut config = WorkerConfig::new(queue_name.clone());
    config.lease_duration = Duration::from_millis(30);
    config.shutdown_grace = Duration::from_millis(20);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, started.notified()).await.unwrap();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), worker_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    store.recover_expired(&queue_name, 1).await.unwrap();
}

#[tokio::test]
async fn shutdown_drains_handlers_that_finish_within_the_grace_period() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("shutdown-drain-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("shutdown-drain-handler-{suffix}")).unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let handler_started = Arc::clone(&started);
    let handler_release = Arc::clone(&release);
    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |_task| {
            let started = Arc::clone(&handler_started);
            let release = Arc::clone(&handler_release);
            async move {
                started.notify_one();
                release.acquire().await.unwrap().forget();
                Ok(json!({"drained": true}))
            }
        },
    );
    let mut request = EnqueueRequest::new(task_name, json!({}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let worker = Worker::new(store.clone(), registry, WorkerConfig::new(queue_name)).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, started.notified()).await.unwrap();
    shutdown.cancel();
    release.add_permits(1);
    worker_task.await.unwrap().unwrap();
    let task = store.get_task(task_id).await.unwrap().unwrap();
    assert_eq!(task.state, TaskState::Succeeded);
    assert_eq!(task.result, Some(json!({"drained": true})));
}

#[tokio::test]
async fn durable_result_wait_releases_the_worker_until_the_child_finishes() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("durable-result-{suffix}")).unwrap();
    let parent_name = TaskName::new(format!("durable-result-parent-{suffix}")).unwrap();
    let child_name = TaskName::new(format!("durable-result-child-{suffix}")).unwrap();
    let mut parent_request = EnqueueRequest::new(parent_name.clone(), json!({}));
    parent_request.queue_name = queue_name.clone();
    let parent_id = store.enqueue(&parent_request).await.unwrap().task_id;

    let mut registry = HandlerRegistry::new();
    let child_name_for_parent = child_name.clone();
    let queue_name_for_parent = queue_name.clone();
    registry.register_durable(
        parent_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |_task, context| {
            let child_name = child_name_for_parent.clone();
            let queue_name = queue_name_for_parent.clone();
            async move {
                let mut child_request = EnqueueRequest::new(child_name, json!({}));
                child_request.queue_name = queue_name;
                let child_id = context
                    .spawn(&StepName::new("spawn-child").unwrap(), 0, &child_request)
                    .await?;
                context
                    .wait_for_result(&StepName::new("child-result").unwrap(), 0, child_id, None)
                    .await
            }
        },
    );
    registry.register(
        child_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_task| async { Ok(json!({"child": "finished"})) },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.concurrency = NonZeroU16::new(1).unwrap();
    config.poll_interval = Duration::from_secs(5);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let parent = store.get_task(parent_id).await.unwrap().unwrap();
            if parent.state == TaskState::Succeeded {
                assert_eq!(parent.attempt, 2);
                assert_eq!(
                    parent.result,
                    Some(json!({
                        "state": "succeeded",
                        "result": {"child": "finished"},
                        "error": null
                    }))
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn durable_result_wait_handles_ready_and_stale_parents() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("durable-ready-result-{suffix}")).unwrap();
    // Each parent spawns a child under its own name. Sharing one name lets complete_ready_child
    // claim the other parent's child, leaving the ready parent waiting on a child nobody finishes.
    let child_name = TaskName::new(format!("durable-ready-child-{suffix}")).unwrap();
    let stale_child_name = TaskName::new(format!("durable-stale-child-{suffix}")).unwrap();
    let parent_name = TaskName::new(format!("durable-ready-parent-{suffix}")).unwrap();
    let stale_name = TaskName::new(format!("durable-stale-parent-{suffix}")).unwrap();

    let mut child_request = EnqueueRequest::new(child_name.clone(), json!({}));
    child_request.queue_name = queue_name.clone();
    let mut stale_child_request = EnqueueRequest::new(stale_child_name, json!({}));
    stale_child_request.queue_name = queue_name.clone();

    let mut registry = HandlerRegistry::new();
    let ready_store = store.clone();
    let ready_child_name = child_name.clone();
    let ready_child_request = child_request.clone();
    registry.register_durable(
        parent_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |_task, context| {
            let store = ready_store.clone();
            let child_name = ready_child_name.clone();
            let child_request = ready_child_request.clone();
            async move {
                let child_id = context
                    .spawn(&StepName::new("spawn-ready").unwrap(), 0, &child_request)
                    .await?;
                complete_ready_child(&store, &child_request.queue_name, &child_name).await?;
                context
                    .wait_for_result(&StepName::new("already-ready").unwrap(), 0, child_id, None)
                    .await
            }
        },
    );
    let started = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let handler_started = Arc::clone(&started);
    let handler_release = Arc::clone(&release);
    registry.register_durable(
        stale_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |_task, context| {
            let started = Arc::clone(&handler_started);
            let release = Arc::clone(&handler_release);
            let child_request = stale_child_request.clone();
            async move {
                let child_id = context
                    .spawn(&StepName::new("spawn-stale").unwrap(), 0, &child_request)
                    .await?;
                started.notify_one();
                release.acquire().await.unwrap().forget();
                context
                    .wait_for_result(&StepName::new("stale-result").unwrap(), 0, child_id, None)
                    .await
            }
        },
    );

    let mut parent_request = EnqueueRequest::new(parent_name, json!({}));
    parent_request.queue_name = queue_name.clone();
    let parent_id = store.enqueue(&parent_request).await.unwrap().task_id;
    let mut stale_request = EnqueueRequest::new(stale_name, json!({}));
    stale_request.queue_name = queue_name.clone();
    let stale_id = store.enqueue(&stale_request).await.unwrap().task_id;
    let mut config = WorkerConfig::new(queue_name);
    config.concurrency = NonZeroU16::new(2).unwrap();
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });

    tokio::time::timeout(TEST_TIMEOUT, started.notified()).await.unwrap();
    assert!(store.cancel(stale_id).await.unwrap());
    release.add_permits(1);
    let expected = json!({"state": "succeeded", "result": {"ready": true}, "error": null});
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let parent = store.get_task(parent_id).await.unwrap().unwrap();
            let stale = store.get_task(stale_id).await.unwrap().unwrap();
            if parent.state == TaskState::Succeeded && stale.state == TaskState::Cancelled {
                assert_eq!(parent.result, Some(expected));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn worker_refuses_to_start_against_an_unmigrated_database() {
    let Some(database_url) = database_url() else {
        return;
    };
    let database_name = format!("pgtask_unmigrated_{}", Uuid::new_v4().simple());
    let options = PgConnectOptions::from_str(&database_url).unwrap();
    let maintenance = PgPool::connect_with(options.clone().database("postgres"))
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database_name}")))
        .execute(&maintenance)
        .await
        .unwrap();

    // Deliberately not migrated: this is a worker that won the race against its migration Job.
    let store = Store::from_pool(PgPool::connect_with(options.database(&database_name)).await.unwrap());
    let task_name = TaskName::new("unmigrated-task").unwrap();
    let queue_name = QueueName::new(format!("unmigrated-{}", Uuid::new_v4())).unwrap();
    let worker = Worker::new(store, successful_registry(&task_name), WorkerConfig::new(queue_name)).unwrap();

    // It must fail rather than idle, because a deployment orders the migration before the workers
    // and a silent worker would look healthy while doing nothing.
    let error = tokio::time::timeout(TEST_TIMEOUT, worker.run(CancellationToken::new()))
        .await
        .expect("worker idled instead of failing")
        .expect_err("worker started without a schema");
    let message = error.to_string();
    assert!(
        message.contains("schema \"pgtask\" does not exist"),
        "the error should say the schema is missing rather than anything vaguer: {message}"
    );

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE {database_name} WITH (FORCE)"
    )))
    .execute(&maintenance)
    .await
    .unwrap();
}
