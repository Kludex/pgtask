use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use pgtask_core::{
    Checkpoint, EnqueueRequest, EnqueueResult, HandlerVersion, LeaseRenewal, LeaseToken, MisfirePolicy, Queue,
    QueueConfig, QueueName, RetryPolicy, Schedule, ScheduleConfig, ScheduleDefinition, ScheduleError, ScheduleId,
    ScheduleName, Signal, SignalName, StepName, StorageProtocolRange, Task, TaskId, TaskName, TaskResult, TaskState,
    WorkerId, WorkerRecord,
};
use serde_json::{Value, json};
use sqlx::{
    FromRow, PgConnection, PgPool,
    postgres::{PgListener, PgPoolOptions},
};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{Instrument, info_span};
use uuid::Uuid;

static MIGRATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const UNDEFINED_SCHEMA: &str = "3F000";

#[derive(Debug, Error)]
pub enum PostgresError {
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("database migration failed: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("invalid task data returned by Postgres: {0}")]
    InvalidTask(String),
    #[error("max_attempts must be greater than zero")]
    InvalidMaxAttempts,
    #[error("claim limit must be greater than zero")]
    InvalidClaimLimit,
    #[error("retention limit must be greater than zero")]
    InvalidRetentionLimit,
    #[error("lease duration must be greater than zero")]
    InvalidLeaseDuration,
    #[error("at least one handler capability is required")]
    MissingCapabilities,
    #[error("at least one queue is required")]
    MissingQueues,
    #[error("handler version exceeds the Postgres integer range")]
    InvalidHandlerVersion,
    #[error("schedule claim limit must be greater than zero")]
    InvalidScheduleLimit,
    #[error("sleep duration exceeds the PostgreSQL bigint range")]
    InvalidSleepDuration,
    #[error("wait recovery limit must be greater than zero")]
    InvalidWaitLimit,
    #[error("result wait timeout must be greater than zero and fit in the PostgreSQL bigint range")]
    InvalidResultWaitTimeout,
    #[error("retry policy values exceed the PostgreSQL integer range")]
    InvalidRetryPolicy,
    #[error("notification listener failed: {0}")]
    Notification(String),
    #[error("invalid storage protocol range {minimum}..={maximum} returned by Postgres")]
    InvalidStorageProtocolRange { minimum: i32, maximum: i32 },
    #[error(
        "database storage protocols {database_minimum}..={database_maximum} are incompatible with client protocols {client_minimum}..={client_maximum}"
    )]
    IncompatibleStorageProtocol {
        database_minimum: u32,
        database_maximum: u32,
        client_minimum: u32,
        client_maximum: u32,
    },
    #[error(transparent)]
    Schedule(#[from] ScheduleError),
}

#[derive(Clone)]
pub struct StoreConfig {
    database_url: String,
    listener_url: String,
    query_connections: NonZeroU32,
    listener_connections: NonZeroU32,
}

impl StoreConfig {
    pub fn new(database_url: impl Into<String>) -> Self {
        let database_url = database_url.into();
        Self {
            listener_url: database_url.clone(),
            database_url,
            query_connections: NonZeroU32::new(10).expect("10 is nonzero"),
            listener_connections: NonZeroU32::MIN,
        }
    }

    #[must_use]
    pub fn with_listener_url(mut self, listener_url: impl Into<String>) -> Self {
        self.listener_url = listener_url.into();
        self
    }

    #[must_use]
    pub const fn with_query_connections(mut self, connections: NonZeroU32) -> Self {
        self.query_connections = connections;
        self
    }

    #[must_use]
    pub const fn with_listener_connections(mut self, connections: NonZeroU32) -> Self {
        self.listener_connections = connections;
        self
    }
}

#[derive(Clone, Debug)]
pub struct Store {
    pool: PgPool,
    notifications: Arc<NotificationHub>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notification {
    channel: String,
    payload: String,
}

impl Notification {
    pub fn channel(&self) -> &str {
        &self.channel
    }

    pub fn payload(&self) -> &str {
        &self.payload
    }
}

#[derive(Debug)]
pub struct ReadyListener {
    filters: HashMap<String, Option<String>>,
    receiver: broadcast::Receiver<NotificationEvent>,
}

impl ReadyListener {
    pub async fn recv(&mut self) -> Result<Notification, PostgresError> {
        loop {
            match self.receiver.recv().await {
                Ok(NotificationEvent::Ready(notification))
                    if self.filters.get(notification.channel()).is_some_and(|payload| {
                        payload.as_ref().is_none_or(|payload| payload == notification.payload())
                    }) =>
                {
                    return Ok(notification);
                }
                Ok(NotificationEvent::Ready(_)) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Ok(NotificationEvent::Disconnected(error)) => return Err(PostgresError::Notification(error)),
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(PostgresError::Notification("notification hub stopped".to_owned()));
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
enum NotificationEvent {
    Ready(Notification),
    Disconnected(String),
}

#[derive(Debug)]
struct NotificationHub {
    commands: mpsc::Sender<NotificationCommand>,
    events: broadcast::Sender<NotificationEvent>,
}

#[derive(Debug)]
struct NotificationCommand {
    channels: Vec<String>,
    ready: oneshot::Sender<Result<(), String>>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SignalWait {
    Ready(Value),
    Waiting,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ResultWait {
    Ready(Value),
    Waiting,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TaskResultWait {
    Ready(TaskResult),
    NotFound,
    TimedOut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueDemand {
    pub ready_tasks: u64,
    pub capable_tasks: u64,
    pub unroutable_tasks: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TaskCompletion {
    pub task_id: TaskId,
    pub attempt: u16,
    pub lease_token: LeaseToken,
    pub result: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TaskFailure {
    pub task_id: TaskId,
    pub attempt: u16,
    pub lease_token: LeaseToken,
    pub error: Value,
    pub retry_after: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct SignalWaitRequest<'a> {
    pub task_id: TaskId,
    pub attempt: u16,
    pub lease_token: LeaseToken,
    pub step_name: &'a StepName,
    pub occurrence: u32,
    pub signal_name: &'a SignalName,
    pub signal_occurrence: u32,
    pub timeout: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct ResultWaitRequest<'a> {
    pub task_id: TaskId,
    pub attempt: u16,
    pub lease_token: LeaseToken,
    pub step_name: &'a StepName,
    pub occurrence: u32,
    pub result_task_id: TaskId,
    pub timeout: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct SpawnRequest<'a> {
    pub parent_task_id: TaskId,
    pub parent_attempt: u16,
    pub parent_lease_token: LeaseToken,
    pub step_name: &'a StepName,
    pub occurrence: u32,
    pub task: &'a EnqueueRequest,
}

struct SuspendTaskRequest<'a> {
    task_id: TaskId,
    attempt: u16,
    lease_token: LeaseToken,
    step_name: &'a StepName,
    occurrence: u32,
    wake_at: Option<DateTime<Utc>>,
    delay_milliseconds: Option<i64>,
}

impl NotificationHub {
    fn start(pool: PgPool) -> Arc<Self> {
        let (commands, receiver) = mpsc::channel(128);
        let (events, _) = broadcast::channel(1_024);
        let hub = Arc::new(Self { commands, events });
        tokio::spawn(run_notification_hub(pool, receiver, hub.events.clone()));
        hub
    }

    async fn subscribe(&self, filters: HashMap<String, Option<String>>) -> Result<ReadyListener, PostgresError> {
        let channels = filters.keys().cloned().collect();
        let receiver = self.events.subscribe();
        let (ready, confirmation) = oneshot::channel();
        self.commands
            .send(NotificationCommand { channels, ready })
            .await
            .map_err(|_| PostgresError::Notification("notification hub stopped".to_owned()))?;
        confirmation
            .await
            .map_err(|_| PostgresError::Notification("notification hub stopped".to_owned()))?
            .map_err(PostgresError::Notification)?;
        Ok(ReadyListener { filters, receiver })
    }
}

async fn run_notification_hub(
    pool: PgPool,
    mut commands: mpsc::Receiver<NotificationCommand>,
    events: broadcast::Sender<NotificationEvent>,
) {
    let mut channels = HashSet::new();
    while let Some(command) = commands.recv().await {
        channels.extend(command.channels);
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(listener) => listener,
            Err(error) => {
                let _ = command.ready.send(Err(error.to_string()));
                continue;
            }
        };
        if let Err(error) = listen_to_channels(&mut listener, &channels).await {
            let _ = command.ready.send(Err(error.to_string()));
            continue;
        }
        let _ = command.ready.send(Ok(()));

        'connected: loop {
            tokio::select! {
                command = commands.recv() => {
                    let Some(command) = command else {
                        return;
                    };
                    let new_channels: Vec<_> = command
                        .channels
                        .into_iter()
                        .filter(|channel| channels.insert(channel.clone()))
                        .collect();
                    for channel in new_channels {
                        if let Err(error) = listener.listen(&channel).await {
                            let message = error.to_string();
                            let _ = command.ready.send(Err(message.clone()));
                            let _ = events.send(NotificationEvent::Disconnected(message));
                            break 'connected;
                        }
                    }
                    let _ = command.ready.send(Ok(()));
                }
                notification = listener.recv() => {
                    match notification {
                        Ok(notification) => {
                            let _ = events.send(NotificationEvent::Ready(Notification {
                                channel: notification.channel().to_owned(),
                                payload: notification.payload().to_owned(),
                            }));
                        }
                        Err(error) => {
                            let _ = events.send(NotificationEvent::Disconnected(error.to_string()));
                            break 'connected;
                        }
                    }
                }
            }
        }
    }
}

async fn listen_to_channels(listener: &mut PgListener, channels: &HashSet<String>) -> Result<(), sqlx::Error> {
    for channel in channels {
        listener.listen(channel).await?;
    }
    Ok(())
}

impl Store {
    pub async fn connect(database_url: &str) -> Result<Self, PostgresError> {
        Self::connect_with_config(&StoreConfig::new(database_url)).await
    }

    pub async fn connect_with_config(config: &StoreConfig) -> Result<Self, PostgresError> {
        let pool = PgPoolOptions::new()
            .max_connections(config.query_connections.get())
            .connect(&config.database_url)
            .await?;
        let listener_pool = PgPoolOptions::new()
            .max_connections(config.listener_connections.get())
            .connect(&config.listener_url)
            .await?;
        Ok(Self {
            pool,
            notifications: NotificationHub::start(listener_pool),
        })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self::from_pools(pool.clone(), pool)
    }

    pub fn from_pools(pool: PgPool, listener_pool: PgPool) -> Self {
        Self {
            pool,
            notifications: NotificationHub::start(listener_pool),
        }
    }

    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn health(&self) -> Result<(), PostgresError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    pub async fn storage_protocol_version(&self) -> Result<u32, PostgresError> {
        let version: i32 = sqlx::query_scalar("SELECT pgtask.storage_protocol_version()")
            .fetch_one(&self.pool)
            .await?;
        u32::try_from(version).map_err(invalid_number)
    }

    pub async fn storage_protocol_range(&self) -> Result<StorageProtocolRange, PostgresError> {
        let (minimum, maximum): (i32, i32) =
            sqlx::query_as("SELECT minimum, maximum FROM pgtask.storage_protocol_range()")
                .fetch_one(&self.pool)
                .await?;
        let Ok(minimum_value) = u32::try_from(minimum) else {
            return Err(PostgresError::InvalidStorageProtocolRange { minimum, maximum });
        };
        let Ok(maximum_value) = u32::try_from(maximum) else {
            return Err(PostgresError::InvalidStorageProtocolRange { minimum, maximum });
        };
        StorageProtocolRange::new(minimum_value, maximum_value)
            .ok_or(PostgresError::InvalidStorageProtocolRange { minimum, maximum })
    }

    /// Returns `None` when the schema is absent, so a caller may still migrate it.
    pub async fn ensure_storage_protocol(
        &self,
        client: StorageProtocolRange,
    ) -> Result<Option<StorageProtocolRange>, PostgresError> {
        let database = match self.storage_protocol_range().await {
            Ok(database) => database,
            Err(PostgresError::Database(sqlx::Error::Database(error)))
                if error.code().as_deref() == Some(UNDEFINED_SCHEMA) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if database.overlaps(client) {
            return Ok(Some(database));
        }
        Err(PostgresError::IncompatibleStorageProtocol {
            database_minimum: database.minimum,
            database_maximum: database.maximum,
            client_minimum: client.minimum,
            client_maximum: client.maximum,
        })
    }

    pub async fn ready_listener(&self, queue_name: &QueueName) -> Result<ReadyListener, PostgresError> {
        self.ready_listener_for(std::slice::from_ref(queue_name)).await
    }

    pub async fn ready_listener_for(&self, queue_names: &[QueueName]) -> Result<ReadyListener, PostgresError> {
        if queue_names.is_empty() {
            return Err(PostgresError::MissingQueues);
        }
        let mut channels = HashMap::from([("pgtask_schedule".to_owned(), None), ("pgtask_wait".to_owned(), None)]);
        for queue_name in queue_names {
            let channel: String = sqlx::query_scalar("SELECT pgtask.ready_channel($1)")
                .bind(queue_name.as_str())
                .fetch_one(&self.pool)
                .await?;
            channels.insert(channel, Some(queue_name.to_string()));
        }
        self.notifications.subscribe(channels).await
    }

    pub async fn result_listener(&self, task_id: TaskId) -> Result<ReadyListener, PostgresError> {
        let channel: String = sqlx::query_scalar("SELECT pgtask.result_channel($1)")
            .bind(task_id.as_uuid())
            .fetch_one(&self.pool)
            .await?;
        self.notifications
            .subscribe(HashMap::from([(channel, Some(task_id.to_string()))]))
            .await
    }

    pub async fn register_worker(
        &self,
        worker_id: WorkerId,
        queue_name: &QueueName,
        version: &str,
        capabilities: &[(TaskName, HandlerVersion, RetryPolicy)],
        ttl: Duration,
    ) -> Result<(), PostgresError> {
        if capabilities.is_empty() {
            return Err(PostgresError::MissingCapabilities);
        }
        let ttl_milliseconds = i64::try_from(ttl.as_millis()).map_err(|_| PostgresError::InvalidLeaseDuration)?;
        if ttl_milliseconds == 0 {
            return Err(PostgresError::InvalidLeaseDuration);
        }
        let task_names: Vec<_> = capabilities.iter().map(|(name, _, _)| name.as_str()).collect();
        let handler_versions: Vec<_> = capabilities
            .iter()
            .map(|(_, handler_version, _)| {
                i32::try_from(handler_version.get()).map_err(|_| PostgresError::InvalidHandlerVersion)
            })
            .collect::<Result<_, _>>()?;
        let policies = capabilities
            .iter()
            .map(|(_, _, policy)| retry_policy_columns(*policy))
            .collect::<Result<Vec<_>, _>>()?;
        let retry_kinds: Vec<_> = policies.iter().map(|policy| policy.kind).collect();
        let base_delays: Vec<_> = policies.iter().map(|policy| policy.base_delay).collect();
        let factors: Vec<_> = policies.iter().map(|policy| policy.factor).collect();
        let max_delays: Vec<_> = policies.iter().map(|policy| policy.max_delay).collect();
        sqlx::query("SELECT pgtask.register_worker($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)")
            .bind(worker_id.as_uuid())
            .bind(queue_name.as_str())
            .bind(version)
            .bind(&task_names)
            .bind(&handler_versions)
            .bind(&retry_kinds)
            .bind(&base_delays)
            .bind(&factors)
            .bind(&max_delays)
            .bind(ttl_milliseconds)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn heartbeat_worker(
        &self,
        worker_id: WorkerId,
        ttl: Duration,
        draining: bool,
    ) -> Result<bool, PostgresError> {
        let ttl_milliseconds = i64::try_from(ttl.as_millis()).map_err(|_| PostgresError::InvalidLeaseDuration)?;
        if ttl_milliseconds == 0 {
            return Err(PostgresError::InvalidLeaseDuration);
        }
        let updated = sqlx::query_scalar("SELECT pgtask.heartbeat_worker($1, $2, $3)")
            .bind(worker_id.as_uuid())
            .bind(ttl_milliseconds)
            .bind(draining)
            .fetch_one(&self.pool)
            .await?;
        Ok(updated)
    }

    /// Counts workers the database still considers live, which is not the same as the number of
    /// processes reporting metrics: a worker whose heartbeat fails keeps reporting and stops counting.
    pub async fn live_worker_count(&self, queue_name: &QueueName) -> Result<u64, PostgresError> {
        let count: i64 = sqlx::query_scalar("SELECT pgtask.live_worker_count($1)")
            .bind(queue_name.as_str())
            .fetch_one(&self.pool)
            .await?;
        u64::try_from(count).map_err(invalid_number)
    }

    pub async fn get_worker(&self, worker_id: WorkerId) -> Result<Option<WorkerRecord>, PostgresError> {
        let row: Option<WorkerRow> = sqlx::query_as(
            r"
            SELECT id, queue_name, version, draining, started_at, heartbeat_at, expires_at
            FROM pgtask.workers
            WHERE id = $1
            ",
        )
        .bind(worker_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let capabilities: Vec<CapabilityRow> = sqlx::query_as(
            r"
            SELECT task_name, handler_version
            FROM pgtask.worker_capabilities
            WHERE worker_id = $1
            ORDER BY task_name, handler_version
            ",
        )
        .bind(worker_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        Ok(Some(row.try_into_record(capabilities)?))
    }

    pub async fn migrate(&self) -> Result<(), PostgresError> {
        let _process_guard = MIGRATION_LOCK.lock().await;
        let mut migrations = sqlx::migrate!();
        migrations.dangerous_set_table_name("public._sqlx_migrations");
        let mut connection = self.pool.acquire().await?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(123_656_071_951_211_i64)
            .execute(&mut *connection)
            .await?;
        let migration = migrations.run(&mut *connection).await;
        let unlock = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(123_656_071_951_211_i64)
            .execute(&mut *connection)
            .await;
        migration?;
        unlock?;
        Ok(())
    }

    pub async fn configure_grants(
        &self,
        owner: &str,
        producer: &str,
        worker: &str,
        observer: &str,
        administrator: &str,
    ) -> Result<(), PostgresError> {
        sqlx::query("SELECT pgtask.configure_grants($1::regrole, $2::regrole, $3::regrole, $4::regrole, $5::regrole)")
            .bind(owner)
            .bind(producer)
            .bind(worker)
            .bind(observer)
            .bind(administrator)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn put_queue(&self, config: &QueueConfig) -> Result<Queue, PostgresError> {
        let retention_seconds = i64::try_from(config.terminal_retention.as_secs())
            .map_err(|_| PostgresError::InvalidTask("queue retention exceeds the Postgres bigint range".to_owned()))?;
        let idempotency_retention_seconds = i64::try_from(config.idempotency_retention.as_secs()).map_err(|_| {
            PostgresError::InvalidTask("idempotency retention exceeds the Postgres bigint range".to_owned())
        })?;
        let max_outstanding_tasks = config
            .max_outstanding_tasks
            .map(|maximum| i64::try_from(maximum.get()))
            .transpose()
            .map_err(|_| PostgresError::InvalidTask("queue capacity exceeds the Postgres bigint range".to_owned()))?;
        let starvation_timeout_seconds = i64::try_from(config.starvation_timeout.as_secs()).map_err(|_| {
            PostgresError::InvalidTask("starvation timeout exceeds the Postgres bigint range".to_owned())
        })?;
        let row: QueueRow = sqlx::query_as(
            "SELECT name, terminal_retention_seconds, idempotency_retention_seconds, max_outstanding_tasks, starvation_timeout_seconds, paused_at, created_at, updated_at FROM pgtask.put_queue($1, $2, $3, $4, $5)",
        )
        .bind(config.name.as_str())
        .bind(retention_seconds)
        .bind(idempotency_retention_seconds)
        .bind(max_outstanding_tasks)
        .bind(starvation_timeout_seconds)
        .fetch_one(&self.pool)
        .await?;
        Queue::try_from(row)
    }

    pub async fn get_queue(&self, queue_name: &QueueName) -> Result<Option<Queue>, PostgresError> {
        let row: Option<QueueRow> = sqlx::query_as(
            r"
            SELECT name, terminal_retention_seconds, idempotency_retention_seconds, max_outstanding_tasks,
                starvation_timeout_seconds, paused_at, created_at, updated_at
            FROM pgtask.queues
            WHERE name = $1
            ",
        )
        .bind(queue_name.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.map(Queue::try_from).transpose()
    }

    pub async fn queue_demand(
        &self,
        queue_name: &QueueName,
        capabilities: &[(TaskName, HandlerVersion)],
    ) -> Result<QueueDemand, PostgresError> {
        if capabilities.is_empty() {
            return Err(PostgresError::MissingCapabilities);
        }
        let task_names: Vec<_> = capabilities.iter().map(|(name, _)| name.as_str()).collect();
        let handler_versions: Vec<_> = capabilities
            .iter()
            .map(|(_, version)| i32::try_from(version.get()).map_err(|_| PostgresError::InvalidHandlerVersion))
            .collect::<Result<_, _>>()?;
        let row: QueueDemandRow = sqlx::query_as("SELECT * FROM pgtask.queue_demand($1, $2, $3)")
            .bind(queue_name.as_str())
            .bind(&task_names)
            .bind(&handler_versions)
            .fetch_one(&self.pool)
            .await?;
        Ok(QueueDemand {
            ready_tasks: u64::try_from(row.ready).map_err(invalid_number)?,
            capable_tasks: u64::try_from(row.capable).map_err(invalid_number)?,
            unroutable_tasks: u64::try_from(row.unroutable).map_err(invalid_number)?,
        })
    }

    pub async fn set_queue_paused(&self, queue_name: &QueueName, paused: bool) -> Result<Option<Queue>, PostgresError> {
        let row: Option<QueueRow> = sqlx::query_as(
            "SELECT name, terminal_retention_seconds, idempotency_retention_seconds, max_outstanding_tasks, starvation_timeout_seconds, paused_at, created_at, updated_at FROM pgtask.set_queue_paused($1, $2)",
        )
        .bind(queue_name.as_str())
        .bind(paused)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Queue::try_from).transpose()
    }

    pub async fn put_schedule(&self, config: &ScheduleConfig) -> Result<Schedule, PostgresError> {
        Self::validate_request(&config.task)?;
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT statement_timestamp()")
            .fetch_one(&self.pool)
            .await?;
        let next_run_at = config.start_at.unwrap_or(config.definition.next_after(now)?);
        let (kind, interval_milliseconds, cron_expression) = match &config.definition {
            ScheduleDefinition::Interval { every } => (
                "interval",
                Some(i64::try_from(every.as_millis()).map_err(|_| ScheduleError::IntervalOutOfRange)?),
                None,
            ),
            ScheduleDefinition::Cron { expression } => ("cron", None, Some(expression.as_str())),
        };
        let (misfire_policy, catch_up_limit) = match config.misfire_policy {
            MisfirePolicy::Skip => ("skip", None),
            MisfirePolicy::Latest => ("latest", None),
            MisfirePolicy::CatchUp { limit } => ("catch_up", Some(i32::from(limit.get()))),
        };
        let handler_version =
            i32::try_from(config.task.handler_version.get()).map_err(|_| PostgresError::InvalidHandlerVersion)?;
        let row: ScheduleRow = sqlx::query_as(
            r"
            SELECT id, name, kind, interval_milliseconds, cron_expression, misfire_policy, catch_up_limit,
                queue_name, task_name, handler_version, payload, headers, priority, max_attempts,
                next_run_at, paused_at, created_at, updated_at
            FROM pgtask.put_schedule($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ",
        )
        .bind(config.id.as_uuid())
        .bind(config.name.as_str())
        .bind(kind)
        .bind(interval_milliseconds)
        .bind(cron_expression)
        .bind(misfire_policy)
        .bind(catch_up_limit)
        .bind(config.task.queue_name.as_str())
        .bind(config.task.task_name.as_str())
        .bind(handler_version)
        .bind(&config.task.payload)
        .bind(Value::Object(config.task.headers.clone()))
        .bind(config.task.priority)
        .bind(i32::from(config.task.max_attempts))
        .bind(next_run_at)
        .fetch_one(&self.pool)
        .await?;
        Schedule::try_from(row)
    }

    pub async fn get_schedule(&self, schedule_id: ScheduleId) -> Result<Option<Schedule>, PostgresError> {
        let row: Option<ScheduleRow> = sqlx::query_as(
            r"
            SELECT id, name, kind, interval_milliseconds, cron_expression, misfire_policy, catch_up_limit,
                queue_name, task_name, handler_version, payload, headers, priority, max_attempts,
                next_run_at, paused_at, created_at, updated_at
            FROM pgtask.get_schedule($1)
            ",
        )
        .bind(schedule_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.map(Schedule::try_from).transpose()
    }

    pub async fn set_schedule_paused(
        &self,
        schedule_id: ScheduleId,
        paused: bool,
    ) -> Result<Option<Schedule>, PostgresError> {
        let row: Option<ScheduleRow> = sqlx::query_as(
            r"
            SELECT id, name, kind, interval_milliseconds, cron_expression, misfire_policy, catch_up_limit,
                queue_name, task_name, handler_version, payload, headers, priority, max_attempts,
                next_run_at, paused_at, created_at, updated_at
            FROM pgtask.set_schedule_paused($1, $2)
            ",
        )
        .bind(schedule_id.as_uuid())
        .bind(paused)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Schedule::try_from).transpose()
    }

    pub async fn delete_schedule(&self, schedule_id: ScheduleId) -> Result<bool, PostgresError> {
        let deleted = sqlx::query_scalar("SELECT pgtask.delete_schedule($1)")
            .bind(schedule_id.as_uuid())
            .fetch_one(&self.pool)
            .await?;
        Ok(deleted)
    }

    pub async fn materialize_due_schedules(&self, limit: u16) -> Result<u64, PostgresError> {
        if limit == 0 {
            return Err(PostgresError::InvalidScheduleLimit);
        }
        let started_at = std::time::Instant::now();
        let mut transaction = self.pool.begin().await?;
        let now: DateTime<Utc> = sqlx::query_scalar("SELECT statement_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let rows: Vec<ScheduleRow> = sqlx::query_as(
            r"
            SELECT id, name, kind, interval_milliseconds, cron_expression, misfire_policy, catch_up_limit,
                queue_name, task_name, handler_version, payload, headers, priority, max_attempts,
                next_run_at, paused_at, created_at, updated_at
            FROM pgtask.claim_due_schedules($1)
            ",
        )
        .bind(i32::from(limit))
        .fetch_all(&mut *transaction)
        .await?;
        let mut total = 0_u64;
        for row in rows {
            let schedule = Schedule::try_from(row)?;
            let materialization =
                schedule
                    .config
                    .definition
                    .materialize(schedule.next_run_at, now, schedule.config.misfire_policy)?;
            let span = info_span!(
                "pgtask.schedule.materialize",
                pgtask.schedule.name = %schedule.config.name,
                pgtask.schedule.occurrences = materialization.occurrences.len(),
            );
            let materialized: i64 = sqlx::query_scalar("SELECT pgtask.materialize_schedule($1, $2, $3, $4)")
                .bind(schedule.config.id.as_uuid())
                .bind(schedule.next_run_at)
                .bind(&materialization.occurrences)
                .bind(materialization.next_run_at)
                .fetch_one(&mut *transaction)
                .instrument(span)
                .await?;
            let materialized = u64::try_from(materialized).map_err(invalid_number)?;
            let kind = match schedule.config.definition {
                ScheduleDefinition::Interval { .. } => "interval",
                ScheduleDefinition::Cron { .. } => "cron",
            };
            let lag = materialization
                .occurrences
                .first()
                .map_or(Duration::ZERO, |occurrence| {
                    now.signed_duration_since(*occurrence).to_std().unwrap_or_default()
                });
            pgtask_otel::record_schedule_occurrences(
                schedule.config.task.queue_name.as_str(),
                schedule.config.task.task_name.as_str(),
                kind,
                materialized,
                materialization.skipped,
                lag,
            );
            total = total
                .checked_add(materialized)
                .ok_or_else(|| PostgresError::InvalidTask("materialized task count overflowed".to_owned()))?;
        }
        transaction.commit().await?;
        pgtask_otel::record_schedule_materialization(started_at.elapsed());
        Ok(total)
    }

    pub async fn next_schedule_delay(&self) -> Result<Option<Duration>, PostgresError> {
        let milliseconds: Option<i64> = sqlx::query_scalar("SELECT pgtask.next_schedule_delay_milliseconds()")
            .fetch_one(&self.pool)
            .await?;
        milliseconds
            .map(|milliseconds| u64::try_from(milliseconds).map(Duration::from_millis))
            .transpose()
            .map_err(invalid_number)
    }

    pub async fn next_task_delay(
        &self,
        queue_name: &QueueName,
        capabilities: &[(TaskName, HandlerVersion)],
    ) -> Result<Option<Duration>, PostgresError> {
        if capabilities.is_empty() {
            return Err(PostgresError::MissingCapabilities);
        }
        let task_names: Vec<_> = capabilities.iter().map(|(name, _)| name.as_str()).collect();
        let handler_versions: Vec<_> = capabilities
            .iter()
            .map(|(_, version)| i32::try_from(version.get()).map_err(|_| PostgresError::InvalidHandlerVersion))
            .collect::<Result<_, _>>()?;
        let milliseconds: Option<i64> = sqlx::query_scalar("SELECT pgtask.next_task_delay_milliseconds($1, $2, $3)")
            .bind(queue_name.as_str())
            .bind(&task_names)
            .bind(&handler_versions)
            .fetch_one(&self.pool)
            .await?;
        milliseconds
            .map(|milliseconds| u64::try_from(milliseconds).map(Duration::from_millis))
            .transpose()
            .map_err(invalid_number)
    }

    pub async fn delete_expired_terminal(&self, queue_name: &QueueName, limit: u16) -> Result<u64, PostgresError> {
        if limit == 0 {
            return Err(PostgresError::InvalidRetentionLimit);
        }
        let deleted: i64 = sqlx::query_scalar("SELECT pgtask.delete_expired_terminal($1, $2)")
            .bind(queue_name.as_str())
            .bind(i32::from(limit))
            .fetch_one(&self.pool)
            .await?;
        u64::try_from(deleted).map_err(invalid_number)
    }

    pub async fn delete_expired_idempotency_keys(
        &self,
        queue_name: &QueueName,
        limit: u16,
    ) -> Result<u64, PostgresError> {
        if limit == 0 {
            return Err(PostgresError::InvalidRetentionLimit);
        }
        let deleted: i64 = sqlx::query_scalar("SELECT pgtask.delete_expired_idempotency_keys($1, $2)")
            .bind(queue_name.as_str())
            .bind(i32::from(limit))
            .fetch_one(&self.pool)
            .await?;
        u64::try_from(deleted).map_err(invalid_number)
    }

    pub async fn enqueue(&self, request: &EnqueueRequest) -> Result<EnqueueResult, PostgresError> {
        Self::validate_request(request)?;
        let handler_version =
            i32::try_from(request.handler_version.get()).map_err(|_| PostgresError::InvalidHandlerVersion)?;
        let span = info_span!(
            "pgtask.enqueue",
            otel.kind = "producer",
            pgtask.task.name = %request.task_name,
            pgtask.queue.name = %request.queue_name,
        );
        let headers = pgtask_otel::inject_span_context(&request.headers, &span);
        let row: EnqueueRow =
            sqlx::query_as("SELECT task_id, created FROM pgtask.enqueue($1, $2, $3, $4, $5, $6, $7, $8, $9)")
                .bind(request.task_name.as_str())
                .bind(&request.payload)
                .bind(request.queue_name.as_str())
                .bind(handler_version)
                .bind(request.run_at)
                .bind(request.priority)
                .bind(i32::from(request.max_attempts))
                .bind(&request.idempotency_key)
                .bind(Value::Object(headers))
                .fetch_one(&self.pool)
                .instrument(span)
                .await?;

        let result: EnqueueResult = row.into();
        if result.created {
            pgtask_otel::record_enqueued(request.queue_name.as_str(), request.task_name.as_str(), 1);
        }
        Ok(result)
    }

    pub async fn spawn_task(&self, request: SpawnRequest<'_>) -> Result<Option<EnqueueResult>, PostgresError> {
        Self::validate_request(request.task)?;
        let parent_occurrence = i32::try_from(request.occurrence).map_err(invalid_number)?;
        let handler_version =
            i32::try_from(request.task.handler_version.get()).map_err(|_| PostgresError::InvalidHandlerVersion)?;
        let span = info_span!(
            "pgtask.spawn",
            otel.kind = "producer",
            pgtask.task.parent_id = %request.parent_task_id,
            pgtask.task.name = %request.task.task_name,
            pgtask.queue.name = %request.task.queue_name,
            pgtask.step.name = %request.step_name,
            pgtask.step.occurrence = request.occurrence,
        );
        let headers = pgtask_otel::inject_span_context(&request.task.headers, &span);
        let row: Option<EnqueueRow> = sqlx::query_as(
            "SELECT task_id, created FROM pgtask.spawn_task($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(request.parent_task_id.as_uuid())
        .bind(i32::from(request.parent_attempt))
        .bind(request.parent_lease_token.as_uuid())
        .bind(request.step_name.as_str())
        .bind(parent_occurrence)
        .bind(request.task.task_name.as_str())
        .bind(&request.task.payload)
        .bind(request.task.queue_name.as_str())
        .bind(handler_version)
        .bind(request.task.run_at)
        .bind(request.task.priority)
        .bind(i32::from(request.task.max_attempts))
        .bind(Value::Object(headers))
        .fetch_optional(&self.pool)
        .instrument(span)
        .await?;
        let result = row.map(EnqueueResult::from);
        if let Some(result) = result
            && result.created
        {
            pgtask_otel::record_enqueued(request.task.queue_name.as_str(), request.task.task_name.as_str(), 1);
        }
        Ok(result)
    }

    pub async fn enqueue_on(
        connection: &mut PgConnection,
        request: &EnqueueRequest,
    ) -> Result<EnqueueResult, PostgresError> {
        Self::validate_request(request)?;
        let handler_version =
            i32::try_from(request.handler_version.get()).map_err(|_| PostgresError::InvalidHandlerVersion)?;
        let span = info_span!(
            "pgtask.enqueue",
            otel.kind = "producer",
            pgtask.task.name = %request.task_name,
            pgtask.queue.name = %request.queue_name,
        );
        let headers = pgtask_otel::inject_span_context(&request.headers, &span);
        let row: EnqueueRow =
            sqlx::query_as("SELECT task_id, created FROM pgtask.enqueue($1, $2, $3, $4, $5, $6, $7, $8, $9)")
                .bind(request.task_name.as_str())
                .bind(&request.payload)
                .bind(request.queue_name.as_str())
                .bind(handler_version)
                .bind(request.run_at)
                .bind(request.priority)
                .bind(i32::from(request.max_attempts))
                .bind(&request.idempotency_key)
                .bind(Value::Object(headers))
                .fetch_one(connection)
                .instrument(span)
                .await?;

        let result: EnqueueResult = row.into();
        if result.created {
            pgtask_otel::record_enqueued(request.queue_name.as_str(), request.task_name.as_str(), 1);
        }
        Ok(result)
    }

    pub async fn enqueue_many(&self, requests: &[EnqueueRequest]) -> Result<Vec<EnqueueResult>, PostgresError> {
        for request in requests {
            Self::validate_request(request)?;
            i32::try_from(request.handler_version.get()).map_err(|_| PostgresError::InvalidHandlerVersion)?;
        }
        let span = info_span!(
            "pgtask.enqueue_many",
            otel.kind = "producer",
            pgtask.task.count = requests.len()
        );
        let requests_with_context: Vec<_> = requests
            .iter()
            .cloned()
            .map(|mut request| {
                request.headers = pgtask_otel::inject_span_context(&request.headers, &span);
                request
            })
            .collect();
        let rows: Vec<BatchEnqueueRow> = sqlx::query_as(
            "SELECT request_index, task_id, created FROM pgtask.enqueue_many($1) ORDER BY request_index",
        )
        .bind(
            serde_json::to_value(&requests_with_context)
                .map_err(|error| PostgresError::InvalidTask(error.to_string()))?,
        )
        .fetch_all(&self.pool)
        .instrument(span)
        .await?;
        Self::batch_results(rows, requests)
    }

    pub async fn enqueue_many_on(
        connection: &mut PgConnection,
        requests: &[EnqueueRequest],
    ) -> Result<Vec<EnqueueResult>, PostgresError> {
        for request in requests {
            Self::validate_request(request)?;
            i32::try_from(request.handler_version.get()).map_err(|_| PostgresError::InvalidHandlerVersion)?;
        }
        let span = info_span!(
            "pgtask.enqueue_many",
            otel.kind = "producer",
            pgtask.task.count = requests.len()
        );
        let requests_with_context: Vec<_> = requests
            .iter()
            .cloned()
            .map(|mut request| {
                request.headers = pgtask_otel::inject_span_context(&request.headers, &span);
                request
            })
            .collect();
        let rows: Vec<BatchEnqueueRow> = sqlx::query_as(
            "SELECT request_index, task_id, created FROM pgtask.enqueue_many($1) ORDER BY request_index",
        )
        .bind(
            serde_json::to_value(&requests_with_context)
                .map_err(|error| PostgresError::InvalidTask(error.to_string()))?,
        )
        .fetch_all(connection)
        .instrument(span)
        .await?;
        Self::batch_results(rows, requests)
    }

    fn batch_results(
        rows: Vec<BatchEnqueueRow>,
        requests: &[EnqueueRequest],
    ) -> Result<Vec<EnqueueResult>, PostgresError> {
        if rows.len() != requests.len()
            || rows
                .iter()
                .enumerate()
                .any(|(index, row)| usize::try_from(row.request_index) != Ok(index))
        {
            return Err(PostgresError::InvalidTask(
                "batch enqueue returned an invalid result set".to_owned(),
            ));
        }
        Ok(rows
            .into_iter()
            .zip(requests)
            .map(|(row, request)| {
                if row.created {
                    pgtask_otel::record_enqueued(request.queue_name.as_str(), request.task_name.as_str(), 1);
                }
                EnqueueResult {
                    task_id: TaskId::from_uuid(row.task_id),
                    created: row.created,
                }
            })
            .collect())
    }

    fn validate_request(request: &EnqueueRequest) -> Result<(), PostgresError> {
        if request.max_attempts == 0 {
            return Err(PostgresError::InvalidMaxAttempts);
        }
        Ok(())
    }

    pub async fn claim(
        &self,
        queue_name: &QueueName,
        worker_id: WorkerId,
        capabilities: &[(TaskName, HandlerVersion)],
        limit: u16,
        lease_duration: Duration,
    ) -> Result<Vec<Task>, PostgresError> {
        if limit == 0 {
            return Err(PostgresError::InvalidClaimLimit);
        }
        if lease_duration.is_zero() {
            return Err(PostgresError::InvalidLeaseDuration);
        }
        if capabilities.is_empty() {
            return Err(PostgresError::MissingCapabilities);
        }

        let task_names: Vec<&str> = capabilities.iter().map(|(name, _)| name.as_str()).collect();
        let handler_versions: Vec<i32> = capabilities
            .iter()
            .map(|(_, version)| i32::try_from(version.get()).map_err(|_| PostgresError::InvalidHandlerVersion))
            .collect::<Result<_, _>>()?;
        let lease_milliseconds =
            i64::try_from(lease_duration.as_millis()).map_err(|_| PostgresError::InvalidLeaseDuration)?;

        let span = info_span!(
            "pgtask.claim",
            pgtask.queue.name = %queue_name,
            pgtask.claim.limit = limit,
        );
        let rows: Vec<TaskRow> = sqlx::query_as(
            r"
            SELECT id, queue_name, task_name, handler_version, payload, headers, state, priority,
                run_at, attempt, max_attempts, lease_token, lease_owner, lease_expires_at,
                created_at, updated_at, completed_at, result, error, parent_task_id,
                retry_kind, retry_base_delay_milliseconds, retry_factor, retry_max_delay_milliseconds
            FROM pgtask.claim($1, $2, $3, $4, $5, $6)
            ",
        )
        .bind(queue_name.as_str())
        .bind(worker_id.as_uuid())
        .bind(&task_names)
        .bind(&handler_versions)
        .bind(i32::from(limit))
        .bind(lease_milliseconds)
        .fetch_all(&self.pool)
        .instrument(span)
        .await?;

        rows.into_iter()
            .map(|row| {
                let task = Task::try_from(row)?;
                pgtask_otel::record_claimed(task.queue_name.as_str(), task.task_name.as_str());
                Ok(task)
            })
            .collect()
    }

    pub async fn get_task(&self, task_id: TaskId) -> Result<Option<Task>, PostgresError> {
        let row: Option<TaskRow> = sqlx::query_as(
            r"
            SELECT id, queue_name, task_name, handler_version, payload, headers, state, priority,
                run_at, attempt, max_attempts, lease_token, lease_owner, lease_expires_at,
                created_at, updated_at, completed_at, result, error, parent_task_id,
                retry_kind, retry_base_delay_milliseconds, retry_factor, retry_max_delay_milliseconds
            FROM pgtask.get_task($1)
            ",
        )
        .bind(task_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.map(Task::try_from).transpose()
    }

    pub async fn task_count_by_state(&self, queue_name: &QueueName, state: TaskState) -> Result<u64, PostgresError> {
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pgtask.task_view WHERE queue_name = $1 AND state = $2")
                .bind(queue_name.as_str())
                .bind(state.as_str())
                .fetch_one(&self.pool)
                .await?;
        Ok(u64::try_from(count).expect("PostgreSQL count is nonnegative"))
    }

    pub async fn get_checkpoint(
        &self,
        task_id: TaskId,
        handler_version: HandlerVersion,
        step_name: &StepName,
        occurrence: u32,
    ) -> Result<Option<Checkpoint>, PostgresError> {
        let handler_version = i32::try_from(handler_version.get()).map_err(|_| PostgresError::InvalidHandlerVersion)?;
        let occurrence = i32::try_from(occurrence).map_err(invalid_number)?;
        let row: Option<CheckpointRow> = sqlx::query_as(
            r"
            SELECT task_id, handler_version, step_name, occurrence, value, created_at
            FROM pgtask.get_checkpoint($1, $2, $3, $4)
            ",
        )
        .bind(task_id.as_uuid())
        .bind(handler_version)
        .bind(step_name.as_str())
        .bind(occurrence)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Checkpoint::try_from).transpose()
    }

    pub async fn commit_checkpoint(
        &self,
        task_id: TaskId,
        attempt: u16,
        lease_token: LeaseToken,
        step_name: &StepName,
        occurrence: u32,
        value: &Value,
    ) -> Result<Option<Checkpoint>, PostgresError> {
        let occurrence = i32::try_from(occurrence).map_err(invalid_number)?;
        let row: Option<CheckpointRow> = sqlx::query_as(
            r"
            SELECT task_id, handler_version, step_name, occurrence, value, created_at
            FROM pgtask.commit_checkpoint($1, $2, $3, $4, $5, $6)
            ",
        )
        .bind(task_id.as_uuid())
        .bind(i32::from(attempt))
        .bind(lease_token.as_uuid())
        .bind(step_name.as_str())
        .bind(occurrence)
        .bind(value)
        .fetch_optional(&self.pool)
        .await?;
        row.map(Checkpoint::try_from).transpose()
    }

    pub async fn sleep_until(
        &self,
        task_id: TaskId,
        attempt: u16,
        lease_token: LeaseToken,
        step_name: &StepName,
        occurrence: u32,
        wake_at: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, PostgresError> {
        self.suspend_task(SuspendTaskRequest {
            task_id,
            attempt,
            lease_token,
            step_name,
            occurrence,
            wake_at: Some(wake_at),
            delay_milliseconds: None,
        })
        .await
    }

    pub async fn sleep_for(
        &self,
        task_id: TaskId,
        attempt: u16,
        lease_token: LeaseToken,
        step_name: &StepName,
        occurrence: u32,
        duration: Duration,
    ) -> Result<Option<DateTime<Utc>>, PostgresError> {
        let delay_milliseconds =
            i64::try_from(duration.as_millis()).map_err(|_| PostgresError::InvalidSleepDuration)?;
        self.suspend_task(SuspendTaskRequest {
            task_id,
            attempt,
            lease_token,
            step_name,
            occurrence,
            wake_at: None,
            delay_milliseconds: Some(delay_milliseconds),
        })
        .await
    }

    async fn suspend_task(&self, request: SuspendTaskRequest<'_>) -> Result<Option<DateTime<Utc>>, PostgresError> {
        let occurrence = i32::try_from(request.occurrence).map_err(invalid_number)?;
        let wake_at = sqlx::query_scalar("SELECT pgtask.suspend_task($1, $2, $3, $4, $5, $6, $7)")
            .bind(request.task_id.as_uuid())
            .bind(i32::from(request.attempt))
            .bind(request.lease_token.as_uuid())
            .bind(request.step_name.as_str())
            .bind(occurrence)
            .bind(request.wake_at)
            .bind(request.delay_milliseconds)
            .fetch_one(&self.pool)
            .await?;
        Ok(wake_at)
    }

    pub async fn emit_signal(
        &self,
        task_id: TaskId,
        signal_name: &SignalName,
        occurrence: u32,
        value: &Value,
    ) -> Result<Signal, PostgresError> {
        let occurrence = i32::try_from(occurrence).map_err(invalid_number)?;
        let row: SignalRow = sqlx::query_as(
            "SELECT task_id, signal_name, occurrence, value, created_at FROM pgtask.emit_signal($1, $2, $3, $4)",
        )
        .bind(task_id.as_uuid())
        .bind(signal_name.as_str())
        .bind(occurrence)
        .bind(value)
        .fetch_one(&self.pool)
        .await?;
        Signal::try_from(row)
    }

    pub async fn wait_for_signal(&self, request: SignalWaitRequest<'_>) -> Result<Option<SignalWait>, PostgresError> {
        let occurrence = i32::try_from(request.occurrence).map_err(invalid_number)?;
        let signal_occurrence = i32::try_from(request.signal_occurrence).map_err(invalid_number)?;
        let timeout_milliseconds = request
            .timeout
            .map(|duration| i64::try_from(duration.as_millis()).map_err(|_| PostgresError::InvalidSleepDuration))
            .transpose()?;
        let row: Option<SignalWaitRow> =
            sqlx::query_as("SELECT status, checkpoint FROM pgtask.wait_for_signal($1, $2, $3, $4, $5, $6, $7, $8)")
                .bind(request.task_id.as_uuid())
                .bind(i32::from(request.attempt))
                .bind(request.lease_token.as_uuid())
                .bind(request.step_name.as_str())
                .bind(occurrence)
                .bind(request.signal_name.as_str())
                .bind(signal_occurrence)
                .bind(timeout_milliseconds)
                .fetch_optional(&self.pool)
                .await?;
        row.map(SignalWait::try_from).transpose()
    }

    pub async fn wait_for_result(&self, request: ResultWaitRequest<'_>) -> Result<Option<ResultWait>, PostgresError> {
        let occurrence = i32::try_from(request.occurrence).map_err(invalid_number)?;
        let timeout_milliseconds = request
            .timeout
            .map(|duration| {
                if duration.is_zero() {
                    return Err(PostgresError::InvalidResultWaitTimeout);
                }
                i64::try_from(duration.as_millis()).map_err(|_| PostgresError::InvalidResultWaitTimeout)
            })
            .transpose()?;
        let row: Option<ResultWaitRow> =
            sqlx::query_as("SELECT status, checkpoint FROM pgtask.wait_for_result($1, $2, $3, $4, $5, $6, $7)")
                .bind(request.task_id.as_uuid())
                .bind(i32::from(request.attempt))
                .bind(request.lease_token.as_uuid())
                .bind(request.step_name.as_str())
                .bind(occurrence)
                .bind(request.result_task_id.as_uuid())
                .bind(timeout_milliseconds)
                .fetch_optional(&self.pool)
                .await?;
        row.map(ResultWait::try_from).transpose()
    }

    pub async fn task_result(&self, task_id: TaskId) -> Result<Option<TaskResult>, PostgresError> {
        let row: Option<TaskResultRow> =
            sqlx::query_as("SELECT state, result, error, completed_at FROM pgtask.task_result($1)")
                .bind(task_id.as_uuid())
                .fetch_optional(&self.pool)
                .await?;
        row.map(TaskResult::try_from).transpose()
    }

    pub async fn wait_for_task_result(
        &self,
        task_id: TaskId,
        timeout: Option<Duration>,
    ) -> Result<TaskResultWait, PostgresError> {
        let mut listener = self.result_listener(task_id).await?;
        let Some(result) = self.task_result(task_id).await? else {
            return Ok(TaskResultWait::NotFound);
        };
        if result.state.is_terminal() {
            return Ok(TaskResultWait::Ready(result));
        }

        let wait = async {
            loop {
                let notification = listener.recv().await?;
                if notification.payload() != task_id.to_string() {
                    continue;
                }
                let result = self.task_result(task_id).await?.ok_or_else(|| {
                    PostgresError::InvalidTask("task disappeared while waiting for result".to_owned())
                })?;
                if result.state.is_terminal() {
                    return Ok(result);
                }
            }
        };
        match timeout {
            Some(timeout) => match tokio::time::timeout(timeout, wait).await {
                Ok(result) => result.map(TaskResultWait::Ready),
                Err(_) => Ok(TaskResultWait::TimedOut),
            },
            None => wait.await.map(TaskResultWait::Ready),
        }
    }

    pub async fn recover_wait_timeouts(&self, limit: u16) -> Result<u64, PostgresError> {
        if limit == 0 {
            return Err(PostgresError::InvalidWaitLimit);
        }
        let recovered: i64 = sqlx::query_scalar("SELECT pgtask.recover_wait_timeouts($1)")
            .bind(i32::from(limit))
            .fetch_one(&self.pool)
            .await?;
        u64::try_from(recovered).map_err(invalid_number)
    }

    pub async fn recover_result_wait_timeouts(&self, limit: u16) -> Result<u64, PostgresError> {
        if limit == 0 {
            return Err(PostgresError::InvalidWaitLimit);
        }
        let recovered: i64 = sqlx::query_scalar("SELECT pgtask.recover_result_wait_timeouts($1)")
            .bind(i32::from(limit))
            .fetch_one(&self.pool)
            .await?;
        u64::try_from(recovered).map_err(invalid_number)
    }

    pub async fn next_wait_delay(&self) -> Result<Option<Duration>, PostgresError> {
        let milliseconds: Option<i64> = sqlx::query_scalar("SELECT pgtask.next_wait_delay_milliseconds()")
            .fetch_one(&self.pool)
            .await?;
        milliseconds
            .map(|milliseconds| u64::try_from(milliseconds).map(Duration::from_millis))
            .transpose()
            .map_err(invalid_number)
    }

    pub async fn cancel(&self, task_id: TaskId) -> Result<bool, PostgresError> {
        let cancelled: Option<CancelledTaskRow> =
            sqlx::query_as("SELECT queue_name, task_name FROM pgtask.cancel_task($1)")
                .bind(task_id.as_uuid())
                .fetch_optional(&self.pool)
                .await?;
        if let Some(cancelled) = cancelled {
            pgtask_otel::record_cancelled(&cancelled.queue_name, &cancelled.task_name);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn renew_lease(
        &self,
        task_id: TaskId,
        attempt: u16,
        lease_token: LeaseToken,
        lease_duration: Duration,
    ) -> Result<bool, PostgresError> {
        let renewed = self
            .renew_leases(
                &[LeaseRenewal {
                    task_id,
                    attempt,
                    lease_token,
                }],
                lease_duration,
            )
            .await?;
        Ok(renewed.contains(&task_id))
    }

    pub async fn renew_leases(
        &self,
        leases: &[LeaseRenewal],
        lease_duration: Duration,
    ) -> Result<Vec<TaskId>, PostgresError> {
        if lease_duration.is_zero() {
            return Err(PostgresError::InvalidLeaseDuration);
        }
        if leases.is_empty() {
            return Ok(Vec::new());
        }
        let lease_milliseconds =
            i64::try_from(lease_duration.as_millis()).map_err(|_| PostgresError::InvalidLeaseDuration)?;
        let task_ids: Vec<_> = leases.iter().map(|lease| lease.task_id.as_uuid()).collect();
        let attempts: Vec<_> = leases.iter().map(|lease| i32::from(lease.attempt)).collect();
        let lease_tokens: Vec<_> = leases.iter().map(|lease| lease.lease_token.as_uuid()).collect();
        let renewed: Vec<Uuid> = sqlx::query_scalar("SELECT * FROM pgtask.renew_leases($1, $2, $3, $4)")
            .bind(&task_ids)
            .bind(&attempts)
            .bind(&lease_tokens)
            .bind(lease_milliseconds)
            .fetch_all(&self.pool)
            .await?;
        Ok(renewed.into_iter().map(TaskId::from_uuid).collect())
    }

    pub async fn complete(
        &self,
        task_id: TaskId,
        attempt: u16,
        lease_token: LeaseToken,
        result: Option<&Value>,
    ) -> Result<bool, PostgresError> {
        let completed = sqlx::query_scalar("SELECT pgtask.complete_task($1, $2, $3, $4)")
            .bind(task_id.as_uuid())
            .bind(i32::from(attempt))
            .bind(lease_token.as_uuid())
            .bind(result)
            .fetch_one(&self.pool)
            .await?;
        Ok(completed)
    }

    pub async fn fail(
        &self,
        task_id: TaskId,
        attempt: u16,
        lease_token: LeaseToken,
        error: &Value,
        retry_after: Option<Duration>,
    ) -> Result<Option<TaskState>, PostgresError> {
        let retry_milliseconds = retry_after
            .map(|duration| i64::try_from(duration.as_millis()).map_err(|_| PostgresError::InvalidLeaseDuration))
            .transpose()?;
        let state: Option<String> = sqlx::query_scalar("SELECT pgtask.fail_task($1, $2, $3, $4, $5)")
            .bind(task_id.as_uuid())
            .bind(i32::from(attempt))
            .bind(lease_token.as_uuid())
            .bind(error)
            .bind(retry_milliseconds)
            .fetch_one(&self.pool)
            .await?;
        state.map(|value| parse_state(&value)).transpose()
    }

    pub async fn complete_many(&self, completions: &[TaskCompletion]) -> Result<Vec<bool>, PostgresError> {
        if completions.is_empty() {
            return Ok(Vec::new());
        }
        if completions
            .iter()
            .map(|completion| completion.task_id)
            .collect::<HashSet<_>>()
            .len()
            != completions.len()
        {
            return Err(PostgresError::InvalidTask(
                "batch completions must contain unique task ids".to_owned(),
            ));
        }
        let payload = Value::Array(
            completions
                .iter()
                .map(|completion| {
                    json!({
                        "task_id": completion.task_id,
                        "attempt": completion.attempt,
                        "lease_token": completion.lease_token,
                        "has_result": completion.result.is_some(),
                        "result": completion.result,
                    })
                })
                .collect(),
        );
        let completed = sqlx::query_scalar("SELECT completed FROM pgtask.complete_tasks($1) ORDER BY request_index")
            .bind(payload)
            .fetch_all(&self.pool)
            .await?;
        Ok(completed)
    }

    pub async fn fail_many(&self, failures: &[TaskFailure]) -> Result<Vec<Option<TaskState>>, PostgresError> {
        if failures.is_empty() {
            return Ok(Vec::new());
        }
        if failures
            .iter()
            .map(|failure| failure.task_id)
            .collect::<HashSet<_>>()
            .len()
            != failures.len()
        {
            return Err(PostgresError::InvalidTask(
                "batch failures must contain unique task ids".to_owned(),
            ));
        }
        let payload = Value::Array(
            failures
                .iter()
                .map(|failure| {
                    let retry_milliseconds = failure
                        .retry_after
                        .map(|duration| {
                            i64::try_from(duration.as_millis()).map_err(|_| PostgresError::InvalidLeaseDuration)
                        })
                        .transpose()?;
                    Ok(json!({
                        "task_id": failure.task_id,
                        "attempt": failure.attempt,
                        "lease_token": failure.lease_token,
                        "error": failure.error,
                        "retry_milliseconds": retry_milliseconds,
                    }))
                })
                .collect::<Result<_, PostgresError>>()?,
        );
        let states: Vec<Option<String>> =
            sqlx::query_scalar("SELECT state FROM pgtask.fail_tasks($1) ORDER BY request_index")
                .bind(payload)
                .fetch_all(&self.pool)
                .await?;
        states
            .into_iter()
            .map(|state| state.map(|state| parse_state(&state)).transpose())
            .collect()
    }

    pub async fn recover_expired(&self, queue_name: &QueueName, limit: u16) -> Result<u64, PostgresError> {
        if limit == 0 {
            return Err(PostgresError::InvalidClaimLimit);
        }
        let recovered: i64 = sqlx::query_scalar("SELECT pgtask.recover_expired($1, $2)")
            .bind(queue_name.as_str())
            .bind(i32::from(limit))
            .fetch_one(&self.pool)
            .await?;
        let recovered = u64::try_from(recovered).map_err(invalid_number)?;
        if recovered > 0 {
            pgtask_otel::record_recovered(queue_name.as_str(), recovered);
        }
        Ok(recovered)
    }
}

#[derive(FromRow)]
struct EnqueueRow {
    task_id: Uuid,
    created: bool,
}

#[derive(FromRow)]
struct QueueDemandRow {
    #[sqlx(rename = "ready_tasks")]
    ready: i64,
    #[sqlx(rename = "capable_tasks")]
    capable: i64,
    #[sqlx(rename = "unroutable_tasks")]
    unroutable: i64,
}

#[derive(FromRow)]
struct BatchEnqueueRow {
    request_index: i64,
    task_id: Uuid,
    created: bool,
}

#[derive(FromRow)]
struct QueueRow {
    name: String,
    terminal_retention_seconds: i64,
    idempotency_retention_seconds: i64,
    max_outstanding_tasks: Option<i64>,
    starvation_timeout_seconds: i64,
    paused_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(FromRow)]
struct ScheduleRow {
    id: Uuid,
    name: String,
    kind: String,
    interval_milliseconds: Option<i64>,
    cron_expression: Option<String>,
    misfire_policy: String,
    catch_up_limit: Option<i32>,
    queue_name: String,
    task_name: String,
    handler_version: i32,
    payload: Value,
    headers: Value,
    priority: i16,
    max_attempts: i32,
    next_run_at: DateTime<Utc>,
    paused_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(FromRow)]
struct WorkerRow {
    id: Uuid,
    queue_name: String,
    version: String,
    draining: bool,
    started_at: DateTime<Utc>,
    heartbeat_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

#[derive(FromRow)]
struct CapabilityRow {
    task_name: String,
    handler_version: i32,
}

#[derive(FromRow)]
struct CheckpointRow {
    task_id: Uuid,
    handler_version: i32,
    step_name: String,
    occurrence: i32,
    value: Value,
    created_at: DateTime<Utc>,
}

#[derive(FromRow)]
struct SignalRow {
    task_id: Uuid,
    signal_name: String,
    occurrence: i32,
    value: Value,
    created_at: DateTime<Utc>,
}

#[derive(FromRow)]
struct SignalWaitRow {
    status: String,
    checkpoint: Option<Value>,
}

#[derive(FromRow)]
struct ResultWaitRow {
    status: String,
    checkpoint: Option<Value>,
}

#[derive(FromRow)]
struct TaskResultRow {
    state: String,
    result: Option<Value>,
    error: Option<Value>,
    completed_at: Option<DateTime<Utc>>,
}

impl TryFrom<CheckpointRow> for Checkpoint {
    type Error = PostgresError;

    fn try_from(row: CheckpointRow) -> Result<Self, Self::Error> {
        Ok(Self {
            task_id: TaskId::from_uuid(row.task_id),
            handler_version: HandlerVersion::new(
                NonZeroU32::new(u32::try_from(row.handler_version).map_err(invalid_number)?)
                    .ok_or_else(|| PostgresError::InvalidTask("handler version is zero".to_owned()))?,
            ),
            step_name: StepName::new(row.step_name).map_err(|error| PostgresError::InvalidTask(error.to_string()))?,
            occurrence: u32::try_from(row.occurrence).map_err(invalid_number)?,
            value: row.value,
            created_at: row.created_at,
        })
    }
}

impl TryFrom<SignalRow> for Signal {
    type Error = PostgresError;

    fn try_from(row: SignalRow) -> Result<Self, Self::Error> {
        Ok(Self {
            task_id: TaskId::from_uuid(row.task_id),
            name: SignalName::new(row.signal_name).map_err(|error| PostgresError::InvalidTask(error.to_string()))?,
            occurrence: u32::try_from(row.occurrence).map_err(invalid_number)?,
            value: row.value,
            created_at: row.created_at,
        })
    }
}

impl TryFrom<SignalWaitRow> for SignalWait {
    type Error = PostgresError;

    fn try_from(row: SignalWaitRow) -> Result<Self, Self::Error> {
        match (row.status.as_str(), row.checkpoint) {
            ("ready", Some(checkpoint)) => Ok(Self::Ready(checkpoint)),
            ("waiting", None) => Ok(Self::Waiting),
            _ => Err(PostgresError::InvalidTask("invalid signal wait result".to_owned())),
        }
    }
}

impl TryFrom<ResultWaitRow> for ResultWait {
    type Error = PostgresError;

    fn try_from(row: ResultWaitRow) -> Result<Self, Self::Error> {
        match (row.status.as_str(), row.checkpoint) {
            ("ready", Some(checkpoint)) => Ok(Self::Ready(checkpoint)),
            ("waiting", None) => Ok(Self::Waiting),
            _ => Err(PostgresError::InvalidTask("invalid result wait response".to_owned())),
        }
    }
}

impl TryFrom<TaskResultRow> for TaskResult {
    type Error = PostgresError;

    fn try_from(row: TaskResultRow) -> Result<Self, Self::Error> {
        Ok(Self {
            state: parse_state(&row.state)?,
            result: row.result,
            error: row.error,
            completed_at: row.completed_at,
        })
    }
}

impl WorkerRow {
    fn try_into_record(self, capabilities: Vec<CapabilityRow>) -> Result<WorkerRecord, PostgresError> {
        let capabilities = capabilities
            .into_iter()
            .map(|capability| -> Result<_, PostgresError> {
                Ok((
                    TaskName::new(capability.task_name)
                        .map_err(|error| PostgresError::InvalidTask(error.to_string()))?,
                    HandlerVersion::new(
                        NonZeroU32::new(u32::try_from(capability.handler_version).map_err(invalid_number)?)
                            .ok_or_else(|| PostgresError::InvalidTask("handler version is zero".to_owned()))?,
                    ),
                ))
            })
            .collect::<Result<_, _>>()?;
        Ok(WorkerRecord {
            id: WorkerId::from_uuid(self.id),
            queue_name: QueueName::new(self.queue_name)
                .map_err(|error| PostgresError::InvalidTask(error.to_string()))?,
            version: self.version,
            draining: self.draining,
            started_at: self.started_at,
            heartbeat_at: self.heartbeat_at,
            expires_at: self.expires_at,
            capabilities,
        })
    }
}

impl TryFrom<QueueRow> for Queue {
    type Error = PostgresError;

    fn try_from(row: QueueRow) -> Result<Self, Self::Error> {
        Ok(Self {
            name: QueueName::new(row.name).map_err(|error| PostgresError::InvalidTask(error.to_string()))?,
            terminal_retention: Duration::from_secs(
                u64::try_from(row.terminal_retention_seconds).map_err(invalid_number)?,
            ),
            idempotency_retention: Duration::from_secs(
                u64::try_from(row.idempotency_retention_seconds).map_err(invalid_number)?,
            ),
            max_outstanding_tasks: row
                .max_outstanding_tasks
                .map(|maximum| {
                    u64::try_from(maximum)
                        .map_err(invalid_number)
                        .and_then(|maximum| std::num::NonZeroU64::new(maximum).ok_or_else(|| invalid_number(maximum)))
                })
                .transpose()?,
            starvation_timeout: Duration::from_secs(
                u64::try_from(row.starvation_timeout_seconds).map_err(invalid_number)?,
            ),
            paused_at: row.paused_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

impl TryFrom<ScheduleRow> for Schedule {
    type Error = PostgresError;

    fn try_from(row: ScheduleRow) -> Result<Self, Self::Error> {
        let definition = match (row.kind.as_str(), row.interval_milliseconds, row.cron_expression) {
            ("interval", Some(milliseconds), None) => ScheduleDefinition::interval(Duration::from_millis(
                u64::try_from(milliseconds).map_err(invalid_number)?,
            ))?,
            ("cron", None, Some(expression)) => ScheduleDefinition::cron(expression)?,
            _ => return Err(PostgresError::InvalidTask("invalid schedule definition".to_owned())),
        };
        let misfire_policy = match (row.misfire_policy.as_str(), row.catch_up_limit) {
            ("skip", None) => MisfirePolicy::Skip,
            ("latest", None) => MisfirePolicy::Latest,
            ("catch_up", Some(limit)) => MisfirePolicy::CatchUp {
                limit: std::num::NonZeroU16::new(u16::try_from(limit).map_err(invalid_number)?)
                    .ok_or_else(|| PostgresError::InvalidTask("catch-up limit is zero".to_owned()))?,
            },
            _ => return Err(PostgresError::InvalidTask("invalid schedule misfire policy".to_owned())),
        };
        let headers = row
            .headers
            .as_object()
            .cloned()
            .ok_or_else(|| PostgresError::InvalidTask("schedule headers are not an object".to_owned()))?;
        let mut task = EnqueueRequest::new(
            TaskName::new(row.task_name).map_err(|error| PostgresError::InvalidTask(error.to_string()))?,
            row.payload,
        );
        task.handler_version = HandlerVersion::new(
            NonZeroU32::new(u32::try_from(row.handler_version).map_err(invalid_number)?)
                .ok_or_else(|| PostgresError::InvalidTask("handler version is zero".to_owned()))?,
        );
        task.queue_name =
            QueueName::new(row.queue_name).map_err(|error| PostgresError::InvalidTask(error.to_string()))?;
        task.priority = row.priority;
        task.max_attempts = u16::try_from(row.max_attempts).map_err(invalid_number)?;
        task.headers = headers;
        Ok(Self {
            config: ScheduleConfig {
                id: ScheduleId::from_uuid(row.id),
                name: ScheduleName::new(row.name).map_err(|error| PostgresError::InvalidTask(error.to_string()))?,
                definition,
                misfire_policy,
                task,
                start_at: None,
            },
            next_run_at: row.next_run_at,
            paused_at: row.paused_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

impl From<EnqueueRow> for EnqueueResult {
    fn from(row: EnqueueRow) -> Self {
        Self {
            task_id: TaskId::from_uuid(row.task_id),
            created: row.created,
        }
    }
}

#[derive(FromRow)]
struct TaskRow {
    id: Uuid,
    parent_task_id: Option<Uuid>,
    queue_name: String,
    task_name: String,
    handler_version: i32,
    payload: Value,
    headers: Value,
    state: String,
    priority: i16,
    run_at: DateTime<Utc>,
    attempt: i32,
    max_attempts: i32,
    retry_kind: Option<String>,
    retry_base_delay_milliseconds: Option<i64>,
    retry_factor: Option<i32>,
    retry_max_delay_milliseconds: Option<i64>,
    lease_token: Option<Uuid>,
    lease_owner: Option<Uuid>,
    lease_expires_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    result: Option<Value>,
    error: Option<Value>,
}

#[derive(FromRow)]
struct CancelledTaskRow {
    queue_name: String,
    task_name: String,
}

impl TryFrom<TaskRow> for Task {
    type Error = PostgresError;

    fn try_from(row: TaskRow) -> Result<Self, Self::Error> {
        let headers = row
            .headers
            .as_object()
            .cloned()
            .ok_or_else(|| PostgresError::InvalidTask("headers are not an object".to_owned()))?;
        Ok(Self {
            id: TaskId::from_uuid(row.id),
            parent_task_id: row.parent_task_id.map(TaskId::from_uuid),
            queue_name: QueueName::new(row.queue_name)
                .map_err(|error| PostgresError::InvalidTask(error.to_string()))?,
            task_name: TaskName::new(row.task_name).map_err(|error| PostgresError::InvalidTask(error.to_string()))?,
            handler_version: HandlerVersion::new(
                NonZeroU32::new(u32::try_from(row.handler_version).map_err(invalid_number)?)
                    .ok_or_else(|| PostgresError::InvalidTask("handler version is zero".to_owned()))?,
            ),
            payload: row.payload,
            headers,
            state: parse_state(&row.state)?,
            priority: row.priority,
            run_at: row.run_at,
            attempt: u16::try_from(row.attempt).map_err(invalid_number)?,
            max_attempts: u16::try_from(row.max_attempts).map_err(invalid_number)?,
            retry_policy: parse_retry_policy(
                row.retry_kind.as_deref(),
                row.retry_base_delay_milliseconds,
                row.retry_factor,
                row.retry_max_delay_milliseconds,
            )?,
            lease_token: row.lease_token.map(LeaseToken::from_uuid),
            lease_owner: row.lease_owner.map(WorkerId::from_uuid),
            lease_expires_at: row.lease_expires_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
            completed_at: row.completed_at,
            result: row.result,
            error: row.error,
        })
    }
}

fn parse_state(value: &str) -> Result<TaskState, PostgresError> {
    match value {
        "pending" => Ok(TaskState::Pending),
        "running" => Ok(TaskState::Running),
        "waiting" => Ok(TaskState::Waiting),
        "succeeded" => Ok(TaskState::Succeeded),
        "failed" => Ok(TaskState::Failed),
        "cancelled" => Ok(TaskState::Cancelled),
        other => Err(PostgresError::InvalidTask(format!("unknown state {other:?}"))),
    }
}

struct RetryPolicyColumns {
    kind: &'static str,
    base_delay: Option<i64>,
    factor: Option<i32>,
    max_delay: Option<i64>,
}

fn retry_policy_columns(policy: RetryPolicy) -> Result<RetryPolicyColumns, PostgresError> {
    let milliseconds =
        |duration: Duration| i64::try_from(duration.as_millis()).map_err(|_| PostgresError::InvalidRetryPolicy);
    match policy {
        RetryPolicy::Never => Ok(RetryPolicyColumns {
            kind: "never",
            base_delay: None,
            factor: None,
            max_delay: None,
        }),
        RetryPolicy::Fixed { delay } => Ok(RetryPolicyColumns {
            kind: "fixed",
            base_delay: Some(milliseconds(delay)?),
            factor: None,
            max_delay: None,
        }),
        RetryPolicy::Exponential {
            base_delay,
            factor,
            max_delay,
        } => Ok(RetryPolicyColumns {
            kind: "exponential",
            base_delay: Some(milliseconds(base_delay)?),
            factor: Some(i32::try_from(factor).map_err(|_| PostgresError::InvalidRetryPolicy)?),
            max_delay: Some(milliseconds(max_delay)?),
        }),
    }
}

fn parse_retry_policy(
    kind: Option<&str>,
    base_delay_milliseconds: Option<i64>,
    factor: Option<i32>,
    max_delay_milliseconds: Option<i64>,
) -> Result<Option<RetryPolicy>, PostgresError> {
    let duration = |milliseconds: i64| {
        u64::try_from(milliseconds)
            .map(Duration::from_millis)
            .map_err(invalid_number)
    };
    match (kind, base_delay_milliseconds, factor, max_delay_milliseconds) {
        (None, None, None, None) => Ok(None),
        (Some("never"), None, None, None) => Ok(Some(RetryPolicy::Never)),
        (Some("fixed"), Some(delay), None, None) => Ok(Some(RetryPolicy::Fixed {
            delay: duration(delay)?,
        })),
        (Some("exponential"), Some(base_delay), Some(factor), Some(max_delay)) => Ok(Some(RetryPolicy::Exponential {
            base_delay: duration(base_delay)?,
            factor: u32::try_from(factor).map_err(invalid_number)?,
            max_delay: duration(max_delay)?,
        })),
        _ => Err(PostgresError::InvalidTask("invalid retry policy".to_owned())),
    }
}

fn invalid_number(error: impl std::fmt::Display) -> PostgresError {
    PostgresError::InvalidTask(error.to_string())
}
