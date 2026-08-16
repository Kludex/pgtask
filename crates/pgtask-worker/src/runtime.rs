use std::{
    collections::HashMap,
    net::SocketAddr,
    num::NonZeroU16,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
    time::Duration,
};

use futures::FutureExt;
use pgtask_core::{
    HandlerVersion, LeaseRenewal, QueueName, ScheduleConfig, Task, TaskId, TaskName, TaskState, WorkerId,
};
use pgtask_postgres::{PostgresError, ReadyListener, Store};
use serde_json::json;
use thiserror::Error;
use tokio::{
    sync::{Mutex, Notify},
    task::{JoinError, JoinSet},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span, warn};

use crate::{
    HandlerRegistry,
    health::{Health, Supervisor},
    registry::RegisteredHandler,
};

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub queue_name: QueueName,
    pub concurrency: NonZeroU16,
    pub claim_batch_size: NonZeroU16,
    pub lease_duration: Duration,
    pub poll_interval: Duration,
    pub shutdown_grace: Duration,
    pub worker_heartbeat_interval: Duration,
    pub worker_ttl: Duration,
    pub scheduler_enabled: bool,
    pub schedule_batch_size: NonZeroU16,
    pub wait_batch_size: NonZeroU16,
    pub schedule_reconciliation_interval: Duration,
    pub retention_enabled: bool,
    pub retention_batch_size: NonZeroU16,
    pub retention_interval: Duration,
    pub declared_schedules: Vec<ScheduleConfig>,
    pub health_address: Option<SocketAddr>,
    pub supervisor_interval: Duration,
    pub overload_protection: OverloadProtectionConfig,
}

#[derive(Clone, Debug)]
pub struct OverloadProtectionConfig {
    pub enabled: bool,
    pub enforce: bool,
    pub event_loop_lag_threshold: Duration,
    pub sustained_samples: NonZeroU16,
    pub recovery_samples: NonZeroU16,
    pub minimum_concurrency: NonZeroU16,
}

impl Default for OverloadProtectionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            enforce: false,
            event_loop_lag_threshold: Duration::from_millis(250),
            sustained_samples: NonZeroU16::new(3).expect("3 is nonzero"),
            recovery_samples: NonZeroU16::new(5).expect("5 is nonzero"),
            minimum_concurrency: NonZeroU16::MIN,
        }
    }
}

impl WorkerConfig {
    pub fn new(queue_name: QueueName) -> Self {
        Self {
            queue_name,
            concurrency: NonZeroU16::new(10).expect("10 is nonzero"),
            claim_batch_size: NonZeroU16::new(10).expect("10 is nonzero"),
            lease_duration: Duration::from_secs(30),
            poll_interval: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(30),
            worker_heartbeat_interval: Duration::from_secs(10),
            worker_ttl: Duration::from_secs(30),
            scheduler_enabled: true,
            schedule_batch_size: NonZeroU16::new(100).expect("100 is nonzero"),
            wait_batch_size: NonZeroU16::new(100).expect("100 is nonzero"),
            schedule_reconciliation_interval: Duration::from_secs(30),
            retention_enabled: true,
            retention_batch_size: NonZeroU16::new(100).expect("100 is nonzero"),
            retention_interval: Duration::from_mins(1),
            declared_schedules: Vec::new(),
            health_address: None,
            supervisor_interval: Duration::from_secs(1),
            overload_protection: OverloadProtectionConfig::default(),
        }
    }
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    Postgres(#[from] PostgresError),
    #[error("lease duration must be at least three milliseconds")]
    InvalidLeaseDuration,
    #[error("poll interval must be greater than zero")]
    InvalidPollInterval,
    #[error("worker heartbeat interval must be nonzero and shorter than its time to live")]
    InvalidWorkerHeartbeat,
    #[error("schedule reconciliation interval must be greater than zero")]
    InvalidScheduleReconciliationInterval,
    #[error("retention interval must be greater than zero")]
    InvalidRetentionInterval,
    #[error("supervisor interval must be greater than zero")]
    InvalidSupervisorInterval,
    #[error("overload protection minimum concurrency exceeds configured concurrency")]
    InvalidMinimumConcurrency,
    #[error("worker supervisor failed: {0}")]
    Supervisor(#[source] std::io::Error),
    #[error("database storage protocol {database} is incompatible with worker protocol {worker}")]
    IncompatibleStorageProtocol { database: u32, worker: u32 },
    #[error("worker has no registered handlers")]
    MissingHandlers,
    #[error("effective concurrency {requested} exceeds configured concurrency {configured}")]
    AdmissionLimitExceedsConfigured { requested: u16, configured: u16 },
    #[error("declared schedule {0} targets another queue or an unregistered handler")]
    InvalidDeclaredSchedule(String),
    #[error("claimed task {0} has no lease token")]
    MissingLeaseToken(pgtask_core::TaskId),
    #[error("claimed task has no registered handler")]
    MissingHandler,
}

pub struct Worker {
    store: Store,
    registry: Arc<HandlerRegistry>,
    config: WorkerConfig,
    control: WorkerControl,
    health: Health,
    id: WorkerId,
}

#[derive(Clone)]
pub struct WorkerControl {
    configured: NonZeroU16,
    effective: Arc<AtomicU16>,
    proposed: Arc<AtomicU16>,
    changed: Arc<Notify>,
    queue_name: QueueName,
}

impl WorkerControl {
    pub fn configured_concurrency(&self) -> NonZeroU16 {
        self.configured
    }

    pub fn effective_concurrency(&self) -> NonZeroU16 {
        NonZeroU16::new(self.effective.load(Ordering::Acquire)).expect("the admission limit is always nonzero")
    }

    pub fn proposed_concurrency(&self) -> NonZeroU16 {
        NonZeroU16::new(self.proposed.load(Ordering::Acquire)).expect("the proposed admission limit is always nonzero")
    }

    pub fn set_effective_concurrency(&self, limit: NonZeroU16) -> Result<(), WorkerError> {
        self.apply_effective_concurrency(limit, "manual")
    }

    pub(crate) fn apply_effective_concurrency(
        &self,
        limit: NonZeroU16,
        reason: &'static str,
    ) -> Result<(), WorkerError> {
        if limit > self.configured {
            return Err(WorkerError::AdmissionLimitExceedsConfigured {
                requested: limit.get(),
                configured: self.configured.get(),
            });
        }
        let previous = self.effective.swap(limit.get(), Ordering::AcqRel);
        if previous != limit.get() {
            pgtask_otel::record_worker_admission_limit(self.queue_name.as_str(), "applied", reason, limit.get());
        }
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn record_proposed_concurrency(&self, limit: NonZeroU16, reason: &'static str) {
        let previous = self.proposed.swap(limit.get(), Ordering::AcqRel);
        if previous != limit.get() {
            pgtask_otel::record_worker_admission_limit(self.queue_name.as_str(), "proposed", reason, limit.get());
        }
    }
}

type ActiveLeases = Arc<Mutex<HashMap<TaskId, ActiveLease>>>;

#[derive(Clone)]
struct ActiveLease {
    renewal: LeaseRenewal,
    queue_name: QueueName,
    task_name: TaskName,
    lost: CancellationToken,
    last_renewed: Instant,
}

struct HeartbeatConfig {
    worker_id: WorkerId,
    queue_name: QueueName,
    capabilities: Vec<(TaskName, HandlerVersion)>,
    interval: Duration,
    ttl: Duration,
}

impl Worker {
    pub fn new(store: Store, registry: HandlerRegistry, config: WorkerConfig) -> Result<Self, WorkerError> {
        if config.lease_duration < Duration::from_millis(3) {
            return Err(WorkerError::InvalidLeaseDuration);
        }
        if config.poll_interval.is_zero() {
            return Err(WorkerError::InvalidPollInterval);
        }
        if config.worker_heartbeat_interval.is_zero() || config.worker_heartbeat_interval >= config.worker_ttl {
            return Err(WorkerError::InvalidWorkerHeartbeat);
        }
        if config.schedule_reconciliation_interval.is_zero() {
            return Err(WorkerError::InvalidScheduleReconciliationInterval);
        }
        if config.retention_interval.is_zero() {
            return Err(WorkerError::InvalidRetentionInterval);
        }
        if config.supervisor_interval.is_zero() {
            return Err(WorkerError::InvalidSupervisorInterval);
        }
        if config.overload_protection.minimum_concurrency > config.concurrency {
            return Err(WorkerError::InvalidMinimumConcurrency);
        }
        if registry.capabilities().is_empty() {
            return Err(WorkerError::MissingHandlers);
        }
        if let Some(schedule) = config.declared_schedules.iter().find(|schedule| {
            schedule.task.queue_name != config.queue_name
                || registry
                    .get(&schedule.task.task_name, schedule.task.handler_version)
                    .is_none()
        }) {
            return Err(WorkerError::InvalidDeclaredSchedule(schedule.name.to_string()));
        }
        let control = WorkerControl {
            configured: config.concurrency,
            effective: Arc::new(AtomicU16::new(config.concurrency.get())),
            proposed: Arc::new(AtomicU16::new(config.concurrency.get())),
            changed: Arc::new(Notify::new()),
            queue_name: config.queue_name.clone(),
        };
        Ok(Self {
            store,
            registry: Arc::new(registry),
            config,
            control,
            health: Health::new(),
            id: WorkerId::new(),
        })
    }

    pub fn control(&self) -> WorkerControl {
        self.control.clone()
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), WorkerError> {
        let database_protocol = self.store.storage_protocol_version().await?;
        if database_protocol != pgtask_core::STORAGE_PROTOCOL_VERSION {
            return Err(WorkerError::IncompatibleStorageProtocol {
                database: database_protocol,
                worker: pgtask_core::STORAGE_PROTOCOL_VERSION,
            });
        }
        let _supervisor = Supervisor::start(
            self.health.clone(),
            self.config.queue_name.clone(),
            self.config.supervisor_interval,
            self.config.health_address,
            self.control.clone(),
            self.config.overload_protection.clone(),
            self.config.lease_duration * 2 / 3,
        )
        .map_err(WorkerError::Supervisor)?;
        let active_leases = Arc::new(Mutex::new(HashMap::new()));
        let task_wakeup = Arc::new(Notify::new());
        let schedule_wakeup = Arc::new(Notify::new());
        let runtime_shutdown = CancellationToken::new();
        let capabilities = self.registry.capabilities();
        let registrations = self.registry.registrations();
        let ready_listener = self.store.ready_listener(&self.config.queue_name).await?;
        self.health.set_listener(true);
        for schedule in &self.config.declared_schedules {
            self.store.put_schedule(schedule).await?;
        }
        self.store
            .register_worker_with_policies(
                self.id,
                &self.config.queue_name,
                env!("CARGO_PKG_VERSION"),
                &registrations,
                self.config.worker_ttl,
            )
            .await?;
        self.health.set_database(true);
        self.health.set_admission(true);
        let renewer = renew_leases(
            self.store.clone(),
            Arc::clone(&active_leases),
            self.health.clone(),
            self.config.lease_duration,
            runtime_shutdown.clone(),
        );
        let listener = listen_for_ready(
            self.store.clone(),
            self.config.queue_name.clone(),
            Arc::clone(&task_wakeup),
            Arc::clone(&schedule_wakeup),
            runtime_shutdown.clone(),
            ready_listener,
            self.health.clone(),
        );
        let scheduler = materialize_schedules(
            self.store.clone(),
            self.config.scheduler_enabled,
            self.config.schedule_batch_size,
            self.config.wait_batch_size,
            self.config.schedule_reconciliation_interval,
            schedule_wakeup,
            runtime_shutdown.clone(),
        );
        let retention = delete_expired_terminal(
            self.store.clone(),
            self.config.queue_name.clone(),
            self.config.retention_enabled,
            self.config.retention_batch_size,
            self.config.retention_interval,
            runtime_shutdown.clone(),
        );
        let heartbeat = heartbeat_worker(
            self.store.clone(),
            HeartbeatConfig {
                worker_id: self.id,
                queue_name: self.config.queue_name.clone(),
                capabilities: capabilities.clone(),
                interval: self.config.worker_heartbeat_interval,
                ttl: self.config.worker_ttl,
            },
            runtime_shutdown.clone(),
            self.health.clone(),
        );
        let handlers = async {
            let result = self
                .run_handlers(shutdown, Arc::clone(&active_leases), task_wakeup)
                .await;
            runtime_shutdown.cancel();
            self.health.set_admission(false);
            result
        };
        let ((), (), (), (), (), result) = tokio::join!(renewer, listener, scheduler, retention, heartbeat, handlers);
        result
    }

    async fn run_handlers(
        &self,
        shutdown: CancellationToken,
        active_leases: ActiveLeases,
        wakeup: Arc<Notify>,
    ) -> Result<(), WorkerError> {
        let mut handlers = JoinSet::new();
        let capabilities = self.registry.capabilities();
        loop {
            self.health.record_runtime_progress();
            while let Some(result) = handlers.try_join_next() {
                handle_handler_result(result);
            }
            if shutdown.is_cancelled() {
                break;
            }

            let Some((limit, tasks)) = self
                .claim_tasks(&shutdown, &wakeup, handlers.len(), &capabilities)
                .await
            else {
                continue;
            };
            let claimed_any = !tasks.is_empty();
            for task in tasks {
                self.spawn_task(&mut handlers, &active_leases, task).await?;
            }

            if !claimed_any {
                let deadline_delay = if limit == 0 {
                    self.config.poll_interval
                } else {
                    match self.store.next_task_delay(&self.config.queue_name, &capabilities).await {
                        Ok(delay) => delay
                            .unwrap_or(self.config.poll_interval)
                            .min(self.config.poll_interval)
                            .max(Duration::from_millis(1)),
                        Err(error) => {
                            self.health.set_database(false);
                            warn!(%error, "could not read the next task deadline");
                            Duration::from_secs(1).min(self.config.poll_interval)
                        }
                    }
                };
                if handlers.is_empty() {
                    tokio::select! {
                        () = shutdown.cancelled() => break,
                        () = self.control.changed.notified() => {}
                        () = wakeup.notified() => {}
                        () = tokio::time::sleep(deadline_delay) => {}
                    }
                } else {
                    tokio::select! {
                        () = shutdown.cancelled() => break,
                        () = self.control.changed.notified() => {}
                        result = handlers.join_next() => handle_handler_result(
                            result.expect("a nonempty handler set returns one task"),
                        ),
                        () = wakeup.notified() => {}
                        () = tokio::time::sleep(deadline_delay) => {}
                    }
                }
            }
        }

        let deadline = Instant::now() + self.config.shutdown_grace;
        while !handlers.is_empty() {
            tokio::select! {
                result = handlers.join_next() => handle_handler_result(
                    result.expect("a nonempty handler set returns one task"),
                ),
                () = tokio::time::sleep_until(deadline) => {
                    handlers.abort_all();
                    break;
                }
            }
        }
        active_leases.lock().await.clear();
        Ok(())
    }

    async fn claim_tasks(
        &self,
        shutdown: &CancellationToken,
        wakeup: &Notify,
        active_handlers: usize,
        capabilities: &[(TaskName, HandlerVersion)],
    ) -> Option<(usize, Vec<Task>)> {
        let effective_concurrency = self.control.effective_concurrency().get();
        pgtask_otel::record_worker_capacity(
            self.config.queue_name.as_str(),
            self.config.concurrency.get(),
            effective_concurrency,
            active_handlers,
        );
        if let Err(error) = self
            .store
            .recover_expired(&self.config.queue_name, self.config.claim_batch_size.get())
            .await
        {
            self.health.set_database(false);
            warn!(%error, "could not recover expired task leases");
            wait_after_database_error(shutdown, wakeup).await;
            return None;
        }
        self.health.set_database(true);
        let available = usize::from(effective_concurrency).saturating_sub(active_handlers);
        let limit = available.min(usize::from(self.config.claim_batch_size.get()));
        if limit == 0 {
            return Some((limit, Vec::new()));
        }
        match self
            .store
            .claim(
                &self.config.queue_name,
                self.id,
                capabilities,
                u16::try_from(limit).expect("limit is bounded by a u16 configuration value"),
                self.config.lease_duration,
            )
            .await
        {
            Ok(tasks) => {
                self.health.set_database(true);
                Some((limit, tasks))
            }
            Err(error) => {
                self.health.set_database(false);
                warn!(%error, "could not claim tasks");
                wait_after_database_error(shutdown, wakeup).await;
                None
            }
        }
    }

    async fn spawn_task(
        &self,
        handlers: &mut JoinSet<Result<(), PostgresError>>,
        active_leases: &ActiveLeases,
        task: Task,
    ) -> Result<(), WorkerError> {
        let lease_token = task.lease_token.ok_or(WorkerError::MissingLeaseToken(task.id))?;
        let handler = self
            .registry
            .get(&task.task_name, task.handler_version)
            .ok_or(WorkerError::MissingHandler)?
            .clone();
        let lost = CancellationToken::new();
        active_leases.lock().await.insert(
            task.id,
            ActiveLease {
                renewal: LeaseRenewal {
                    task_id: task.id,
                    attempt: task.attempt,
                    lease_token,
                },
                queue_name: task.queue_name.clone(),
                task_name: task.task_name.clone(),
                lost: lost.clone(),
                last_renewed: Instant::now(),
            },
        );
        self.health.set_active_leases(true);
        let span = info_span!(
            "pgtask.execute",
            otel.kind = "consumer",
            pgtask.task.id = %task.id,
            pgtask.task.name = %task.task_name,
            pgtask.task.attempt = task.attempt,
            pgtask.queue.name = %task.queue_name,
        );
        pgtask_otel::set_parent_from_headers(&span, &task.headers)
            .unwrap_or_else(|error| warn!(%error, "could not attach the producer trace context"));
        let active_leases = Arc::clone(active_leases);
        let store = self.store.clone();
        let health = self.health.clone();
        handlers.spawn(
            async move {
                let task_id = task.id;
                let result = execute(store, handler, task, lease_token, lost).await;
                let mut leases = active_leases.lock().await;
                leases.remove(&task_id);
                health.set_active_leases(!leases.is_empty());
                result
            }
            .instrument(span),
        );
        Ok(())
    }
}

fn handle_handler_result(result: Result<Result<(), PostgresError>, JoinError>) {
    if let Err(error) = result.expect("engine execution tasks do not panic") {
        warn!(%error, "task state transition failed; its lease will be recovered");
    }
}

async fn wait_after_database_error(shutdown: &CancellationToken, wakeup: &Notify) {
    tokio::select! {
        () = shutdown.cancelled() => {}
        () = wakeup.notified() => {}
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
}

async fn execute(
    store: Store,
    handler: RegisteredHandler,
    task: Task,
    lease_token: pgtask_core::LeaseToken,
    lease_lost: CancellationToken,
) -> Result<(), PostgresError> {
    let queue_latency = task
        .updated_at
        .signed_duration_since(task.created_at)
        .to_std()
        .unwrap_or_default();
    pgtask_otel::record_queue_latency(task.queue_name.as_str(), task.task_name.as_str(), queue_latency);
    let started_at = std::time::Instant::now();
    let context = crate::TaskContext::new(store.clone(), &task, lease_token, lease_lost.clone());
    let handler_future = AssertUnwindSafe((handler.function)(task.clone(), context)).catch_unwind();
    tokio::pin!(handler_future);

    tokio::select! {
        result = &mut handler_future => {
            match result {
                    Ok(Ok(result)) => {
                        if store.complete(task.id, task.attempt, lease_token, Some(&result)).await? {
                            pgtask_otel::record_succeeded(task.queue_name.as_str(), task.task_name.as_str());
                            pgtask_otel::record_execution(
                                task.queue_name.as_str(),
                                task.task_name.as_str(),
                                "succeeded",
                                started_at.elapsed(),
                            );
                        } else {
                            pgtask_otel::record_lease_lost(task.queue_name.as_str(), task.task_name.as_str());
                            warn!("task completion lost its lease");
                        }
                    }
                    Ok(Err(error)) => {
                        if error.is_suspended() {
                            pgtask_otel::record_execution(
                                task.queue_name.as_str(),
                                task.task_name.as_str(),
                                "suspended",
                                started_at.elapsed(),
                            );
                            return Ok(());
                        }
                        let retry_after = if error.retryable {
                            task.retry_policy.unwrap_or(handler.retry_policy).delay_for(task.attempt)
                        } else {
                            None
                        };
                        let state = store.fail(task.id, task.attempt, lease_token, &error.error, retry_after).await?;
                        if state.is_none() {
                            pgtask_otel::record_lease_lost(task.queue_name.as_str(), task.task_name.as_str());
                            warn!("task failure lost its lease");
                        } else if state == Some(TaskState::Pending) {
                            tracing::debug!("task scheduled for retry");
                        }
                        record_failure_state(&task, state);
                        pgtask_otel::record_execution(
                            task.queue_name.as_str(),
                            task.task_name.as_str(),
                            if state == Some(TaskState::Pending) { "retry" } else { "failed" },
                            started_at.elapsed(),
                        );
                    }
                    Err(_) => {
                        let error = json!({"type": "handler_panic"});
                        let state = store
                            .fail(
                                task.id,
                                task.attempt,
                                lease_token,
                                &error,
                                task.retry_policy.unwrap_or(handler.retry_policy).delay_for(task.attempt),
                            )
                            .await?;
                        if state.is_none() {
                            pgtask_otel::record_lease_lost(task.queue_name.as_str(), task.task_name.as_str());
                            warn!("panicked task lost its lease");
                        }
                        record_failure_state(&task, state);
                        pgtask_otel::record_execution(
                            task.queue_name.as_str(),
                            task.task_name.as_str(),
                            "panic",
                            started_at.elapsed(),
                        );
                    }
            }
        }
        () = lease_lost.cancelled() => {
            pgtask_otel::record_execution(
                task.queue_name.as_str(),
                task.task_name.as_str(),
                "lease_lost",
                started_at.elapsed(),
            );
            warn!("task lost its lease during execution");
        }
    }
    Ok(())
}

fn record_failure_state(task: &Task, state: Option<TaskState>) {
    match state {
        Some(TaskState::Pending) => pgtask_otel::record_retried(task.queue_name.as_str(), task.task_name.as_str()),
        Some(_) => pgtask_otel::record_failed(task.queue_name.as_str(), task.task_name.as_str()),
        None => {}
    }
}

async fn renew_leases(
    store: Store,
    active: ActiveLeases,
    health: Health,
    lease_duration: Duration,
    shutdown: CancellationToken,
) {
    let renewal_interval = lease_duration / 3;
    let mut interval = tokio::time::interval_at(Instant::now() + renewal_interval, renewal_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = interval.tick() => {
                let leases: Vec<_> = active.lock().await.values().cloned().collect();
                if leases.is_empty() {
                    health.set_active_leases(false);
                    continue;
                }
                match store.renew_leases(
                    &leases.iter().map(|lease| lease.renewal).collect::<Vec<_>>(),
                    lease_duration,
                ).await {
                    Ok(renewed) => {
                        health.set_database(true);
                        health.record_lease_renewal(renewed.len() == leases.len());
                        update_renewed_leases(&active, &leases, &renewed).await;
                    }
                    Err(error) => {
                        health.set_database(false);
                        health.record_lease_renewal(false);
                        warn!(%error, "could not renew active task leases");
                        cancel_uncertain_leases(&active, &leases, lease_duration).await;
                    }
                }
            }
        }
    }
}

async fn update_renewed_leases(active: &ActiveLeases, leases: &[ActiveLease], renewed: &[TaskId]) {
    let now = Instant::now();
    let mut active = active.lock().await;
    for lease in leases {
        let was_renewed = renewed.contains(&lease.renewal.task_id);
        pgtask_otel::record_renewed(lease.queue_name.as_str(), lease.task_name.as_str(), was_renewed);
        if let Some(current) = active.get_mut(&lease.renewal.task_id)
            && current.renewal == lease.renewal
        {
            if was_renewed {
                current.last_renewed = now;
            } else {
                current.lost.cancel();
                pgtask_otel::record_lease_lost(lease.queue_name.as_str(), lease.task_name.as_str());
            }
        }
    }
}

async fn cancel_uncertain_leases(active: &ActiveLeases, leases: &[ActiveLease], lease_duration: Duration) {
    let mut active = active.lock().await;
    for lease in leases {
        if lease.last_renewed.elapsed() >= lease_duration * 2 / 3
            && let Some(current) = active.get_mut(&lease.renewal.task_id)
            && current.renewal == lease.renewal
        {
            current.lost.cancel();
            pgtask_otel::record_lease_lost(lease.queue_name.as_str(), lease.task_name.as_str());
        }
    }
}

async fn listen_for_ready(
    store: Store,
    queue_name: QueueName,
    task_wakeup: Arc<Notify>,
    schedule_wakeup: Arc<Notify>,
    shutdown: CancellationToken,
    mut listener: ReadyListener,
    health: Health,
) {
    let mut retry_delay = Duration::from_millis(100);
    loop {
        loop {
            let notification = tokio::select! {
                () = shutdown.cancelled() => return,
                result = listener.recv() => result,
            };
            match notification {
                Ok(notification)
                    if notification.channel().starts_with("pgtask_ready_")
                        && notification.payload() == queue_name.as_str() =>
                {
                    task_wakeup.notify_one();
                }
                Ok(notification) if matches!(notification.channel(), "pgtask_schedule" | "pgtask_wait") => {
                    schedule_wakeup.notify_one();
                }
                Ok(_) => {}
                Err(error) => {
                    health.set_listener(false);
                    warn!(%error, "task notification listener disconnected");
                    break;
                }
            }
        }
        loop {
            let reconnected = tokio::select! {
                () = shutdown.cancelled() => return,
                result = store.ready_listener(&queue_name) => result,
            };
            match reconnected {
                Ok(reconnected) => {
                    listener = reconnected;
                    health.set_database(true);
                    health.set_listener(true);
                    retry_delay = Duration::from_millis(100);
                    task_wakeup.notify_one();
                    schedule_wakeup.notify_one();
                    break;
                }
                Err(error) => {
                    health.set_database(false);
                    warn!(%error, "could not reconnect the task notification listener");
                    tokio::select! {
                        () = shutdown.cancelled() => return,
                        () = tokio::time::sleep(retry_delay) => {}
                    }
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
                }
            }
        }
    }
}

async fn materialize_schedules(
    store: Store,
    enabled: bool,
    schedule_batch_size: NonZeroU16,
    wait_batch_size: NonZeroU16,
    reconciliation_interval: Duration,
    wakeup: Arc<Notify>,
    shutdown: CancellationToken,
) {
    loop {
        if enabled && let Err(error) = store.materialize_due_schedules(schedule_batch_size.get()).await {
            warn!(%error, "could not materialize due schedules");
        }
        if let Err(error) = store.recover_wait_timeouts(wait_batch_size.get()).await {
            warn!(%error, "could not recover signal wait timeouts");
        }
        if let Err(error) = store.recover_result_wait_timeouts(wait_batch_size.get()).await {
            warn!(%error, "could not recover result wait timeouts");
        }
        let mut delay = reconciliation_interval;
        if enabled {
            match store.next_schedule_delay().await {
                Ok(schedule_delay) => {
                    if let Some(schedule_delay) = schedule_delay {
                        delay = delay.min(schedule_delay);
                    }
                }
                Err(error) => warn!(%error, "could not read the next schedule deadline"),
            }
        }
        match store.next_wait_delay().await {
            Ok(wait_delay) => {
                if let Some(wait_delay) = wait_delay {
                    delay = delay.min(wait_delay);
                }
            }
            Err(error) => warn!(%error, "could not read the next wait deadline"),
        }
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = wakeup.notified() => {}
            () = tokio::time::sleep(delay) => {}
        }
    }
}

async fn delete_expired_terminal(
    store: Store,
    queue_name: QueueName,
    enabled: bool,
    batch_size: NonZeroU16,
    retention_interval: Duration,
    shutdown: CancellationToken,
) {
    let mut interval = tokio::time::interval(retention_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = interval.tick() => {
                if enabled
                    && let Err(error) = store.delete_expired_terminal(&queue_name, batch_size.get()).await
                {
                    warn!(%error, "could not delete expired terminal tasks");
                }
                if enabled
                    && let Err(error) = store.delete_expired_idempotency_keys(&queue_name, batch_size.get()).await
                {
                    warn!(%error, "could not delete expired idempotency keys");
                }
            }
        }
    }
}

async fn heartbeat_worker(store: Store, config: HeartbeatConfig, shutdown: CancellationToken, health: Health) {
    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                if let Err(error) = store.heartbeat_worker(config.worker_id, Duration::from_millis(1), true).await {
                    warn!(%error, "could not mark worker as stopped");
                }
                break;
            }
            _ = interval.tick() => {
                match store.heartbeat_worker(config.worker_id, config.ttl, false).await {
                    Ok(true) => health.set_database(true),
                    Ok(false) => {
                        health.set_database(false);
                        warn!("worker registration disappeared");
                    }
                    Err(error) => {
                        health.set_database(false);
                        warn!(%error, "could not update worker heartbeat");
                    }
                }
                match store.queue_demand(&config.queue_name, &config.capabilities).await {
                    Ok(demand) => pgtask_otel::record_queue_demand(
                        config.queue_name.as_str(),
                        demand.capable_tasks,
                        demand.unroutable_tasks,
                    ),
                    Err(error) => {
                        health.set_database(false);
                        warn!(%error, "could not read queue demand");
                    }
                }
            }
        }
    }
}
