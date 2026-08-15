use std::{
    error::Error,
    num::NonZeroU16,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use pgtask::{
    core::{
        EnqueueRequest, HandlerVersion, QueueConfig, QueueName, RetryPolicy, ScheduleConfig, ScheduleDefinition,
        ScheduleName, TaskName, TaskState,
    },
    postgres::Store,
    worker::{HandlerError, HandlerRegistry, Worker, WorkerConfig, WorkerError},
};
use serde::Serialize;
use serde_json::json;
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Serialize)]
struct Report {
    scenario: String,
    tasks: usize,
    batch_size: usize,
    workers: usize,
    concurrency_per_worker: u16,
    handler_invocations: usize,
    enqueue_seconds: f64,
    enqueue_tasks_per_second: f64,
    drain_seconds: f64,
    drain_tasks_per_second: f64,
    cleanup_seconds: Option<f64>,
    cleanup_tasks: Option<u64>,
}

#[derive(Clone, Copy)]
enum Scenario {
    CpuBound,
    Noop,
    DelayedBurst,
    DatabaseDisconnect,
    IoBound,
    MultiScheduler,
    RateLimited,
    RetainedHistory,
    RetryStorm,
    WorkerDeath,
}

struct Configuration {
    task_count: usize,
    batch_size: usize,
    worker_count: usize,
    concurrency: u16,
    timeout: Duration,
    scenario: Scenario,
    retry_attempts: u16,
}

struct Progress {
    handler_invocations: Arc<AtomicUsize>,
    rate_limiter: Arc<tokio::sync::Mutex<tokio::time::Interval>>,
}

impl Progress {
    fn new() -> Self {
        Self {
            handler_invocations: Arc::new(AtomicUsize::new(0)),
            rate_limiter: Arc::new(tokio::sync::Mutex::new(tokio::time::interval(Duration::from_millis(2)))),
        }
    }
}

impl Configuration {
    fn from_environment() -> Result<Self, Box<dyn Error>> {
        let config = Self {
            task_count: environment_usize("PGTASK_BENCH_TASKS", 1_000)?,
            batch_size: environment_usize("PGTASK_BENCH_BATCH_SIZE", 100)?,
            worker_count: environment_usize("PGTASK_BENCH_WORKERS", 1)?,
            concurrency: u16::try_from(environment_usize("PGTASK_BENCH_CONCURRENCY", 100)?)?,
            timeout: Duration::from_secs(u64::try_from(environment_usize("PGTASK_BENCH_TIMEOUT_SECONDS", 300)?)?),
            scenario: Scenario::from_environment()?,
            retry_attempts: u16::try_from(environment_usize("PGTASK_BENCH_RETRY_ATTEMPTS", 3)?)?,
        };
        if config.task_count == 0
            || config.batch_size == 0
            || config.worker_count == 0
            || config.concurrency == 0
            || config.timeout.is_zero()
        {
            return Err("task, batch, worker, concurrency, and timeout values must be greater than zero".into());
        }
        if matches!(config.scenario, Scenario::RetryStorm) && config.retry_attempts < 2 {
            return Err("retry storm attempts must be at least two".into());
        }
        if matches!(config.scenario, Scenario::WorkerDeath) && config.worker_count < 2 {
            return Err("worker death requires at least two workers".into());
        }
        Ok(config)
    }
}

impl Scenario {
    fn from_environment() -> Result<Self, Box<dyn Error>> {
        match std::env::var("PGTASK_BENCH_SCENARIO") {
            Ok(value) if value == "noop" => Ok(Self::Noop),
            Ok(value) if value == "cpu-bound" => Ok(Self::CpuBound),
            Ok(value) if value == "delayed-burst" => Ok(Self::DelayedBurst),
            Ok(value) if value == "database-disconnect" => Ok(Self::DatabaseDisconnect),
            Ok(value) if value == "io-bound" => Ok(Self::IoBound),
            Ok(value) if value == "multi-scheduler" => Ok(Self::MultiScheduler),
            Ok(value) if value == "rate-limited" => Ok(Self::RateLimited),
            Ok(value) if value == "retained-history" => Ok(Self::RetainedHistory),
            Ok(value) if value == "retry-storm" => Ok(Self::RetryStorm),
            Ok(value) if value == "worker-death" => Ok(Self::WorkerDeath),
            Ok(value) => Err(format!("unknown benchmark scenario {value:?}").into()),
            Err(std::env::VarError::NotPresent) => Ok(Self::Noop),
            Err(error) => Err(error.into()),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::CpuBound => "cpu-bound",
            Self::Noop => "noop",
            Self::DelayedBurst => "delayed-burst",
            Self::DatabaseDisconnect => "database-disconnect",
            Self::IoBound => "io-bound",
            Self::MultiScheduler => "multi-scheduler",
            Self::RateLimited => "rate-limited",
            Self::RetainedHistory => "retained-history",
            Self::RetryStorm => "retry-storm",
            Self::WorkerDeath => "worker-death",
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database_url = std::env::var("PGTASK_DATABASE_URL")?;
    let config = Configuration::from_environment()?;

    let store = Store::connect(&database_url).await?;
    store.migrate().await?;
    let queue_name = QueueName::new(format!("bench-{}", Uuid::new_v4()))?;
    if matches!(config.scenario, Scenario::RetainedHistory) {
        let mut queue = QueueConfig::new(queue_name.clone());
        queue.terminal_retention = Duration::ZERO;
        store.put_queue(&queue).await?;
    }
    let task_name = TaskName::new("benchmark.noop")?;
    let delayed_until = chrono::Utc::now() + chrono::TimeDelta::seconds(1);
    let requests: Vec<_> = (0..config.task_count)
        .map(|sequence| {
            let mut request = EnqueueRequest::new(task_name.clone(), json!({"sequence": sequence}));
            request.queue_name = queue_name.clone();
            if matches!(config.scenario, Scenario::DelayedBurst) {
                request.run_at = Some(delayed_until);
            }
            if matches!(config.scenario, Scenario::RetryStorm) {
                request.max_attempts = config.retry_attempts;
            }
            request
        })
        .collect();

    let enqueue_started = std::time::Instant::now();
    if matches!(config.scenario, Scenario::MultiScheduler) {
        for (sequence, request) in requests.into_iter().enumerate() {
            let mut schedule = ScheduleConfig::new(
                ScheduleName::new(format!("benchmark-schedule-{sequence}-{}", Uuid::new_v4()))?,
                ScheduleDefinition::interval(Duration::from_hours(1))?,
                request,
            );
            schedule.start_at = Some(chrono::Utc::now());
            store.put_schedule(&schedule).await?;
        }
    } else {
        for batch in requests.chunks(config.batch_size) {
            store.enqueue_many(batch).await?;
        }
    }
    let enqueue_elapsed = enqueue_started.elapsed();

    let progress = Progress::new();
    let shutdown = CancellationToken::new();
    let workers = start_workers(&config, &store, &queue_name, &task_name, &progress, &shutdown).await?;

    let drain_started = std::time::Instant::now();
    tokio::time::timeout(config.timeout, async {
        loop {
            if store
                .task_count_by_state(&queue_name, TaskState::Succeeded)
                .await
                .is_ok_and(|count| count == u64::try_from(config.task_count).unwrap_or(u64::MAX))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    shutdown.cancel();
    for (worker_index, worker) in workers.into_iter().enumerate() {
        match worker.await {
            Err(_) if matches!(config.scenario, Scenario::WorkerDeath) && worker_index == 0 => {}
            result => result??,
        }
    }
    let drain_elapsed = drain_started.elapsed();
    let cleanup = cleanup_history(&store, &queue_name, config.scenario).await?;

    let task_count_float = f64::from(u32::try_from(config.task_count)?);
    let report = Report {
        scenario: config.scenario.name().to_owned(),
        tasks: config.task_count,
        batch_size: config.batch_size,
        workers: config.worker_count,
        concurrency_per_worker: config.concurrency,
        handler_invocations: progress.handler_invocations.load(Ordering::Relaxed),
        enqueue_seconds: enqueue_elapsed.as_secs_f64(),
        enqueue_tasks_per_second: task_count_float / enqueue_elapsed.as_secs_f64(),
        drain_seconds: drain_elapsed.as_secs_f64(),
        drain_tasks_per_second: task_count_float / drain_elapsed.as_secs_f64(),
        cleanup_seconds: cleanup.map(|(duration, _)| duration.as_secs_f64()),
        cleanup_tasks: cleanup.map(|(_, deleted)| deleted),
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

async fn cleanup_history(
    store: &Store,
    queue_name: &QueueName,
    scenario: Scenario,
) -> Result<Option<(Duration, u64)>, Box<dyn Error>> {
    if !matches!(scenario, Scenario::RetainedHistory) {
        return Ok(None);
    }
    let started = std::time::Instant::now();
    let mut deleted = 0_u64;
    loop {
        let batch = store.delete_expired_terminal(queue_name, 1_000).await?;
        deleted += batch;
        if batch == 0 {
            return Ok(Some((started.elapsed(), deleted)));
        }
    }
}

async fn start_workers(
    config: &Configuration,
    store: &Store,
    queue_name: &QueueName,
    task_name: &TaskName,
    progress: &Progress,
    shutdown: &CancellationToken,
) -> Result<Vec<JoinHandle<Result<(), WorkerError>>>, Box<dyn Error>> {
    let mut workers = Vec::with_capacity(config.worker_count);
    for worker_index in 0..config.worker_count {
        let mut registry = HandlerRegistry::new();
        let invocations = Arc::clone(&progress.handler_invocations);
        let rate_limiter = Arc::clone(&progress.rate_limiter);
        let handler_started = Arc::new(Notify::new());
        let registered_started = Arc::clone(&handler_started);
        let scenario = config.scenario;
        let retry_attempts = config.retry_attempts;
        let crash_worker = matches!(scenario, Scenario::WorkerDeath) && worker_index == 0;
        registry.register(
            task_name.clone(),
            HandlerVersion::default(),
            if matches!(scenario, Scenario::RetryStorm) {
                RetryPolicy::Fixed {
                    delay: Duration::from_millis(1),
                }
            } else {
                RetryPolicy::Never
            },
            move |task| {
                let invocations = Arc::clone(&invocations);
                let rate_limiter = Arc::clone(&rate_limiter);
                let handler_started = Arc::clone(&registered_started);
                async move {
                    invocations.fetch_add(1, Ordering::Relaxed);
                    handler_started.notify_one();
                    if crash_worker {
                        return std::future::pending().await;
                    }
                    if matches!(scenario, Scenario::DatabaseDisconnect) {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    if matches!(scenario, Scenario::IoBound) {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    if matches!(scenario, Scenario::RateLimited) {
                        rate_limiter.lock().await.tick().await;
                    }
                    if matches!(scenario, Scenario::CpuBound) {
                        let mut value = u64::from(task.attempt);
                        for sequence in 0_u64..100_000 {
                            value = value.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(sequence);
                        }
                        std::hint::black_box(value);
                    }
                    if matches!(scenario, Scenario::RetryStorm) && task.attempt < retry_attempts {
                        return Err(HandlerError::retryable("benchmark retry"));
                    }
                    Ok(json!(null))
                }
            },
        );
        let mut worker_config = WorkerConfig::new(queue_name.clone());
        worker_config.concurrency = NonZeroU16::new(config.concurrency).ok_or("concurrency must be nonzero")?;
        worker_config.claim_batch_size = worker_config.concurrency;
        worker_config.scheduler_enabled = matches!(scenario, Scenario::MultiScheduler);
        if matches!(scenario, Scenario::WorkerDeath | Scenario::DatabaseDisconnect) {
            worker_config.lease_duration = Duration::from_secs(2);
        }
        let worker = Worker::new(store.clone(), registry, worker_config)?;
        let worker_shutdown = shutdown.clone();
        workers.push(tokio::spawn(async move { worker.run(worker_shutdown).await }));
        if matches!(scenario, Scenario::WorkerDeath) && worker_index == 0 {
            tokio::time::timeout(config.timeout, handler_started.notified()).await?;
            workers[0].abort();
        }
    }
    Ok(workers)
}

fn environment_usize(name: &str, default: usize) -> Result<usize, Box<dyn Error>> {
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}
