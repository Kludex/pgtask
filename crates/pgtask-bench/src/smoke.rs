use std::{
    error::Error,
    net::SocketAddr,
    num::{NonZeroU16, NonZeroU32},
    time::Duration,
};

use pgtask::{
    core::{EnqueueRequest, HandlerVersion, QueueName, RetryPolicy, TaskName, TaskState},
    postgres::{Store, StoreConfig},
    worker::{HandlerRegistry, Worker, WorkerConfig},
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

const TASK_NAME: &str = "pgtask.smoke";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let command = std::env::args().nth(1).ok_or("expected worker or enqueue")?;
    let mut store_config = StoreConfig::new(std::env::var("PGTASK_DATABASE_URL")?);
    if let Ok(listener_url) = std::env::var("PGTASK_LISTENER_DATABASE_URL") {
        store_config = store_config.with_listener_url(listener_url);
    }
    store_config = store_config
        .with_query_connections(environment_nonzero_u32("PGTASK_MAX_QUERY_CONNECTIONS", 10)?)
        .with_listener_connections(environment_nonzero_u32("PGTASK_MAX_LISTENER_CONNECTIONS", 1)?);
    let store = Store::connect_with_config(&store_config).await?;
    match command.as_str() {
        "worker" => run_worker(store).await,
        "enqueue" => enqueue_and_wait(store).await,
        _ => Err(format!("unknown command {command:?}").into()),
    }
}

async fn run_worker(store: Store) -> Result<(), Box<dyn Error>> {
    let queue_name = queue_name()?;
    let handler_delay = Duration::from_millis(u64::from(environment_u16("PGTASK_SMOKE_HANDLER_MILLISECONDS", 0)?));
    let mut registry = HandlerRegistry::new();
    registry.register(
        TaskName::new(TASK_NAME)?,
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |_| async move {
            tokio::time::sleep(handler_delay).await;
            Ok(json!({"worker": "pgtask-smoke"}))
        },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.concurrency = NonZeroU16::new(environment_u16("PGTASK_CONCURRENCY", 10)?)
        .ok_or("PGTASK_CONCURRENCY must be greater than zero")?;
    config.claim_batch_size = config.concurrency;
    config.scheduler_enabled = environment_bool("PGTASK_SCHEDULER_ENABLED", true)?;
    config.lease_duration = Duration::from_secs(u64::from(environment_u16("PGTASK_SMOKE_LEASE_SECONDS", 30)?));
    config.health_address = std::env::var("PGTASK_HEALTH_ADDRESS")
        .ok()
        .map(|value| value.parse::<SocketAddr>())
        .transpose()?;
    let worker = Worker::new(store, registry, config)?;
    let shutdown = CancellationToken::new();
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        signal_shutdown.cancel();
    });
    worker.run(shutdown).await?;
    Ok(())
}

async fn enqueue_and_wait(store: Store) -> Result<(), Box<dyn Error>> {
    let queue_name = queue_name()?;
    let task_name = TaskName::new(TASK_NAME)?;
    let count = environment_u16("PGTASK_SMOKE_TASKS", 100)?;
    let mut task_ids = Vec::with_capacity(usize::from(count));
    for sequence in 0..count {
        let mut request = EnqueueRequest::new(task_name.clone(), json!({"sequence": sequence}));
        request.queue_name = queue_name.clone();
        task_ids.push(store.enqueue(&request).await?.task_id);
    }
    tokio::time::timeout(Duration::from_mins(1), async {
        loop {
            let mut succeeded = 0_u16;
            for task_id in &task_ids {
                if store
                    .task_result(*task_id)
                    .await?
                    .is_some_and(|result| result.state == TaskState::Succeeded)
                {
                    succeeded += 1;
                }
            }
            if succeeded == count {
                return Ok::<(), pgtask::postgres::PostgresError>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    println!("drained {count} tasks");
    Ok(())
}

fn queue_name() -> Result<QueueName, Box<dyn Error>> {
    Ok(QueueName::new(
        std::env::var("PGTASK_QUEUE").unwrap_or_else(|_| "default".to_owned()),
    )?)
}

fn environment_u16(name: &str, default: u16) -> Result<u16, Box<dyn Error>> {
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn environment_nonzero_u32(name: &str, default: u32) -> Result<NonZeroU32, Box<dyn Error>> {
    let value = match std::env::var(name) {
        Ok(value) => value.parse()?,
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    NonZeroU32::new(value).ok_or_else(|| format!("{name} must be greater than zero").into())
}

fn environment_bool(name: &str, default: bool) -> Result<bool, Box<dyn Error>> {
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
async fn shutdown_signal() {
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler installs");
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.expect("SIGINT handler runs"),
        signal = terminate.recv() => assert!(signal.is_some()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.expect("shutdown handler runs");
}
