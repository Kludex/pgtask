//! Built-in retention keeps up with more than one batch per tick and removes dead worker rows.

use std::{num::NonZeroU16, time::Duration};

use pgtask_core::{EnqueueRequest, HandlerVersion, QueueConfig, QueueName, RetryPolicy, TaskName, WorkerId};
use pgtask_postgres::Store;
use pgtask_worker::{HandlerRegistry, Worker, WorkerConfig};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

fn registry(task_name: &TaskName) -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { Ok(json!(null)) },
    );
    registry
}

/// Retention runs once when the worker starts and then not again for an hour, so whatever is gone
/// by the deadline was deleted by that first tick.
fn one_tick_config(queue_name: QueueName) -> WorkerConfig {
    let mut config = WorkerConfig::new(queue_name);
    config.retention_batch_size = NonZeroU16::new(100).unwrap();
    config.retention_interval = Duration::from_hours(1);
    config
}

async fn count(store: &Store, query: &'static str, queue_name: &QueueName) -> i64 {
    sqlx::query_scalar(query)
        .bind(queue_name.as_str())
        .fetch_one(store.pool())
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn one_retention_tick_drains_more_than_one_batch() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("retention-drain-{suffix}")).unwrap();
    let task_name = TaskName::new("retention-drain").unwrap();
    let mut queue = QueueConfig::new(queue_name.clone());
    queue.terminal_retention = Duration::ZERO;
    queue.idempotency_retention = Duration::ZERO;
    store.put_queue(&queue).await.unwrap();
    let requests: Vec<_> = (0..250)
        .map(|index| {
            let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
            request.queue_name = queue_name.clone();
            request.idempotency_key = Some(format!("drain-{index}"));
            request
        })
        .collect();
    for enqueued in store.enqueue_many(&requests).await.unwrap() {
        assert!(store.cancel(enqueued.task_id).await.unwrap());
    }

    let worker = Worker::new(store.clone(), registry(&task_name), one_tick_config(queue_name.clone())).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    let drained = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let tasks = count(
                &store,
                "SELECT count(*) FROM pgtask.tasks WHERE queue_name = $1",
                &queue_name,
            )
            .await;
            let keys = count(
                &store,
                "SELECT count(*) FROM pgtask.idempotency_keys WHERE queue_name = $1",
                &queue_name,
            )
            .await;
            if tasks == 0 && keys == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let remaining = count(
        &store,
        "SELECT count(*) FROM pgtask.tasks WHERE queue_name = $1",
        &queue_name,
    )
    .await;
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
    assert!(
        drained.is_ok(),
        "{remaining} of 250 expired tasks remain after one retention tick"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn retention_deletes_worker_rows_that_expired_before_the_grace_period() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let queue_name = QueueName::new(format!("retention-workers-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("retention-workers").unwrap();
    let capabilities = [(task_name.clone(), HandlerVersion::default(), RetryPolicy::Never)];
    let dead = WorkerId::new();
    store
        .register_worker(dead, &queue_name, "dead", &capabilities, Duration::from_millis(1))
        .await
        .unwrap();
    let alive = WorkerId::new();
    store
        .register_worker(alive, &queue_name, "alive", &capabilities, Duration::from_hours(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut config = one_tick_config(queue_name.clone());
    // Other tests read their own rows right after expiring them; stay well clear of that.
    config.expired_worker_retention = Duration::from_millis(200);
    let worker = Worker::new(store.clone(), registry(&task_name), config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    let deleted = tokio::time::timeout(TEST_TIMEOUT, async {
        while store.get_worker(dead).await.unwrap().is_some() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
    assert!(deleted.is_ok(), "the expired worker row was not deleted");
    assert!(store.get_worker(alive).await.unwrap().is_some());
    let dead_capabilities: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pgtask.worker_capabilities WHERE worker_id = $1")
            .bind(dead.as_uuid())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(dead_capabilities, 0);
}
