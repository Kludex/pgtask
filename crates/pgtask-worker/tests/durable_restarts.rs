use std::{
    num::NonZeroU16,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::Utc;
use pgtask_core::{
    EnqueueRequest, HandlerVersion, QueueName, RetryPolicy, SignalName, StepName, TaskId, TaskName, TaskState,
};
use pgtask_postgres::Store;
use pgtask_worker::{HandlerRegistry, Worker, WorkerConfig, WorkerError};
use serde_json::json;
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const TEST_TIMEOUT: Duration = Duration::from_mins(1);

struct Controls {
    pause_after_checkpoint: AtomicBool,
    checkpoint_ready: Notify,
    checkpoint_operations: AtomicUsize,
    child_ready: Notify,
    child_runs: AtomicUsize,
}

impl Controls {
    fn new() -> Self {
        Self {
            pause_after_checkpoint: AtomicBool::new(true),
            checkpoint_ready: Notify::new(),
            checkpoint_operations: AtomicUsize::new(0),
            child_ready: Notify::new(),
            child_runs: AtomicUsize::new(0),
        }
    }
}

fn registry(
    parent_name: &TaskName,
    child_name: &TaskName,
    queue_name: &QueueName,
    controls: &Arc<Controls>,
) -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    let parent_controls = Arc::clone(controls);
    let spawned_child_name = child_name.clone();
    let spawned_queue_name = queue_name.clone();
    registry.register_durable(
        parent_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |_task, context| {
            let controls = Arc::clone(&parent_controls);
            let child_name = spawned_child_name.clone();
            let queue_name = spawned_queue_name.clone();
            async move {
                let checkpoint = context
                    .step(&StepName::new("checkpoint").unwrap(), 0, || async {
                        controls.checkpoint_operations.fetch_add(1, Ordering::SeqCst);
                        Ok(json!({"checkpoint": true}))
                    })
                    .await?;
                if controls.pause_after_checkpoint.swap(false, Ordering::SeqCst) {
                    controls.checkpoint_ready.notify_one();
                    std::future::pending::<()>().await;
                }
                context
                    .sleep_for(&StepName::new("sleep").unwrap(), 0, Duration::from_secs(3))
                    .await?;
                let signal = context
                    .wait_for_signal(
                        &StepName::new("signal").unwrap(),
                        0,
                        &SignalName::new("approval").unwrap(),
                        0,
                        None,
                    )
                    .await?;
                let mut child_request = EnqueueRequest::new(child_name, json!({}));
                child_request.queue_name = queue_name;
                let child_id = context
                    .spawn(&StepName::new("child").unwrap(), 0, &child_request)
                    .await?;
                let child = context
                    .wait_for_result(&StepName::new("child-result").unwrap(), 0, child_id, None)
                    .await?;
                Ok(json!({"checkpoint": checkpoint, "signal": signal, "child": child}))
            }
        },
    );
    let child_controls = Arc::clone(controls);
    registry.register(
        child_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |_| {
            let controls = Arc::clone(&child_controls);
            async move {
                if controls.child_runs.fetch_add(1, Ordering::SeqCst) == 0 {
                    controls.child_ready.notify_one();
                    std::future::pending::<()>().await;
                }
                Ok(json!({"finished": true}))
            }
        },
    );
    registry
}

fn start_worker(
    store: &Store,
    parent_name: &TaskName,
    child_name: &TaskName,
    queue_name: &QueueName,
    controls: &Arc<Controls>,
    shutdown: CancellationToken,
) -> JoinHandle<Result<(), WorkerError>> {
    let mut config = WorkerConfig::new(queue_name.clone());
    config.concurrency = NonZeroU16::new(2).unwrap();
    config.claim_batch_size = NonZeroU16::new(2).unwrap();
    config.lease_duration = Duration::from_secs(1);
    config.poll_interval = Duration::from_millis(5);
    config.schedule_reconciliation_interval = Duration::from_millis(5);
    let worker = Worker::new(
        store.clone(),
        registry(parent_name, child_name, queue_name, controls),
        config,
    )
    .unwrap();
    tokio::spawn(async move { worker.run(shutdown).await })
}

async fn wait_for_task(store: &Store, task_id: TaskId, predicate: impl Fn(&pgtask_core::Task) -> bool) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let task = store.get_task(task_id).await.unwrap().unwrap();
            if predicate(&task) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

async fn abort_worker(worker: JoinHandle<Result<(), WorkerError>>) {
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn workflow_resumes_after_worker_death_at_every_durable_primitive() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("durable-restarts-{suffix}")).unwrap();
    let parent_name = TaskName::new(format!("durable-restarts-parent-{suffix}")).unwrap();
    let child_name = TaskName::new(format!("durable-restarts-child-{suffix}")).unwrap();
    let controls = Arc::new(Controls::new());
    let mut request = EnqueueRequest::new(parent_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    let parent_id = store.enqueue(&request).await.unwrap().task_id;

    let first = start_worker(
        &store,
        &parent_name,
        &child_name,
        &queue_name,
        &controls,
        CancellationToken::new(),
    );
    tokio::time::timeout(TEST_TIMEOUT, controls.checkpoint_ready.notified())
        .await
        .unwrap();
    abort_worker(first).await;

    let second = start_worker(
        &store,
        &parent_name,
        &child_name,
        &queue_name,
        &controls,
        CancellationToken::new(),
    );
    wait_for_task(&store, parent_id, |task| {
        task.state == TaskState::Pending && task.attempt == 2 && task.run_at > Utc::now()
    })
    .await;
    abort_worker(second).await;

    let third = start_worker(
        &store,
        &parent_name,
        &child_name,
        &queue_name,
        &controls,
        CancellationToken::new(),
    );
    wait_for_task(&store, parent_id, |task| task.state == TaskState::Waiting).await;
    abort_worker(third).await;
    store
        .emit_signal(
            parent_id,
            &SignalName::new("approval").unwrap(),
            0,
            &json!({"approved": true}),
        )
        .await
        .unwrap();

    let fourth = start_worker(
        &store,
        &parent_name,
        &child_name,
        &queue_name,
        &controls,
        CancellationToken::new(),
    );
    tokio::time::timeout(TEST_TIMEOUT, controls.child_ready.notified())
        .await
        .unwrap();
    wait_for_task(&store, parent_id, |task| task.state == TaskState::Waiting).await;
    abort_worker(fourth).await;

    let shutdown = CancellationToken::new();
    let fifth = start_worker(
        &store,
        &parent_name,
        &child_name,
        &queue_name,
        &controls,
        shutdown.clone(),
    );
    wait_for_task(&store, parent_id, |task| task.state == TaskState::Succeeded).await;
    let parent = store.get_task(parent_id).await.unwrap().unwrap();
    assert_eq!(
        parent.result,
        Some(json!({
            "checkpoint": {"checkpoint": true},
            "signal": {"approved": true},
            "child": {"state": "succeeded", "result": {"finished": true}, "error": null}
        }))
    );
    assert_eq!(controls.checkpoint_operations.load(Ordering::SeqCst), 1);
    assert_eq!(controls.child_runs.load(Ordering::SeqCst), 2);
    shutdown.cancel();
    fifth.await.unwrap().unwrap();
}
