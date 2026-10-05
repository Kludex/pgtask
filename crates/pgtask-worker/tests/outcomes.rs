//! A handler outcome PostgreSQL cannot store fails only its own task, with an error that says why.

use std::{
    collections::HashMap,
    num::NonZeroU16,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use pgtask_core::{EnqueueRequest, HandlerVersion, QueueName, RetryPolicy, Task, TaskId, TaskName, TaskState};
use pgtask_postgres::Store;
use pgtask_worker::{HandlerError, HandlerRegistry, Worker, WorkerConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgConnectOptions};
use tokio::sync::Barrier;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

type Outcome = fn() -> Result<Value, HandlerError>;

/// Handlers whose first runs finish together, so their transitions share one batch.
struct Scenario {
    queue_name: QueueName,
    registry: HandlerRegistry,
    runs: HashMap<&'static str, Arc<AtomicUsize>>,
}

impl Scenario {
    fn new(outcomes: &[(&'static str, Outcome)]) -> Self {
        let queue_name = QueueName::new(format!("outcomes-{}", Uuid::new_v4().simple())).unwrap();
        let barrier = Arc::new(Barrier::new(outcomes.len()));
        let mut registry = HandlerRegistry::new();
        let mut runs = HashMap::new();
        for &(name, outcome) in outcomes {
            let counter = Arc::new(AtomicUsize::new(0));
            runs.insert(name, Arc::clone(&counter));
            let barrier = Arc::clone(&barrier);
            registry.register(
                TaskName::new(name).unwrap(),
                HandlerVersion::default(),
                RetryPolicy::Fixed {
                    delay: Duration::from_millis(10),
                },
                move |_| {
                    let barrier = Arc::clone(&barrier);
                    let first_run = counter.fetch_add(1, Ordering::SeqCst) == 0;
                    async move {
                        if first_run {
                            barrier.wait().await;
                        }
                        outcome()
                    }
                },
            );
        }
        Self {
            queue_name,
            registry,
            runs,
        }
    }

    /// Enqueues one task per handler, runs a worker until every task is terminal, and returns them.
    async fn run(self, store: &Store) -> HashMap<&'static str, (Task, usize)> {
        let mut task_ids = HashMap::new();
        for &name in self.runs.keys() {
            let mut request = EnqueueRequest::new(TaskName::new(name).unwrap(), json!({}));
            request.queue_name = self.queue_name.clone();
            request.max_attempts = 2;
            task_ids.insert(name, store.enqueue(&request).await.unwrap().task_id);
        }
        let mut config = WorkerConfig::new(self.queue_name.clone());
        let concurrency = NonZeroU16::new(u16::try_from(self.runs.len()).unwrap()).unwrap();
        config.concurrency = concurrency;
        config.claim_batch_size = concurrency;
        config.lease_duration = Duration::from_secs(2);
        config.poll_interval = Duration::from_millis(20);
        config.retention_enabled = false;
        let worker = Worker::new(store.clone(), self.registry, config).unwrap();
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
        let ids: Vec<TaskId> = task_ids.values().copied().collect();
        tokio::time::timeout(TEST_TIMEOUT, async {
            while !all_terminal(store, &ids).await {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("every task reaches a terminal state");
        shutdown.cancel();
        worker_task.await.unwrap().unwrap();
        let mut finished = HashMap::new();
        for (name, task_id) in task_ids {
            let task = store.get_task(task_id).await.unwrap().unwrap();
            finished.insert(name, (task, self.runs[name].load(Ordering::SeqCst)));
        }
        finished
    }
}

async fn all_terminal(store: &Store, task_ids: &[TaskId]) -> bool {
    for task_id in task_ids {
        let state = store.get_task(*task_id).await.unwrap().unwrap().state;
        if !matches!(state, TaskState::Succeeded | TaskState::Failed) {
            return false;
        }
    }
    true
}

/// A database of its own, so the extra constraint below cannot reach other tests.
async fn isolated_store(database_url: &str) -> (Store, PgPool, String) {
    let database_name = format!("pgtask_outcomes_{}", Uuid::new_v4().simple());
    let options = PgConnectOptions::from_str(database_url).unwrap();
    let maintenance = PgPool::connect_with(options.clone().database("postgres"))
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database_name}")))
        .execute(&maintenance)
        .await
        .unwrap();
    let store = Store::from_pool(PgPool::connect_with(options.database(&database_name)).await.unwrap());
    store.migrate().await.unwrap();
    (store, maintenance, database_name)
}

async fn drop_isolated_store(store: Store, maintenance: &PgPool, database_name: &str) {
    store.pool().close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE {database_name} WITH (FORCE)"
    )))
    .execute(maintenance)
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_outcomes_fail_only_their_own_task() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let finished = Scenario::new(&[
        ("big-result", || Ok(json!("x".repeat(1_100_000)))),
        ("small-result", || Ok(json!("ok"))),
        ("big-error", || Err(HandlerError::retryable("e".repeat(300_000)))),
        ("small-error", || Err(HandlerError::terminal("small failed normally"))),
    ])
    .run(&store)
    .await;

    let (small_result, runs) = &finished["small-result"];
    assert_eq!((small_result.state, *runs), (TaskState::Succeeded, 1));
    assert_eq!(small_result.failed_attempts, 0);
    assert_eq!(small_result.result, Some(json!("ok")));

    let (small_error, runs) = &finished["small-error"];
    assert_eq!((small_error.state, *runs), (TaskState::Failed, 1));
    assert_eq!(small_error.failed_attempts, 1);
    assert_eq!(
        small_error.error,
        Some(json!({"type": "handler_error", "message": "small failed normally", "retryable": false}))
    );

    let (big_result, runs) = &finished["big-result"];
    assert_eq!((big_result.state, *runs), (TaskState::Failed, 1));
    assert_eq!(
        big_result.error,
        Some(json!({"type": "result_too_large", "bytes": 1_100_002, "limit": 1_048_576}))
    );
    assert_eq!(big_result.result, None);

    let (big_error, runs) = &finished["big-error"];
    assert_eq!((big_error.state, *runs), (TaskState::Failed, 2));
    assert_eq!(big_error.failed_attempts, 2);
    let error = big_error.error.as_ref().unwrap();
    assert_eq!(error["type"], "handler_error");
    assert_eq!(error["truncated"], true);
    assert_eq!(error["retryable"], true);
    assert!(error["message"].as_str().unwrap().starts_with("eeee"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_batch_falls_back_to_writing_each_transition_alone() {
    let Some(database_url) = database_url() else {
        return;
    };
    let (store, maintenance, database_name) = isolated_store(&database_url).await;
    // A rule the worker cannot know about stands in for any value PostgreSQL rejects at write time.
    sqlx::query(
        r#"
        ALTER TABLE pgtask.tasks ADD CONSTRAINT tasks_poison_check CHECK (
            result IS DISTINCT FROM '"poison"'::jsonb
            AND (error IS NULL OR error->>'message' IS DISTINCT FROM 'poison')
        )
        "#,
    )
    .execute(store.pool())
    .await
    .unwrap();

    let finished = Scenario::new(&[
        ("poison-result", || Ok(json!("poison"))),
        ("small-result", || Ok(json!("ok"))),
        ("poison-error", || Err(HandlerError::retryable("poison"))),
        ("small-error", || Err(HandlerError::terminal("small failed normally"))),
    ])
    .run(&store)
    .await;

    let (small_result, runs) = &finished["small-result"];
    assert_eq!((small_result.state, *runs), (TaskState::Succeeded, 1));
    assert_eq!(small_result.failed_attempts, 0);

    let (small_error, runs) = &finished["small-error"];
    assert_eq!((small_error.state, *runs), (TaskState::Failed, 1));
    assert_eq!(small_error.failed_attempts, 1);
    assert_eq!(
        small_error.error,
        Some(json!({"type": "handler_error", "message": "small failed normally", "retryable": false}))
    );

    let (poison_result, runs) = &finished["poison-result"];
    assert_eq!((poison_result.state, *runs), (TaskState::Failed, 1));
    let error = poison_result.error.as_ref().unwrap();
    assert_eq!(error["type"], "result_rejected");
    assert!(error["message"].as_str().unwrap().contains("tasks_poison_check"));

    // The rejected error is replaced, so the task still follows its retry policy.
    let (poison_error, runs) = &finished["poison-error"];
    assert_eq!((poison_error.state, *runs), (TaskState::Failed, 2));
    assert_eq!(poison_error.failed_attempts, 2);
    assert_eq!(poison_error.error.as_ref().unwrap()["type"], "error_rejected");

    drop_isolated_store(store, &maintenance, &database_name).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lone_unstorable_outcome_records_why_instead_of_expiring() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    for (name, outcome) in [
        (
            "big-error",
            (|| Err(HandlerError::terminal("e".repeat(300_000)))) as Outcome,
        ),
        ("nul-result", || Ok(json!({"text": "a\u{0}b"}))),
        ("nul-error", || Err(HandlerError::terminal("a\u{0}b"))),
    ] {
        let finished = Scenario::new(&[(name, outcome)]).run(&store).await;
        let (task, runs) = &finished[name];
        assert_eq!(*runs, 1, "{name} ran once");
        assert_eq!(
            task.failed_attempts,
            u16::from(task.state == TaskState::Failed),
            "{name}"
        );
        match name {
            "big-error" => {
                let error = task.error.as_ref().unwrap();
                assert_eq!(
                    (task.state, &error["type"]),
                    (TaskState::Failed, &json!("handler_error"))
                );
                assert_eq!(error["truncated"], true);
                assert_eq!(error["original_bytes"], 300_060);
            }
            "nul-result" => {
                assert_eq!(task.state, TaskState::Succeeded);
                assert_eq!(task.result, Some(json!({"text": "a\\u0000b"})));
            }
            _ => {
                assert_eq!(task.state, TaskState::Failed);
                assert_eq!(
                    task.error,
                    Some(json!({"type": "handler_error", "message": "a\\u0000b", "retryable": false}))
                );
            }
        }
    }
}
