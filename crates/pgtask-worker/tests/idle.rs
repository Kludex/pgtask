//! An idle worker sleeps between polls instead of asking the database for its next deadline in a
//! loop.

use std::{str::FromStr, time::Duration};

use pgtask_core::{HandlerVersion, QueueName, RetryPolicy, TaskName};
use pgtask_postgres::Store;
use pgtask_worker::{HandlerRegistry, Worker, WorkerConfig};
use serde_json::json;
use sqlx::{PgPool, postgres::PgConnectOptions};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DELAY_FUNCTIONS: [(&str, &str); 3] = [
    ("next_schedule_delay_milliseconds", ""),
    ("next_wait_delay_milliseconds", ""),
    (
        "next_task_delay_milliseconds",
        "p_queue_name text, p_task_names text[], p_handler_versions integer[]",
    ),
];

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

/// Migrates a fresh database and wraps each delay function so every call is counted.
async fn observed_delay_store(database_url: &str) -> (Store, PgPool, String) {
    let database_name = format!("pgtask_idle_{}", Uuid::new_v4().simple());
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
    sqlx::query("CREATE TABLE public.delay_calls (function_name text PRIMARY KEY, calls bigint NOT NULL DEFAULT 0)")
        .execute(store.pool())
        .await
        .unwrap();
    for (function, parameters) in DELAY_FUNCTIONS {
        let arguments = parameters
            .split(", ")
            .filter(|parameter| !parameter.is_empty())
            .map(|parameter| parameter.split_once(' ').unwrap().0)
            .collect::<Vec<_>>()
            .join(", ");
        for statement in [
            format!("INSERT INTO public.delay_calls (function_name) VALUES ('{function}')"),
            format!("ALTER FUNCTION pgtask.{function}({parameters}) RENAME TO {function}_unobserved"),
            format!(
                "CREATE FUNCTION pgtask.{function}({parameters}) RETURNS bigint LANGUAGE plpgsql AS $$ \
                 BEGIN \
                     UPDATE public.delay_calls SET calls = calls + 1 WHERE function_name = '{function}'; \
                     RETURN pgtask.{function}_unobserved({arguments}); \
                 END $$"
            ),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(store.pool())
                .await
                .unwrap();
        }
    }
    (store, maintenance, database_name)
}

async fn delay_calls(store: &Store) -> Vec<(String, i64)> {
    sqlx::query_as("SELECT function_name, calls FROM public.delay_calls ORDER BY function_name")
        .fetch_all(store.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn idle_worker_does_not_poll_for_deadlines_in_a_loop() {
    let Some(database_url) = database_url() else {
        return;
    };
    let (store, maintenance, database_name) = observed_delay_store(&database_url).await;

    let queue_name = QueueName::new("idle").unwrap();
    let task_name = TaskName::new("idle-task").unwrap();
    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { Ok(json!(null)) },
    );
    let mut config = WorkerConfig::new(queue_name);
    config.poll_interval = Duration::from_secs(30);
    config.schedule_reconciliation_interval = Duration::from_secs(30);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    tokio::time::sleep(Duration::from_secs(1)).await;
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();

    // Nothing is scheduled, waiting, or pending, so each loop asks once at startup and then sleeps for
    // its full interval. A few extra calls cover wakeups from the listener connecting.
    let calls = delay_calls(&store).await;
    assert_eq!(calls.len(), DELAY_FUNCTIONS.len());
    for (function, count) in &calls {
        assert!(
            (1..=5).contains(count),
            "{function} was called {count} times in one idle second: {calls:?}"
        );
    }

    drop(store);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE {database_name} WITH (FORCE)"
    )))
    .execute(&maintenance)
    .await
    .unwrap();
}
