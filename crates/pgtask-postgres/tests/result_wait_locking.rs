use std::time::Duration;

use pgtask_core::{EnqueueRequest, HandlerVersion, QueueName, StepName, Task, TaskName, TaskState, WorkerId};
use pgtask_postgres::{SpawnRequest, Store};
use serde_json::{Value, json};
use sqlx::PgConnection;
use uuid::Uuid;

async fn connect() -> Option<Store> {
    let database_url = std::env::var("PGTASK_DATABASE_URL").ok()?;
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    Some(store)
}

async fn family(store: &Store, count: usize) -> Vec<Task> {
    let queue = QueueName::new(format!("wait-locks-{}", Uuid::new_v4())).unwrap();
    let name = TaskName::new("wait-locks").unwrap();
    let mut request = EnqueueRequest::new(name.clone(), json!({}));
    request.queue_name = queue.clone();
    let mut tasks: Vec<Task> = Vec::new();
    for _ in 0..count {
        if let Some(parent) = tasks.last() {
            store
                .spawn_task(SpawnRequest {
                    parent_task_id: parent.id,
                    parent_attempt: parent.attempt,
                    parent_lease_token: parent.lease_token.unwrap(),
                    step_name: &StepName::new("spawn").unwrap(),
                    occurrence: 0,
                    task: &request,
                })
                .await
                .unwrap()
                .unwrap();
        } else {
            store.enqueue(&request).await.unwrap();
        }
        tasks.push(
            store
                .claim(
                    &queue,
                    WorkerId::new(),
                    &[(name.clone(), HandlerVersion::default())],
                    1,
                    Duration::from_mins(1),
                )
                .await
                .unwrap()
                .pop()
                .unwrap(),
        );
    }
    tasks
}

async fn backend_pid(connection: &mut PgConnection) -> i32 {
    sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(connection)
        .await
        .unwrap()
}

async fn await_blocked(store: &Store, waiter: i32, holder: i32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT $1 = ANY(pg_blocking_pids($2))")
                .bind(holder)
                .bind(waiter)
                .fetch_one(store.pool())
                .await
                .unwrap();
            if blocked {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the operation must block on the controlled transaction");
}

async fn wait_for_child(connection: &mut PgConnection, parent: &Task, child: &Task) -> (String, Option<Value>) {
    sqlx::query_as("SELECT * FROM pgtask.wait_for_result($1, $2, $3, 'wait', 0, $4, NULL)")
        .bind(parent.id.as_uuid())
        .bind(i32::from(parent.attempt))
        .bind(parent.lease_token.unwrap().as_uuid())
        .bind(child.id.as_uuid())
        .fetch_one(connection)
        .await
        .unwrap()
}

async fn complete_child(connection: &mut PgConnection, child: &Task) {
    let completed: bool = sqlx::query_scalar("SELECT pgtask.complete_task($1, $2, $3, $4)")
        .bind(child.id.as_uuid())
        .bind(i32::from(child.attempt))
        .bind(child.lease_token.unwrap().as_uuid())
        .bind(json!({"answer": 42}))
        .fetch_one(connection)
        .await
        .unwrap();
    assert!(completed);
}

#[tokio::test]
async fn child_completion_waits_for_registration_to_commit() {
    let Some(store) = connect().await else { return };
    let tasks = family(&store, 2).await;
    let mut registration = store.pool().begin().await.unwrap();
    let holder = backend_pid(&mut registration).await;
    assert_eq!(
        wait_for_child(&mut registration, &tasks[0], &tasks[1]).await,
        ("waiting".into(), None)
    );

    let mut completion = store.pool().acquire().await.unwrap();
    let waiter = backend_pid(&mut completion).await;
    let child = tasks[1].clone();
    let completing = tokio::spawn(async move { complete_child(&mut completion, &child).await });
    await_blocked(&store, waiter, holder).await;
    registration.commit().await.unwrap();
    completing.await.unwrap();

    assert_eq!(
        store.get_task(tasks[0].id).await.unwrap().unwrap().state,
        TaskState::Pending
    );
    let checkpoint = store
        .get_checkpoint(
            tasks[0].id,
            HandlerVersion::default(),
            &StepName::new("wait").unwrap(),
            0,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        checkpoint.value,
        json!({"state": "succeeded", "result": {"answer": 42}, "error": null})
    );
}

#[tokio::test]
async fn registration_reads_a_child_completion_that_commits_while_blocked() {
    let Some(store) = connect().await else { return };
    let tasks = family(&store, 2).await;
    let mut completion = store.pool().begin().await.unwrap();
    let holder = backend_pid(&mut completion).await;
    complete_child(&mut completion, &tasks[1]).await;

    let mut registration = store.pool().acquire().await.unwrap();
    let waiter = backend_pid(&mut registration).await;
    let waiting = tokio::spawn(async move { wait_for_child(&mut registration, &tasks[0], &tasks[1]).await });
    await_blocked(&store, waiter, holder).await;
    completion.commit().await.unwrap();
    assert_eq!(
        waiting.await.unwrap(),
        (
            "ready".into(),
            Some(json!({"state": "succeeded", "result": {"answer": 42}, "error": null}))
        )
    );
}

#[tokio::test]
async fn renewal_locks_ancestors_before_descendants() {
    let Some(store) = connect().await else { return };
    for include_middle in [true, false] {
        let tasks = family(&store, 3).await;
        let mut registration = store.pool().begin().await.unwrap();
        let holder = backend_pid(&mut registration).await;
        sqlx::query("SELECT id FROM pgtask.tasks WHERE id = $1 FOR UPDATE")
            .bind(tasks[0].id.as_uuid())
            .fetch_one(&mut *registration)
            .await
            .unwrap();

        let mut renewal = store.pool().acquire().await.unwrap();
        let waiter = backend_pid(&mut renewal).await;
        let leases: Vec<_> = tasks
            .iter()
            .enumerate()
            .rev()
            .filter(|(index, _)| include_middle || *index != 1)
            .map(|(_, task)| task.clone())
            .collect();
        let renewing = tokio::spawn(async move {
            sqlx::query_scalar::<_, Uuid>("SELECT * FROM pgtask.renew_leases($1, $2, $3, 60000)")
                .bind(leases.iter().map(|task| task.id.as_uuid()).collect::<Vec<_>>())
                .bind(leases.iter().map(|task| i32::from(task.attempt)).collect::<Vec<_>>())
                .bind(
                    leases
                        .iter()
                        .map(|task| task.lease_token.unwrap().as_uuid())
                        .collect::<Vec<_>>(),
                )
                .fetch_all(&mut *renewal)
                .await
                .unwrap()
        });
        await_blocked(&store, waiter, holder).await;
        sqlx::query("SELECT id FROM pgtask.tasks WHERE id = ANY($1) FOR NO KEY UPDATE NOWAIT")
            .bind(vec![tasks[1].id.as_uuid(), tasks[2].id.as_uuid()])
            .fetch_all(&mut *registration)
            .await
            .expect("renewal must not hold descendant locks while waiting for an ancestor");
        assert_eq!(
            wait_for_child(&mut registration, &tasks[0], &tasks[1]).await,
            ("waiting".into(), None)
        );
        registration.commit().await.unwrap();
        let renewed = renewing.await.unwrap();
        assert_eq!(renewed.len(), if include_middle { 2 } else { 1 });
        assert!(renewed.contains(&tasks[2].id.as_uuid()));
        assert!(!renewed.contains(&tasks[0].id.as_uuid()));
    }
}
