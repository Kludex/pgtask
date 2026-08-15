use std::{str::FromStr, time::Duration};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
};
use chrono::{TimeDelta, Utc};
use pgtask::{
    core::{
        EnqueueRequest, HandlerVersion, QueueName, ScheduleConfig, ScheduleDefinition, ScheduleName, SignalName,
        StepName, TaskName, WorkerId,
    },
    postgres::Store,
};
use pgtask_web::{AdministratorConfig, application, application_with_administrator};
use serde_json::json;
use sqlx::{PgPool, postgres::PgConnectOptions};
use tower::ServiceExt;
use uuid::Uuid;

async fn response(app: &Router, path: &str) -> (StatusCode, String) {
    request(app, Method::GET, path, None).await
}

async fn request(app: &Router, method: Method, path: &str, actor: Option<&str>) -> (StatusCode, String) {
    let mut request = Request::builder().method(method).uri(path);
    if let Some(actor) = actor {
        request = request.header("x-pgtask-actor", actor);
    }
    let response = app.clone().oneshot(request.body(Body::empty()).unwrap()).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1_000_000).await.unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

#[tokio::test]
async fn administrator_mode_mutates_through_audited_post_operations() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let (_observer, store, _task_id, schedule_id, _worker_id) = seeded_application(&database_url).await;
    let suffix = Uuid::new_v4();
    let mut task = EnqueueRequest::new(TaskName::new(format!("admin-task-{suffix}")).unwrap(), json!({}));
    task.queue_name = QueueName::new(format!("admin-{suffix}")).unwrap();
    let task_id = store.enqueue(&task).await.unwrap().task_id.as_uuid();
    let app = application_with_administrator(store.pool().clone(), AdministratorConfig::default());

    assert_eq!(
        request(&app, Method::POST, &format!("/admin/tasks/{task_id}/cancel"), None,)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    for (path, actor) in [
        (format!("/admin/tasks/{task_id}/cancel"), "alice@example.com"),
        (format!("/admin/tasks/{task_id}/retry"), "bob@example.com"),
        (format!("/admin/schedules/{schedule_id}/pause"), "alice@example.com"),
    ] {
        assert_eq!(
            request(&app, Method::POST, &path, Some(actor)).await.0,
            StatusCode::SEE_OTHER
        );
    }
    let (_, paused_schedule_page) = response(&app, "/schedules").await;
    assert!(paused_schedule_page.contains("paused"));
    assert_eq!(
        request(
            &app,
            Method::POST,
            &format!("/admin/schedules/{schedule_id}/resume"),
            Some("bob@example.com"),
        )
        .await
        .0,
        StatusCode::SEE_OTHER
    );
    for path in [
        "/admin/tasks/00000000-0000-0000-0000-000000000000/cancel",
        "/admin/tasks/00000000-0000-0000-0000-000000000000/retry",
        "/admin/schedules/00000000-0000-0000-0000-000000000000/pause",
    ] {
        assert_eq!(
            request(&app, Method::POST, path, Some("admin@example.com")).await.0,
            StatusCode::NOT_FOUND
        );
    }
    let (_, task_page) = response(&app, &format!("/tasks/{task_id}")).await;
    assert!(task_page.contains("task.cancel"));
    assert!(task_page.contains("task.retry"));
    assert!(task_page.contains("alice@example.com"));
    let (_, schedule_page) = response(&app, &format!("/schedules/{schedule_id}")).await;
    assert!(schedule_page.contains("schedule.pause"));
    assert!(schedule_page.contains("schedule.resume"));
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pgtask.administrator_audit_view \
         WHERE task_id = $1 OR schedule_id = $2",
    )
    .bind(task_id)
    .bind(schedule_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(audit_count, 4);
}

async fn seeded_application(database_url: &str) -> (Router, Store, Uuid, Uuid, Uuid) {
    let store = Store::connect(database_url).await.unwrap();
    store.migrate().await.unwrap();
    let suffix = Uuid::new_v4();
    let queue_name = QueueName::new(format!("web-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("web-task-{suffix}")).unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({"unsafe": "<script>"}));
    request.queue_name = queue_name.clone();
    let task_id = store.enqueue(&request).await.unwrap().task_id;
    let task = store
        .claim(
            &queue_name,
            WorkerId::new(),
            &[(task_name.clone(), HandlerVersion::default())],
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    let lease_token = task.lease_token.unwrap();
    store
        .commit_checkpoint(
            task_id,
            task.attempt,
            lease_token,
            &StepName::new("render").unwrap(),
            0,
            &json!({"rendered": true}),
        )
        .await
        .unwrap();
    store
        .complete(task_id, task.attempt, lease_token, Some(&json!({"ok": true})))
        .await
        .unwrap();
    store
        .emit_signal(task_id, &SignalName::new("audit").unwrap(), 0, &json!({"seen": true}))
        .await
        .unwrap();

    let mut scheduled_request = EnqueueRequest::new(task_name.clone(), json!({}));
    scheduled_request.queue_name = queue_name.clone();
    let mut schedule = ScheduleConfig::new(
        ScheduleName::new(format!("web-schedule-{suffix}")).unwrap(),
        ScheduleDefinition::interval(Duration::from_mins(1)).unwrap(),
        scheduled_request,
    );
    schedule.start_at = Some(Utc::now() - TimeDelta::minutes(1));
    let schedule_id = store.put_schedule(&schedule).await.unwrap().config.id;
    assert_eq!(store.materialize_due_schedules(1).await.unwrap(), 1);

    let worker_id = WorkerId::new();
    store
        .register_worker(
            worker_id,
            &queue_name,
            "test-version",
            &[(task_name, HandlerVersion::default())],
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    (
        application(store.pool().clone()),
        store,
        task_id.as_uuid(),
        schedule_id.as_uuid(),
        worker_id.as_uuid(),
    )
}

#[tokio::test]
async fn database_failures_return_internal_server_errors() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let (app, store, task_id, _schedule_id, _worker_id) = seeded_application(&database_url).await;
    store.pool().close().await;
    for path in ["/".to_owned(), "/healthz".to_owned(), format!("/tasks/{task_id}")] {
        assert_eq!(response(&app, &path).await.0, StatusCode::INTERNAL_SERVER_ERROR);
    }
}

#[tokio::test]
async fn observer_pages_cover_queues_tasks_schedules_workers_and_not_found() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let (app, _store, task_id, schedule_id, worker_id) = seeded_application(&database_url).await;
    for path in ["/", "/healthz", "/tasks", "/schedules", "/workers"] {
        assert_eq!(response(&app, path).await.0, StatusCode::OK);
    }
    let (_, tasks) = response(&app, "/tasks?query=%3Cscript%3E").await;
    assert!(tasks.contains("&lt;script&gt;"));
    assert!(!tasks.contains("value=\"<script>\""));
    let (_, task) = response(&app, &format!("/tasks/{task_id}")).await;
    assert!(task.contains("&lt;script&gt;"));
    assert!(task.contains("render"));
    assert!(task.contains("audit"));
    let (_, schedule) = response(&app, &format!("/schedules/{schedule_id}")).await;
    assert!(schedule.contains("web-schedule"));
    let (_, worker) = response(&app, &format!("/workers/{worker_id}")).await;
    assert!(worker.contains("test-version"));
    for path in [
        "/tasks/00000000-0000-0000-0000-000000000000",
        "/schedules/00000000-0000-0000-0000-000000000000",
        "/workers/00000000-0000-0000-0000-000000000000",
    ] {
        assert_eq!(response(&app, path).await.0, StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
async fn application_runs_with_a_role_that_can_only_read_observer_views() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let admin = PgPool::connect(&database_url).await.unwrap();
    Store::connect(&database_url).await.unwrap().migrate().await.unwrap();
    let role = format!("pgtask_ui_{}", Uuid::new_v4().simple());
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'observer-test'"
    )))
    .execute(&admin)
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("GRANT USAGE ON SCHEMA pgtask TO {role}")))
        .execute(&admin)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "GRANT SELECT ON \
         pgtask.queue_overview, pgtask.task_view, pgtask.attempt_view, pgtask.worker_view, \
         pgtask.worker_capability_view, pgtask.checkpoint_view, pgtask.schedule_view, \
         pgtask.schedule_occurrence_view, pgtask.signal_view, pgtask.administrator_audit_view TO {role}"
    )))
    .execute(&admin)
    .await
    .unwrap();
    let options = PgConnectOptions::from_str(&database_url)
        .unwrap()
        .username(&role)
        .password("observer-test");
    let observer = PgPool::connect_with(options).await.unwrap();
    let app = application(observer.clone());
    assert_eq!(response(&app, "/").await.0, StatusCode::OK);
    assert!(
        sqlx::query("DELETE FROM pgtask.tasks")
            .execute(&observer)
            .await
            .is_err()
    );
    drop(app);
    observer.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP OWNED BY {role}")))
        .execute(&admin)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP ROLE {role}")))
        .execute(&admin)
        .await
        .unwrap();
}
