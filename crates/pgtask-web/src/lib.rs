#![doc = "Read-only web interface for pgtask."]

mod model;
mod pages;

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderName, StatusCode},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use serde::Deserialize;
use sqlx::PgPool;
use thiserror::Error;
use uuid::Uuid;

use crate::model::{Dashboard, ScheduleDetail, TaskDetail, WorkerDetail};

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    administrator: Option<AdministratorConfig>,
}

#[derive(Clone, Debug)]
pub struct AdministratorConfig {
    pub actor_header: HeaderName,
}

impl Default for AdministratorConfig {
    fn default() -> Self {
        Self {
            actor_header: HeaderName::from_static("x-pgtask-actor"),
        }
    }
}

#[derive(Debug, Error)]
enum WebError {
    #[error("database query failed")]
    Database(#[from] sqlx::Error),
    #[error("resource not found")]
    NotFound,
    #[error("administrator identity is required")]
    Unauthorized,
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
        };
        (status, Html(pages::error(status.as_u16(), &self.to_string()))).into_response()
    }
}

#[derive(Deserialize)]
struct TaskSearch {
    query: Option<String>,
}

pub fn application(pool: PgPool) -> Router {
    build_application(pool, None)
}

pub fn application_with_administrator(pool: PgPool, config: AdministratorConfig) -> Router {
    build_application(pool, Some(config))
}

fn build_application(pool: PgPool, administrator: Option<AdministratorConfig>) -> Router {
    let mut router = Router::new()
        .route("/", get(dashboard))
        .route("/healthz", get(health))
        .route("/tasks", get(tasks))
        .route("/tasks/{task_id}", get(task))
        .route("/schedules", get(schedules))
        .route("/schedules/{schedule_id}", get(schedule))
        .route("/workers", get(workers))
        .route("/workers/{worker_id}", get(worker));
    if administrator.is_some() {
        router = router
            .route("/admin/tasks/{task_id}/cancel", post(cancel_task))
            .route("/admin/tasks/{task_id}/retry", post(retry_task))
            .route("/admin/schedules/{schedule_id}/pause", post(pause_schedule))
            .route("/admin/schedules/{schedule_id}/resume", post(resume_schedule));
    }
    router.with_state(AppState { pool, administrator })
}

async fn health(State(state): State<AppState>) -> Result<&'static str, WebError> {
    sqlx::query("SELECT 1").execute(&state.pool).await?;
    Ok("healthy")
}

async fn dashboard(State(state): State<AppState>) -> Result<Html<String>, WebError> {
    Ok(Html(pages::dashboard(&Dashboard::load(&state.pool).await?)))
}

async fn tasks(State(state): State<AppState>, Query(search): Query<TaskSearch>) -> Result<Html<String>, WebError> {
    Ok(Html(pages::tasks(
        &model::TaskSummary::search(&state.pool, search.query.as_deref()).await?,
        search.query.as_deref(),
    )))
}

async fn task(State(state): State<AppState>, Path(task_id): Path<Uuid>) -> Result<Html<String>, WebError> {
    let detail = TaskDetail::load(&state.pool, task_id)
        .await?
        .ok_or(WebError::NotFound)?;
    Ok(Html(pages::task(&detail, state.administrator.is_some())))
}

async fn schedules(State(state): State<AppState>) -> Result<Html<String>, WebError> {
    Ok(Html(pages::schedules(&model::ScheduleSummary::all(&state.pool).await?)))
}

async fn schedule(State(state): State<AppState>, Path(schedule_id): Path<Uuid>) -> Result<Html<String>, WebError> {
    let detail = ScheduleDetail::load(&state.pool, schedule_id)
        .await?
        .ok_or(WebError::NotFound)?;
    Ok(Html(pages::schedule(&detail, state.administrator.is_some())))
}

async fn workers(State(state): State<AppState>) -> Result<Html<String>, WebError> {
    Ok(Html(pages::workers(&model::WorkerSummary::all(&state.pool).await?)))
}

async fn worker(State(state): State<AppState>, Path(worker_id): Path<Uuid>) -> Result<Html<String>, WebError> {
    let detail = WorkerDetail::load(&state.pool, worker_id)
        .await?
        .ok_or(WebError::NotFound)?;
    Ok(Html(pages::worker(&detail)))
}

fn administrator_actor<'a>(state: &AppState, headers: &'a HeaderMap) -> Result<&'a str, WebError> {
    let config = state.administrator.as_ref().ok_or(WebError::NotFound)?;
    headers
        .get(&config.actor_header)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .ok_or(WebError::Unauthorized)
}

async fn cancel_task(
    State(state): State<AppState>,
    Path(task_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Redirect, WebError> {
    let actor = administrator_actor(&state, &headers)?;
    let changed: bool = sqlx::query_scalar("SELECT pgtask.admin_cancel_task($1, $2)")
        .bind(task_id)
        .bind(actor)
        .fetch_one(&state.pool)
        .await?;
    if !changed {
        return Err(WebError::NotFound);
    }
    Ok(Redirect::to(&format!("/tasks/{task_id}")))
}

async fn retry_task(
    State(state): State<AppState>,
    Path(task_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Redirect, WebError> {
    let actor = administrator_actor(&state, &headers)?;
    let changed: bool = sqlx::query_scalar("SELECT pgtask.admin_retry_task($1, $2)")
        .bind(task_id)
        .bind(actor)
        .fetch_one(&state.pool)
        .await?;
    if !changed {
        return Err(WebError::NotFound);
    }
    Ok(Redirect::to(&format!("/tasks/{task_id}")))
}

async fn pause_schedule(
    State(state): State<AppState>,
    Path(schedule_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Redirect, WebError> {
    set_schedule_paused(&state, schedule_id, &headers, true).await
}

async fn resume_schedule(
    State(state): State<AppState>,
    Path(schedule_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Redirect, WebError> {
    set_schedule_paused(&state, schedule_id, &headers, false).await
}

async fn set_schedule_paused(
    state: &AppState,
    schedule_id: Uuid,
    headers: &HeaderMap,
    paused: bool,
) -> Result<Redirect, WebError> {
    let actor = administrator_actor(state, headers)?;
    let changed: bool = sqlx::query_scalar("SELECT pgtask.admin_set_schedule_paused($1, $2, $3)")
        .bind(schedule_id)
        .bind(paused)
        .bind(actor)
        .fetch_one(&state.pool)
        .await?;
    if !changed {
        return Err(WebError::NotFound);
    }
    Ok(Redirect::to(&format!("/schedules/{schedule_id}")))
}
