use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(FromRow)]
pub struct QueueSummary {
    pub name: String,
    pub paused_at: Option<DateTime<Utc>>,
    pub pending_count: i64,
    pub ready_count: i64,
    pub routable_count: i64,
    pub unroutable_count: i64,
    pub running_count: i64,
    pub waiting_count: i64,
    pub terminal_count: i64,
}

impl QueueSummary {
    async fn all(pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as(
            "SELECT name, paused_at, pending_count, ready_count, routable_count, unroutable_count, \
             running_count, waiting_count, terminal_count \
             FROM pgtask.queue_overview ORDER BY name",
        )
        .fetch_all(pool)
        .await
    }
}

#[derive(FromRow)]
pub struct TaskSummary {
    pub id: Uuid,
    pub queue_name: String,
    pub task_name: String,
    pub state: String,
    pub attempt: i32,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl TaskSummary {
    pub async fn search(pool: &PgPool, query: Option<&str>) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as(
            "SELECT id, queue_name, task_name, state, attempt, created_at, completed_at \
             FROM pgtask.task_view \
             WHERE $1::text IS NULL OR task_name ILIKE '%' || $1 || '%' OR id::text = $1 \
             ORDER BY created_at DESC LIMIT 100",
        )
        .bind(query)
        .fetch_all(pool)
        .await
    }
}

#[derive(FromRow)]
pub struct Attempt {
    #[sqlx(rename = "attempt")]
    pub number: i32,
    pub state: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

#[derive(FromRow)]
pub struct Checkpoint {
    pub step_name: String,
    pub occurrence: i32,
    pub value: String,
    pub created_at: DateTime<Utc>,
}

#[derive(FromRow)]
pub struct Signal {
    #[sqlx(rename = "signal_name")]
    pub name: String,
    pub occurrence: i32,
    pub value: String,
    pub created_at: DateTime<Utc>,
}

#[derive(FromRow)]
pub struct AdministratorAudit {
    pub actor: String,
    pub action: String,
    pub occurred_at: DateTime<Utc>,
}

#[derive(FromRow)]
pub struct TaskDetail {
    pub id: Uuid,
    pub parent_task_id: Option<Uuid>,
    pub queue_name: String,
    pub task_name: String,
    pub handler_version: i32,
    pub state: String,
    pub attempt: i32,
    pub max_attempts: i32,
    pub payload: String,
    pub result: Option<String>,
    pub error: Option<String>,
    pub run_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    #[sqlx(skip)]
    pub attempts: Vec<Attempt>,
    #[sqlx(skip)]
    pub checkpoints: Vec<Checkpoint>,
    #[sqlx(skip)]
    pub signals: Vec<Signal>,
    #[sqlx(skip)]
    pub administrator_audit: Vec<AdministratorAudit>,
}

impl TaskDetail {
    pub async fn load(pool: &PgPool, task_id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        let Some(mut task): Option<Self> = sqlx::query_as(
            "SELECT id, parent_task_id, queue_name, task_name, handler_version, state, attempt, max_attempts, \
             payload::text AS payload, result::text AS result, error::text AS error, run_at, created_at, completed_at \
             FROM pgtask.task_view WHERE id = $1",
        )
        .bind(task_id)
        .fetch_optional(pool)
        .await?
        else {
            return Ok(None);
        };
        let details = tokio::try_join!(
            sqlx::query_as(
                "SELECT attempt, state, started_at, finished_at, error::text AS error \
                 FROM pgtask.attempt_view WHERE task_id = $1 ORDER BY attempt"
            )
            .bind(task_id)
            .fetch_all(pool),
            sqlx::query_as(
                "SELECT step_name, occurrence, value::text AS value, created_at \
                 FROM pgtask.checkpoint_view WHERE task_id = $1 ORDER BY created_at"
            )
            .bind(task_id)
            .fetch_all(pool),
            sqlx::query_as(
                "SELECT signal_name, occurrence, value::text AS value, created_at \
                 FROM pgtask.signal_view WHERE task_id = $1 ORDER BY created_at"
            )
            .bind(task_id)
            .fetch_all(pool),
            sqlx::query_as(
                "SELECT actor, action, occurred_at FROM pgtask.administrator_audit_view \
                 WHERE task_id = $1 ORDER BY occurred_at"
            )
            .bind(task_id)
            .fetch_all(pool),
        );
        let (attempts, checkpoints, signals, administrator_audit) = details?;
        task.attempts = attempts;
        task.checkpoints = checkpoints;
        task.signals = signals;
        task.administrator_audit = administrator_audit;
        Ok(Some(task))
    }
}

#[derive(FromRow)]
pub struct ScheduleSummary {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub queue_name: String,
    pub task_name: String,
    pub next_run_at: DateTime<Utc>,
    pub paused_at: Option<DateTime<Utc>>,
}

impl ScheduleSummary {
    pub async fn all(pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as(
            "SELECT id, name, kind, queue_name, task_name, next_run_at, paused_at \
             FROM pgtask.schedule_view ORDER BY name",
        )
        .fetch_all(pool)
        .await
    }
}

#[derive(FromRow)]
pub struct ScheduleOccurrence {
    pub scheduled_for: DateTime<Utc>,
    pub task_id: Uuid,
    pub state: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

pub struct ScheduleDetail {
    pub schedule: ScheduleSummary,
    pub occurrences: Vec<ScheduleOccurrence>,
    pub administrator_audit: Vec<AdministratorAudit>,
}

impl ScheduleDetail {
    pub async fn load(pool: &PgPool, schedule_id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        let schedule = sqlx::query_as(
            "SELECT id, name, kind, queue_name, task_name, next_run_at, paused_at \
             FROM pgtask.schedule_view WHERE id = $1",
        )
        .bind(schedule_id)
        .fetch_optional(pool)
        .await?;
        let Some(schedule) = schedule else {
            return Ok(None);
        };
        let occurrences = sqlx::query_as(
            "SELECT scheduled_for, task_id, state, created_at, completed_at \
             FROM pgtask.schedule_occurrence_view WHERE schedule_id = $1 ORDER BY scheduled_for DESC LIMIT 100",
        )
        .bind(schedule_id)
        .fetch_all(pool)
        .await?;
        let administrator_audit = sqlx::query_as(
            "SELECT actor, action, occurred_at FROM pgtask.administrator_audit_view \
             WHERE schedule_id = $1 ORDER BY occurred_at",
        )
        .bind(schedule_id)
        .fetch_all(pool)
        .await?;
        Ok(Some(Self {
            schedule,
            occurrences,
            administrator_audit,
        }))
    }
}

#[derive(FromRow)]
pub struct WorkerSummary {
    pub id: Uuid,
    pub queue_name: String,
    pub version: String,
    pub draining: bool,
    pub live: bool,
    pub heartbeat_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl WorkerSummary {
    pub async fn all(pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as(
            "SELECT id, queue_name, version, draining, live, heartbeat_at, expires_at \
             FROM pgtask.worker_view ORDER BY heartbeat_at DESC",
        )
        .fetch_all(pool)
        .await
    }
}

#[derive(FromRow)]
pub struct Capability {
    pub task_name: String,
    pub handler_version: i32,
}

pub struct WorkerDetail {
    pub worker: WorkerSummary,
    pub capabilities: Vec<Capability>,
}

impl WorkerDetail {
    pub async fn load(pool: &PgPool, worker_id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        let worker = sqlx::query_as(
            "SELECT id, queue_name, version, draining, live, heartbeat_at, expires_at \
             FROM pgtask.worker_view WHERE id = $1",
        )
        .bind(worker_id)
        .fetch_optional(pool)
        .await?;
        let Some(worker) = worker else {
            return Ok(None);
        };
        let capabilities = sqlx::query_as(
            "SELECT task_name, handler_version FROM pgtask.worker_capability_view \
             WHERE worker_id = $1 ORDER BY task_name, handler_version",
        )
        .bind(worker_id)
        .fetch_all(pool)
        .await?;
        Ok(Some(Self { worker, capabilities }))
    }
}

pub struct Dashboard {
    pub queues: Vec<QueueSummary>,
    pub tasks: Vec<TaskSummary>,
    pub schedules: Vec<ScheduleSummary>,
    pub workers: Vec<WorkerSummary>,
}

impl Dashboard {
    pub async fn load(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let (queues, tasks, schedules, workers) = tokio::try_join!(
            QueueSummary::all(pool),
            TaskSummary::search(pool, None),
            ScheduleSummary::all(pool),
            WorkerSummary::all(pool),
        )?;
        Ok(Self {
            queues,
            tasks,
            schedules,
            workers,
        })
    }
}
