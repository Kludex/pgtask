use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{HandlerVersion, LeaseToken, QueueName, RetryPolicy, SignalName, StepName, TaskId, TaskName, WorkerId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueConfig {
    pub name: QueueName,
    pub terminal_retention: std::time::Duration,
    pub idempotency_retention: std::time::Duration,
    pub max_outstanding_tasks: Option<std::num::NonZeroU64>,
    pub starvation_timeout: std::time::Duration,
}

impl QueueConfig {
    pub fn new(name: QueueName) -> Self {
        Self {
            name,
            terminal_retention: std::time::Duration::from_hours(7 * 24),
            idempotency_retention: std::time::Duration::from_hours(30 * 24),
            max_outstanding_tasks: None,
            starvation_timeout: std::time::Duration::from_mins(5),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Queue {
    pub name: QueueName,
    pub terminal_retention: std::time::Duration,
    pub idempotency_retention: std::time::Duration,
    pub max_outstanding_tasks: Option<std::num::NonZeroU64>,
    pub starvation_timeout: std::time::Duration,
    pub paused_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseRenewal {
    pub task_id: TaskId,
    pub attempt: u16,
    pub lease_token: LeaseToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerRecord {
    pub id: WorkerId,
    pub queue_name: QueueName,
    pub version: String,
    pub draining: bool,
    pub started_at: DateTime<Utc>,
    pub heartbeat_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub capabilities: Vec<(TaskName, HandlerVersion)>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Checkpoint {
    pub task_id: TaskId,
    pub handler_version: HandlerVersion,
    pub step_name: StepName,
    pub occurrence: u32,
    pub value: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Signal {
    pub task_id: TaskId,
    pub name: SignalName,
    pub occurrence: u32,
    pub value: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Running,
    Waiting,
    Succeeded,
    Failed,
    Cancelled,
}

impl TaskState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TaskResult {
    pub state: TaskState,
    pub result: Option<Value>,
    pub error: Option<Value>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EnqueueRequest {
    pub task_name: TaskName,
    pub handler_version: HandlerVersion,
    pub payload: Value,
    pub queue_name: QueueName,
    pub run_at: Option<DateTime<Utc>>,
    pub priority: i16,
    pub max_attempts: u16,
    pub idempotency_key: Option<String>,
    pub headers: Map<String, Value>,
}

impl EnqueueRequest {
    pub fn new(task_name: TaskName, payload: Value) -> Self {
        Self {
            task_name,
            handler_version: HandlerVersion::default(),
            payload,
            queue_name: QueueName::default(),
            run_at: None,
            priority: 0,
            max_attempts: 5,
            idempotency_key: None,
            headers: Map::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnqueueResult {
    pub task_id: TaskId,
    pub created: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Task {
    pub id: TaskId,
    pub parent_task_id: Option<TaskId>,
    pub queue_name: QueueName,
    pub task_name: TaskName,
    pub handler_version: HandlerVersion,
    pub payload: Value,
    pub headers: Map<String, Value>,
    pub state: TaskState,
    pub priority: i16,
    pub run_at: DateTime<Utc>,
    pub attempt: u16,
    pub max_attempts: u16,
    pub retry_policy: Option<RetryPolicy>,
    pub lease_token: Option<LeaseToken>,
    pub lease_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub result: Option<Value>,
    pub error: Option<Value>,
}
