use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use pgtask_core::{
    EnqueueRequest, HandlerVersion, LeaseToken, RetryPolicy, SignalName, StepName, Task, TaskId, TaskName,
};
use pgtask_postgres::{ResultWait, ResultWaitRequest, SignalWait, SignalWaitRequest, SpawnRequest, Store};
use serde_json::{Value, json};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, HandlerError>> + Send>>;

type HandlerFunction = dyn Fn(Task, TaskContext) -> HandlerFuture + Send + Sync;

#[derive(Clone, Debug, Error)]
#[error("task handler failed")]
pub struct HandlerError {
    pub error: Value,
    pub retryable: bool,
    control: HandlerControl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandlerControl {
    Failure,
    Suspended,
}

impl HandlerError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            error: json!({"type": "handler_error", "message": message.into()}),
            retryable: true,
            control: HandlerControl::Failure,
        }
    }

    pub fn terminal(message: impl Into<String>) -> Self {
        Self {
            error: json!({"type": "handler_error", "message": message.into()}),
            retryable: false,
            control: HandlerControl::Failure,
        }
    }

    fn checkpoint(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            error: json!({"type": kind, "message": message.into()}),
            retryable: true,
            control: HandlerControl::Failure,
        }
    }

    pub fn suspended() -> Self {
        Self {
            error: json!({"type": "suspended"}),
            retryable: false,
            control: HandlerControl::Suspended,
        }
    }

    pub fn is_suspended(&self) -> bool {
        self.control == HandlerControl::Suspended
    }
}

#[derive(Clone)]
pub struct TaskContext {
    store: Store,
    task_id: TaskId,
    handler_version: HandlerVersion,
    attempt: u16,
    lease_token: LeaseToken,
    cancellation: CancellationToken,
}

impl TaskContext {
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub async fn step<F, Fut>(&self, step_name: &StepName, occurrence: u32, operation: F) -> Result<Value, HandlerError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Value, HandlerError>>,
    {
        let span = info_span!(
            "pgtask.checkpoint",
            pgtask.task.id = %self.task_id,
            pgtask.step.name = %step_name,
            pgtask.step.occurrence = occurrence,
        );
        async {
            if self.cancellation.is_cancelled() {
                return Err(HandlerError::checkpoint("lease_lost", "task lease is no longer active"));
            }
            if let Some(checkpoint) = self
                .store
                .get_checkpoint(self.task_id, self.handler_version, step_name, occurrence)
                .await
                .map_err(|error| HandlerError::checkpoint("checkpoint_read_error", error.to_string()))?
            {
                return Ok(checkpoint.value);
            }
            let value = operation().await?;
            self.store
                .commit_checkpoint(
                    self.task_id,
                    self.attempt,
                    self.lease_token,
                    step_name,
                    occurrence,
                    &value,
                )
                .await
                .map_err(|error| HandlerError::checkpoint("checkpoint_write_error", error.to_string()))?
                .map(|checkpoint| checkpoint.value)
                .ok_or_else(|| HandlerError::checkpoint("lease_lost", "task lease is no longer active"))
        }
        .instrument(span)
        .await
    }

    pub async fn sleep_until(
        &self,
        step_name: &StepName,
        occurrence: u32,
        wake_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), HandlerError> {
        if self.checkpoint_exists(step_name, occurrence).await? {
            return Ok(());
        }
        self.store
            .sleep_until(
                self.task_id,
                self.attempt,
                self.lease_token,
                step_name,
                occurrence,
                wake_at,
            )
            .await
            .map_err(|error| HandlerError::checkpoint("sleep_write_error", error.to_string()))?
            .ok_or_else(|| HandlerError::checkpoint("lease_lost", "task lease is no longer active"))?;
        Err(HandlerError::suspended())
    }

    pub async fn sleep_for(
        &self,
        step_name: &StepName,
        occurrence: u32,
        duration: std::time::Duration,
    ) -> Result<(), HandlerError> {
        if self.checkpoint_exists(step_name, occurrence).await? {
            return Ok(());
        }
        self.store
            .sleep_for(
                self.task_id,
                self.attempt,
                self.lease_token,
                step_name,
                occurrence,
                duration,
            )
            .await
            .map_err(|error| HandlerError::checkpoint("sleep_write_error", error.to_string()))?
            .ok_or_else(|| HandlerError::checkpoint("lease_lost", "task lease is no longer active"))?;
        Err(HandlerError::suspended())
    }

    pub async fn wait_for_signal(
        &self,
        step_name: &StepName,
        occurrence: u32,
        signal_name: &SignalName,
        signal_occurrence: u32,
        timeout: Option<std::time::Duration>,
    ) -> Result<Option<Value>, HandlerError> {
        if let Some(checkpoint) = self
            .store
            .get_checkpoint(self.task_id, self.handler_version, step_name, occurrence)
            .await
            .map_err(|error| HandlerError::checkpoint("checkpoint_read_error", error.to_string()))?
        {
            return decode_signal_checkpoint(&checkpoint.value);
        }
        match self
            .store
            .wait_for_signal(SignalWaitRequest {
                task_id: self.task_id,
                attempt: self.attempt,
                lease_token: self.lease_token,
                step_name,
                occurrence,
                signal_name,
                signal_occurrence,
                timeout,
            })
            .await
            .map_err(|error| HandlerError::checkpoint("signal_wait_error", error.to_string()))?
        {
            Some(SignalWait::Ready(checkpoint)) => decode_signal_checkpoint(&checkpoint),
            Some(SignalWait::Waiting) => Err(HandlerError::suspended()),
            None => Err(HandlerError::checkpoint("lease_lost", "task lease is no longer active")),
        }
    }

    pub async fn wait_for_result(
        &self,
        step_name: &StepName,
        occurrence: u32,
        result_task_id: TaskId,
    ) -> Result<Value, HandlerError> {
        if let Some(checkpoint) = self
            .store
            .get_checkpoint(self.task_id, self.handler_version, step_name, occurrence)
            .await
            .map_err(|error| HandlerError::checkpoint("checkpoint_read_error", error.to_string()))?
        {
            return Ok(checkpoint.value);
        }
        match self
            .store
            .wait_for_result(ResultWaitRequest {
                task_id: self.task_id,
                attempt: self.attempt,
                lease_token: self.lease_token,
                step_name,
                occurrence,
                result_task_id,
            })
            .await
            .map_err(|error| HandlerError::checkpoint("result_wait_error", error.to_string()))?
        {
            Some(ResultWait::Ready(checkpoint)) => Ok(checkpoint),
            Some(ResultWait::Waiting) => Err(HandlerError::suspended()),
            None => Err(HandlerError::checkpoint("lease_lost", "task lease is no longer active")),
        }
    }

    pub async fn spawn(
        &self,
        step_name: &StepName,
        occurrence: u32,
        request: &EnqueueRequest,
    ) -> Result<TaskId, HandlerError> {
        self.store
            .spawn_task(SpawnRequest {
                parent_task_id: self.task_id,
                parent_attempt: self.attempt,
                parent_lease_token: self.lease_token,
                step_name,
                occurrence,
                task: request,
            })
            .await
            .map_err(|error| HandlerError::checkpoint("child_spawn_error", error.to_string()))?
            .map(|result| result.task_id)
            .ok_or_else(|| HandlerError::checkpoint("lease_lost", "task lease is no longer active"))
    }

    async fn checkpoint_exists(&self, step_name: &StepName, occurrence: u32) -> Result<bool, HandlerError> {
        self.store
            .get_checkpoint(self.task_id, self.handler_version, step_name, occurrence)
            .await
            .map(|checkpoint| checkpoint.is_some())
            .map_err(|error| HandlerError::checkpoint("checkpoint_read_error", error.to_string()))
    }

    pub(crate) fn new(store: Store, task: &Task, lease_token: LeaseToken, cancellation: CancellationToken) -> Self {
        Self {
            store,
            task_id: task.id,
            handler_version: task.handler_version,
            attempt: task.attempt,
            lease_token,
            cancellation,
        }
    }
}

fn decode_signal_checkpoint(checkpoint: &Value) -> Result<Option<Value>, HandlerError> {
    let Some(checkpoint) = checkpoint.as_object() else {
        return Err(HandlerError::terminal("signal checkpoint is not an object"));
    };
    match checkpoint.get("outcome").and_then(Value::as_str) {
        Some("signal") => checkpoint
            .get("value")
            .cloned()
            .map(Some)
            .ok_or_else(|| HandlerError::terminal("signal checkpoint has no value")),
        Some("timeout") => Ok(None),
        _ => Err(HandlerError::terminal("signal checkpoint has an invalid outcome")),
    }
}

#[derive(Clone)]
pub(crate) struct RegisteredHandler {
    pub function: Arc<HandlerFunction>,
    pub retry_policy: RetryPolicy,
}

#[derive(Clone, Default)]
pub struct HandlerRegistry {
    handlers: HashMap<(TaskName, HandlerVersion), RegisteredHandler>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F, Fut>(
        &mut self,
        task_name: TaskName,
        handler_version: HandlerVersion,
        retry_policy: RetryPolicy,
        handler: F,
    ) -> bool
    where
        F: Fn(Task) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, HandlerError>> + Send + 'static,
    {
        let registered = RegisteredHandler {
            function: Arc::new(move |task, _context| Box::pin(handler(task))),
            retry_policy,
        };
        self.handlers.insert((task_name, handler_version), registered).is_none()
    }

    pub fn register_durable<F, Fut>(
        &mut self,
        task_name: TaskName,
        handler_version: HandlerVersion,
        retry_policy: RetryPolicy,
        handler: F,
    ) -> bool
    where
        F: Fn(Task, TaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, HandlerError>> + Send + 'static,
    {
        let registered = RegisteredHandler {
            function: Arc::new(move |task, context| Box::pin(handler(task, context))),
            retry_policy,
        };
        self.handlers.insert((task_name, handler_version), registered).is_none()
    }

    pub fn capabilities(&self) -> Vec<(TaskName, HandlerVersion)> {
        self.handlers.keys().cloned().collect()
    }

    pub(crate) fn get(&self, task_name: &TaskName, handler_version: HandlerVersion) -> Option<&RegisteredHandler> {
        self.handlers.get(&(task_name.clone(), handler_version))
    }
}
