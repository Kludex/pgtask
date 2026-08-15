#![doc = "Core types and state transitions for pgtask."]

mod identifier;
mod retry;
mod schedule;
mod task;

pub use identifier::{
    HandlerVersion, LeaseToken, NameError, QueueName, ScheduleId, ScheduleName, SignalName, StepName, TaskId, TaskName,
    WorkerId,
};
pub use retry::RetryPolicy;
pub use schedule::{Materialization, MisfirePolicy, Schedule, ScheduleConfig, ScheduleDefinition, ScheduleError};
pub use task::{
    Checkpoint, EnqueueRequest, EnqueueResult, LeaseRenewal, Queue, QueueConfig, Signal, Task, TaskResult, TaskState,
    WorkerRecord,
};

/// Current development version of the storage protocol.
pub const STORAGE_PROTOCOL_VERSION: u32 = 1;
