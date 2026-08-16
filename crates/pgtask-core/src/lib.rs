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

/// Oldest storage protocol understood by this release.
pub const STORAGE_PROTOCOL_MIN_VERSION: u32 = 1;

/// Newest storage protocol understood by this release.
pub const STORAGE_PROTOCOL_MAX_VERSION: u32 = 1;

/// Current storage protocol emitted by this release.
pub const STORAGE_PROTOCOL_VERSION: u32 = STORAGE_PROTOCOL_MAX_VERSION;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageProtocolRange {
    pub minimum: u32,
    pub maximum: u32,
}

impl StorageProtocolRange {
    pub const fn new(minimum: u32, maximum: u32) -> Option<Self> {
        if minimum == 0 || maximum < minimum {
            return None;
        }
        Some(Self { minimum, maximum })
    }

    pub const fn overlaps(self, other: Self) -> bool {
        self.minimum <= other.maximum && other.minimum <= self.maximum
    }
}

pub const STORAGE_PROTOCOL_RANGE: StorageProtocolRange = StorageProtocolRange {
    minimum: STORAGE_PROTOCOL_MIN_VERSION,
    maximum: STORAGE_PROTOCOL_MAX_VERSION,
};
