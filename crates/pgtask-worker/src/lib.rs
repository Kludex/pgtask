#![doc = "Worker and scheduler runtime for pgtask."]

mod health;
mod registry;
mod runtime;

pub use pgtask_core::{
    STORAGE_PROTOCOL_MAX_VERSION, STORAGE_PROTOCOL_MIN_VERSION, STORAGE_PROTOCOL_RANGE, STORAGE_PROTOCOL_VERSION,
};
pub use registry::{HandlerError, HandlerRegistry, TaskContext};
pub use runtime::{OverloadProtectionConfig, Worker, WorkerConfig, WorkerControl, WorkerError};
