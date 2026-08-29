#![doc = "Worker and scheduler runtime for pgtask."]

mod health;
mod registry;
mod runtime;

pub use pgtask_core::StorageProtocolRange;
pub use registry::{HandlerError, HandlerRegistry, TaskContext};
pub use runtime::{OverloadProtectionConfig, Worker, WorkerConfig, WorkerControl, WorkerError};

pub const STORAGE_PROTOCOL_MIN_VERSION: u32 = 2;
pub const STORAGE_PROTOCOL_MAX_VERSION: u32 = 2;
pub const STORAGE_PROTOCOL_VERSION: u32 = STORAGE_PROTOCOL_MAX_VERSION;
pub const STORAGE_PROTOCOL_RANGE: StorageProtocolRange = StorageProtocolRange {
    minimum: STORAGE_PROTOCOL_MIN_VERSION,
    maximum: STORAGE_PROTOCOL_MAX_VERSION,
};
