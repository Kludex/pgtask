#![doc = "Postgres storage implementation for pgtask."]

mod store;

pub use pgtask_core::{
    STORAGE_PROTOCOL_MAX_VERSION, STORAGE_PROTOCOL_MIN_VERSION, STORAGE_PROTOCOL_RANGE, STORAGE_PROTOCOL_VERSION,
    StorageProtocolRange,
};
pub use store::{
    Notification, PostgresError, QueueDemand, ReadyListener, ResultWait, ResultWaitRequest, SignalWait,
    SignalWaitRequest, SpawnRequest, Store, StoreConfig, TaskCompletion, TaskFailure, TaskResultWait,
};
