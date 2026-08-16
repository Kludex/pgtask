#![doc = "Postgres storage implementation for pgtask."]

mod store;

pub use pgtask_core::STORAGE_PROTOCOL_VERSION;
pub use store::{
    Notification, PostgresError, ReadyListener, ResultWait, ResultWaitRequest, SignalWait, SignalWaitRequest,
    SpawnRequest, Store, StoreConfig, TaskResultWait,
};
