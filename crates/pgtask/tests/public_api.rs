use std::{num::NonZeroU16, time::Duration};

use pgtask::{
    core::{EnqueueRequest, HandlerVersion, QueueName, RetryPolicy, TaskName},
    postgres::Store,
    worker::{HandlerRegistry, Worker, WorkerConfig, WorkerError},
};
use serde_json::json;

#[test]
fn reference_crate_exposes_the_supported_engine_surface() {
    let queue_name = QueueName::new("public-contract").unwrap();
    let task_name = TaskName::new("contract.echo").unwrap();
    let request = EnqueueRequest::new(task_name.clone(), json!({"value": 42}));
    assert_eq!(request.queue_name.as_str(), "default");

    let mut registry = HandlerRegistry::new();
    assert!(registry.register(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |task| async move { Ok(task.payload) },
    ));
    let mut config = WorkerConfig::new(queue_name);
    config.concurrency = NonZeroU16::new(4).unwrap();
    config.poll_interval = Duration::from_secs(30);

    std::hint::black_box(Store::connect);
    std::hint::black_box(Worker::new as fn(Store, HandlerRegistry, WorkerConfig) -> Result<Worker, WorkerError>);
    assert_eq!(config.concurrency.get(), 4);
}
