use std::{num::NonZeroU32, str::FromStr, time::Duration};

use pgtask_core::{
    EnqueueRequest, HandlerVersion, LeaseToken, NameError, QueueConfig, QueueName, RetryPolicy, ScheduleId,
    ScheduleName, SignalName, StepName, StorageProtocolRange, TaskId, TaskName, TaskState, WorkerId,
};
use serde_json::json;
use uuid::Uuid;

#[test]
fn storage_protocol_ranges_validate_and_overlap() {
    let legacy = StorageProtocolRange::new(1, 1).unwrap();
    let rolling = StorageProtocolRange::new(1, 2).unwrap();
    let future = StorageProtocolRange::new(2, 3).unwrap();

    assert!(legacy.overlaps(rolling));
    assert!(rolling.overlaps(future));
    assert!(!legacy.overlaps(future));
    assert_eq!(StorageProtocolRange::new(0, 1), None);
    assert_eq!(StorageProtocolRange::new(2, 1), None);
}

#[test]
fn names_validate_and_support_their_value_conversions() {
    macro_rules! assert_name {
        ($type:ty, $value:literal) => {{
            let name = <$type>::new($value).unwrap();
            assert_eq!(name.as_str(), $value);
            assert_eq!(name.as_ref(), $value);
            assert_eq!(name.to_string(), $value);
            assert_eq!(String::from(name.clone()), $value);
            assert_eq!(<$type>::try_from($value.to_owned()).unwrap(), name);
            assert_eq!(serde_json::from_value::<$type>(json!($value)).unwrap(), name);
            assert_eq!(serde_json::to_value(name).unwrap(), json!($value));
        }};
    }

    assert_name!(QueueName, "queue");
    assert_name!(ScheduleName, "schedule");
    assert_name!(SignalName, "signal");
    assert_name!(StepName, "step");
    assert_name!(TaskName, "task");
    assert_eq!(QueueName::default().as_str(), "default");

    assert!(matches!(QueueName::new(""), Err(NameError::Empty { .. })));
    assert!(matches!(
        QueueName::new("x".repeat(129)),
        Err(NameError::TooLong {
            maximum: 128,
            actual: 129,
            ..
        })
    ));
    assert!(matches!(
        TaskName::new("not allowed"),
        Err(NameError::UnsupportedCharacter { character: ' ', .. })
    ));
}

#[test]
fn identifiers_support_uuid_round_trips() {
    macro_rules! assert_identifier {
        ($type:ty) => {{
            let generated = <$type>::new();
            assert_eq!(<$type>::from_str(&generated.to_string()).unwrap(), generated);
            assert_eq!(Uuid::from(generated), generated.as_uuid());
            assert_ne!(<$type>::default(), generated);
            let fixed = Uuid::nil();
            assert_eq!(<$type>::from_uuid(fixed).as_uuid(), fixed);
            assert_eq!(serde_json::from_value::<$type>(json!(fixed)).unwrap().as_uuid(), fixed);
        }};
    }

    assert_identifier!(TaskId);
    assert_identifier!(ScheduleId);
    assert_identifier!(WorkerId);
    assert_identifier!(LeaseToken);
    assert!(TaskId::from_str("not-a-uuid").is_err());
}

#[test]
fn task_defaults_and_states_are_explicit() {
    let version = HandlerVersion::new(NonZeroU32::new(7).unwrap());
    assert_eq!(version.get(), 7);
    assert_eq!(HandlerVersion::default().get(), 1);

    let queue = QueueConfig::new(QueueName::new("types").unwrap());
    assert_eq!(queue.terminal_retention, Duration::from_hours(7 * 24));
    assert_eq!(queue.idempotency_retention, Duration::from_hours(30 * 24));

    let request = EnqueueRequest::new(TaskName::new("types.task").unwrap(), json!({"value": 42}));
    assert_eq!(request.queue_name, QueueName::default());
    assert_eq!(request.handler_version, HandlerVersion::default());
    assert_eq!(request.max_attempts, 5);
    assert_eq!(request.payload, json!({"value": 42}));

    for (state, name, terminal) in [
        (TaskState::Pending, "pending", false),
        (TaskState::Running, "running", false),
        (TaskState::Waiting, "waiting", false),
        (TaskState::Succeeded, "succeeded", true),
        (TaskState::Failed, "failed", true),
        (TaskState::Cancelled, "cancelled", true),
    ] {
        assert_eq!(state.as_str(), name);
        assert_eq!(state.is_terminal(), terminal);
        assert_eq!(serde_json::from_value::<TaskState>(json!(name)).unwrap(), state);
    }
}

#[test]
fn retry_defaults_and_overflow_are_bounded() {
    assert!(RetryPolicy::default().delay_for(1).unwrap() <= Duration::from_secs(1));
    let overflowing = RetryPolicy::Exponential {
        base_delay: Duration::MAX,
        factor: u32::MAX,
        max_delay: Duration::MAX,
    };
    assert!(overflowing.delay_for(u16::MAX).unwrap() <= Duration::from_nanos(u64::MAX));
}
