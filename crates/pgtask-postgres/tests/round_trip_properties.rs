//! Property tests for the Rust/PostgreSQL boundary.
//!
//! The Rust types and the SQL `CHECK` constraints are two independent
//! statements of the same rules, maintained by hand in two places. These check
//! they still agree: a value the Rust constructor accepts should be storable,
//! and reading it back should give the same value.
//!
//! Case counts are deliberately small by default because each case is a round
//! trip to a real database; raise with `PROPTEST_CASES`.

use std::{sync::OnceLock, time::Duration};

use pgtask_core::{
    EnqueueRequest, MisfirePolicy, QueueName, ScheduleConfig, ScheduleDefinition, ScheduleName, TaskName,
};
use pgtask_postgres::Store;
use proptest::prelude::*;
use serde_json::json;
use tokio::runtime::Runtime;
use uuid::Uuid;

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(48)
}

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: cases(),
        ..ProptestConfig::default()
    }
}

/// One runtime and one migrated store for the whole file: building either per
/// case would dominate the run time.
fn store() -> Option<&'static (Runtime, Store)> {
    static STORE: OnceLock<Option<(Runtime, Store)>> = OnceLock::new();
    STORE
        .get_or_init(|| {
            let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
                return None;
            };
            let runtime = Runtime::new().expect("failed to create the Tokio runtime");
            let store = runtime
                .block_on(Store::connect(&database_url))
                .expect("failed to connect to PGTASK_DATABASE_URL");
            runtime
                .block_on(store.migrate())
                .expect("failed to migrate the database");
            Some((runtime, store))
        })
        .as_ref()
}

/// Characters the Rust name types accept, so the generated names are exactly
/// the set the constructors call legal.
fn any_name(max_len: usize) -> impl Strategy<Value = String> {
    proptest::collection::vec(
        prop_oneof![
            proptest::char::range('a', 'z'),
            proptest::char::range('A', 'Z'),
            proptest::char::range('0', '9'),
            Just('.'),
            Just('_'),
            Just(':'),
            Just('-'),
        ],
        1..max_len,
    )
    .prop_map(|characters| characters.into_iter().collect())
}

proptest! {
    #![proptest_config(config())]

    /// A task name the Rust type accepts must be one `pgtask.enqueue` accepts.
    /// Both sides spell out the same rule separately, so they can drift.
    #[test]
    fn any_valid_task_name_is_accepted_by_the_database(suffix in any_name(200)) {
        let Some((runtime, store)) = store() else { return Ok(()); };

        // Prefixed so concurrent runs of this test cannot collide, and still
        // within the 255 byte limit the Rust type enforces.
        let raw = format!("prop-{suffix}");
        let task_name = TaskName::new(raw.clone()).expect("generated from the legal character set");
        let queue = QueueName::new(format!("prop-names-{}", Uuid::new_v4())).unwrap();

        let mut request = EnqueueRequest::new(task_name, json!({}));
        request.queue_name = queue;

        let result = runtime.block_on(store.enqueue(&request));
        prop_assert!(
            result.is_ok(),
            "Rust accepted the task name {raw:?} but the database rejected it: {:?}",
            result.err()
        );
    }

    /// Storing a schedule and reading it back must give the same definition.
    #[test]
    fn a_stored_interval_schedule_reads_back_unchanged(every_millis in 1_u64..10_000) {
        let Some((runtime, store)) = store() else { return Ok(()); };

        let every = Duration::from_millis(every_millis);
        let definition = ScheduleDefinition::interval(every).expect("non-zero interval");

        let queue = QueueName::new(format!("prop-sched-{}", Uuid::new_v4())).unwrap();
        let mut task = EnqueueRequest::new(TaskName::new("prop.scheduled").unwrap(), json!({}));
        task.queue_name = queue;
        let name = ScheduleName::new(format!("prop-{}", Uuid::new_v4())).unwrap();
        let config = ScheduleConfig::new(name, definition.clone(), task);

        let stored = runtime.block_on(store.put_schedule(&config));
        let stored = match stored {
            Ok(stored) => stored,
            Err(error) => {
                prop_assert!(
                    false,
                    "Rust accepted an interval of {every:?} but the database rejected it: {error:?}"
                );
                unreachable!()
            }
        };

        prop_assert_eq!(
            stored.config.definition,
            definition,
            "an interval of {:?} did not survive the round trip",
            every
        );
    }

    /// A sub-millisecond interval is constructible but truncates to zero on the
    /// way to a database that only stores whole milliseconds.
    #[test]
    fn a_sub_millisecond_interval_is_not_storable(every_micros in 1_u64..1_000) {
        let Some((runtime, store)) = store() else { return Ok(()); };

        let every = Duration::from_micros(every_micros);
        let definition = ScheduleDefinition::interval(every).expect("non-zero interval");

        let queue = QueueName::new(format!("prop-subms-{}", Uuid::new_v4())).unwrap();
        let mut task = EnqueueRequest::new(TaskName::new("prop.scheduled").unwrap(), json!({}));
        task.queue_name = queue;
        let name = ScheduleName::new(format!("prop-{}", Uuid::new_v4())).unwrap();
        let config = ScheduleConfig::new(name, definition, task);

        let result = runtime.block_on(store.put_schedule(&config));
        prop_assert!(
            result.is_err(),
            "the database stored a sub-millisecond interval as if it were {every:?}"
        );
    }

    /// The misfire policy is an enum on both sides; every variant must survive.
    #[test]
    fn a_stored_misfire_policy_reads_back_unchanged(
        policy in prop_oneof![
            Just(MisfirePolicy::Skip),
            Just(MisfirePolicy::Latest),
            (1_u16..1_000).prop_map(|limit| MisfirePolicy::CatchUp {
                limit: std::num::NonZeroU16::new(limit).unwrap()
            }),
        ],
    ) {
        let Some((runtime, store)) = store() else { return Ok(()); };

        let queue = QueueName::new(format!("prop-misfire-{}", Uuid::new_v4())).unwrap();
        let mut task = EnqueueRequest::new(TaskName::new("prop.scheduled").unwrap(), json!({}));
        task.queue_name = queue;
        let name = ScheduleName::new(format!("prop-{}", Uuid::new_v4())).unwrap();
        let mut config = ScheduleConfig::new(
            name,
            ScheduleDefinition::interval(Duration::from_mins(1)).unwrap(),
            task,
        );
        config.misfire_policy = policy;

        let stored = runtime.block_on(store.put_schedule(&config)).expect("storable schedule");
        prop_assert_eq!(stored.config.misfire_policy, policy);
    }
}
