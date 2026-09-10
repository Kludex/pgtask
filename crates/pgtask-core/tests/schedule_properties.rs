//! Property tests for schedule materialisation.
//!
//! `ScheduleDefinition` is the pure core of the scheduler: given where a
//! schedule last got to and what time it is now, it decides which occurrences to
//! create and where the cursor lands next. It needs no database and no clock, so
//! it can be hammered with thousands of cases in a fraction of a second.
//!
//! The properties below are the ones `materialize`'s callers rely on. Every
//! `next_run_at` it returns is written straight back to `pgtask.schedules`, and
//! every occurrence becomes a task row keyed by `(schedule_id, scheduled_for)`,
//! so a cursor that fails to advance means a schedule that either stalls or
//! spins, and an out-of-order occurrence means a duplicate key.
//!
//! Case count follows `PROPTEST_CASES` so CI can turn it up.

use std::{num::NonZeroU16, time::Duration};

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use pgtask_core::{MisfirePolicy, ScheduleDefinition, ScheduleError};
use proptest::prelude::*;

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(512)
}

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: cases(),
        ..ProptestConfig::default()
    }
}

fn epoch() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
}

fn first_due(definition: &ScheduleDefinition) -> DateTime<Utc> {
    definition
        .next_after(epoch())
        .expect("generated schedule has a future occurrence")
}

/// Intervals the constructor accepts: whole milliseconds, which is also what
/// the database stores.
fn any_interval() -> impl Strategy<Value = Duration> {
    prop_oneof![
        (1_u64..86_400_000).prop_map(Duration::from_millis),
        (1_u64..86_400).prop_map(Duration::from_secs),
    ]
}

fn any_policy() -> impl Strategy<Value = MisfirePolicy> {
    prop_oneof![
        Just(MisfirePolicy::Skip),
        Just(MisfirePolicy::Latest),
        (1_u16..64).prop_map(|limit| MisfirePolicy::CatchUp {
            limit: NonZeroU16::new(limit).unwrap()
        }),
    ]
}

/// A few real cron expressions rather than generated gibberish; the parser is
/// the `cron` crate's problem, the arithmetic around it is ours.
fn any_cron() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("0 * * * * *".to_owned()),    // every minute
        Just("0 0 * * * *".to_owned()),    // hourly
        Just("*/5 * * * * *".to_owned()),  // every 5 seconds
        Just("0 30 9 * * *".to_owned()),   // 09:30 daily
        Just("0 0 0 1 * *".to_owned()),    // monthly
        Just("0 0 12 * * Mon".to_owned()), // Mondays at noon
    ]
}

fn any_definition() -> impl Strategy<Value = ScheduleDefinition> {
    prop_oneof![
        any_interval().prop_map(|every| ScheduleDefinition::interval(every).unwrap()),
        any_cron().prop_map(|expression| ScheduleDefinition::cron(expression).unwrap()),
    ]
}

/// How far behind the cursor is, in seconds. Zero means on time.
fn any_lateness() -> impl Strategy<Value = i64> {
    prop_oneof![Just(0_i64), 1_i64..60, 60_i64..86_400, 86_400_i64..(86_400 * 40),]
}

proptest! {
    #![proptest_config(config())]

    /// Whatever the definition and however far behind the cursor is,
    /// materialisation is an ordinary result, not a crash. Every caller sits
    /// inside a worker loop that handles `Err` and nothing else.
    #[test]
    fn materialize_never_panics(
        definition in any_definition(),
        policy in any_policy(),
        lateness in any_lateness(),
    ) {
        let next_run_at = first_due(&definition);
        let now = next_run_at + TimeDelta::seconds(lateness);
        let _ = definition.materialize(next_run_at, now, policy);
    }

    /// The cursor must land strictly in the future, or the scheduler either
    /// stalls on the same instant or spins re-materialising it.
    #[test]
    fn the_cursor_always_advances_past_now(
        definition in any_definition(),
        policy in any_policy(),
        lateness in any_lateness(),
    ) {
        let next_run_at = first_due(&definition);
        let now = next_run_at + TimeDelta::seconds(lateness);
        let result = definition.materialize(next_run_at, now, policy);
        prop_assert!(result.is_ok(), "generated schedule did not materialize: {result:?}");
        let materialization = result.unwrap();
        prop_assert!(
            materialization.next_run_at > now,
            "cursor landed at {} which is not after {now}",
            materialization.next_run_at
        );
    }

    /// Occurrences are due times: never before the cursor, never in the future,
    /// and strictly increasing so they cannot collide on
    /// (schedule_id, scheduled_for).
    #[test]
    fn occurrences_are_due_and_strictly_increasing(
        definition in any_definition(),
        policy in any_policy(),
        lateness in any_lateness(),
    ) {
        let next_run_at = first_due(&definition);
        let now = next_run_at + TimeDelta::seconds(lateness);
        let result = definition.materialize(next_run_at, now, policy);
        prop_assert!(result.is_ok(), "generated schedule did not materialize: {result:?}");
        let materialization = result.unwrap();
        for occurrence in &materialization.occurrences {
            prop_assert!(*occurrence >= next_run_at, "{occurrence} is before the cursor");
            prop_assert!(*occurrence <= now, "{occurrence} is in the future");
        }
        for pair in materialization.occurrences.windows(2) {
            prop_assert!(pair[0] < pair[1], "occurrences {:?} are not increasing", pair);
        }
    }

    /// `catch_up` exists to bound the burst after downtime, so the limit has to
    /// hold no matter how far behind the schedule is.
    #[test]
    fn catch_up_never_exceeds_its_limit(
        definition in any_definition(),
        limit in 1_u16..64,
        lateness in any_lateness(),
    ) {
        let policy = MisfirePolicy::CatchUp { limit: NonZeroU16::new(limit).unwrap() };
        let next_run_at = first_due(&definition);
        let now = next_run_at + TimeDelta::seconds(lateness);
        let result = definition.materialize(next_run_at, now, policy);
        prop_assert!(result.is_ok(), "generated schedule did not materialize: {result:?}");
        let materialization = result.unwrap();
        prop_assert!(
            materialization.occurrences.len() <= usize::from(limit),
            "{} occurrences for a limit of {limit}",
            materialization.occurrences.len()
        );
    }

    /// `skipped` is what operators read to notice a silent gap, so everything
    /// that was due is either created or counted as dropped.
    ///
    /// The expected total is computed here by hand rather than read back from
    /// the implementation: for an interval schedule first due at `t` and now
    /// `t + elapsed`, the occurrences due are at `t`, `t + every`, ... so there
    /// are `elapsed / every + 1` of them. Asserting against the code's own
    /// arithmetic would only prove it agrees with itself.
    #[test]
    fn everything_due_is_either_created_or_counted_as_skipped(
        every_millis in 1_u64..3_600_000,
        policy in any_policy(),
        lateness_millis in 0_u64..(86_400_000 * 3),
    ) {
        let definition = ScheduleDefinition::interval(Duration::from_millis(every_millis)).unwrap();
        let next_run_at = epoch();
        let now = next_run_at + TimeDelta::milliseconds(i64::try_from(lateness_millis).unwrap());

        let expected_due = lateness_millis / every_millis + 1;

        let materialization = definition.materialize(next_run_at, now, policy).unwrap();
        let accounted = materialization.occurrences.len() as u64 + materialization.skipped;
        prop_assert_eq!(
            accounted,
            expected_due,
            "{} due, but {} created and {} skipped",
            expected_due,
            materialization.occurrences.len(),
            materialization.skipped
        );
    }

    /// A sub-millisecond interval is an error from every misfire policy, not a
    /// panic from some of them.
    ///
    /// `ScheduleDefinition::interval` accepts anything non-zero, and both
    /// `due_count` and `latest_due` divide by the interval in whole
    /// milliseconds. `due_count` guarded the zero; `latest_due` did not, and
    /// `materialize` reaches it first under `Latest` and `Skip` alike, so which
    /// policy you chose decided whether you got an error or a crash.
    #[test]
    fn a_sub_millisecond_interval_errors_under_every_policy(
        every_micros in 1_u64..1_000,
        policy in any_policy(),
        lateness in 0_i64..60,
    ) {
        let definition = ScheduleDefinition::interval(Duration::from_micros(every_micros)).unwrap();
        let next_run_at = epoch();
        let now = next_run_at + TimeDelta::seconds(lateness);
        // `materialize` only returns early when the cursor is still in the
        // future, and `lateness` starts at zero, so every case here reaches the
        // arithmetic.
        let result = definition.materialize(next_run_at, now, policy);
        prop_assert!(
            matches!(result, Err(ScheduleError::ZeroInterval)),
            "{policy:?} on a {every_micros}us interval gave {result:?}"
        );
    }

    /// Nothing is due yet, so nothing is created and the cursor stays put.
    #[test]
    fn a_schedule_that_is_not_due_yet_does_nothing(
        definition in any_definition(),
        policy in any_policy(),
        ahead in 1_i64..86_400,
    ) {
        let next_run_at = first_due(&definition);
        let now = next_run_at - TimeDelta::seconds(ahead);
        let materialization = definition.materialize(next_run_at, now, policy).unwrap();
        prop_assert!(materialization.occurrences.is_empty());
        prop_assert_eq!(materialization.next_run_at, next_run_at);
        prop_assert_eq!(materialization.skipped, 0);
    }
}
