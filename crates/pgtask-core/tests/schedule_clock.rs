use std::{num::NonZeroU16, time::Duration};

use chrono::{TimeZone, Utc};
use pgtask_core::{MisfirePolicy, ScheduleDefinition};

#[test]
fn schedule_state_remains_ordered_when_database_time_moves_backward_and_forward() {
    let schedule = ScheduleDefinition::interval(Duration::from_mins(1)).unwrap();
    let first_due = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();

    let before_due = schedule
        .materialize(
            first_due,
            Utc.with_ymd_and_hms(2026, 8, 15, 11, 55, 0).unwrap(),
            MisfirePolicy::Latest,
        )
        .unwrap();
    assert!(before_due.occurrences.is_empty());
    assert_eq!(before_due.next_run_at, first_due);

    let forward = schedule
        .materialize(
            before_due.next_run_at,
            Utc.with_ymd_and_hms(2026, 8, 15, 12, 5, 0).unwrap(),
            MisfirePolicy::Latest,
        )
        .unwrap();
    assert_eq!(
        forward.occurrences,
        [Utc.with_ymd_and_hms(2026, 8, 15, 12, 5, 0).unwrap()]
    );

    let backward = schedule
        .materialize(
            forward.next_run_at,
            Utc.with_ymd_and_hms(2026, 8, 15, 12, 3, 0).unwrap(),
            MisfirePolicy::Latest,
        )
        .unwrap();
    assert!(backward.occurrences.is_empty());
    assert_eq!(backward.next_run_at, forward.next_run_at);

    let recovered = schedule
        .materialize(
            backward.next_run_at,
            Utc.with_ymd_and_hms(2026, 8, 15, 12, 10, 0).unwrap(),
            MisfirePolicy::Latest,
        )
        .unwrap();
    assert_eq!(
        recovered.occurrences,
        [Utc.with_ymd_and_hms(2026, 8, 15, 12, 10, 0).unwrap()]
    );
    assert_eq!(
        recovered.next_run_at,
        Utc.with_ymd_and_hms(2026, 8, 15, 12, 11, 0).unwrap()
    );
}

#[test]
fn every_misfire_policy_reports_the_occurrences_it_discarded() {
    let schedule = ScheduleDefinition::interval(Duration::from_mins(1)).unwrap();
    let first_due = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
    // Ten minutes late, so eleven occurrences are due: 12:00 through 12:10.
    let now = Utc.with_ymd_and_hms(2026, 8, 15, 12, 10, 0).unwrap();

    let skip = schedule.materialize(first_due, now, MisfirePolicy::Skip).unwrap();
    assert_eq!(skip.occurrences.len(), 1);
    assert_eq!(skip.skipped, 10);

    let latest = schedule.materialize(first_due, now, MisfirePolicy::Latest).unwrap();
    assert_eq!(latest.occurrences.len(), 1);
    assert_eq!(latest.skipped, 10);

    let bounded = schedule
        .materialize(
            first_due,
            now,
            MisfirePolicy::CatchUp {
                limit: NonZeroU16::new(4).unwrap(),
            },
        )
        .unwrap();
    assert_eq!(bounded.occurrences.len(), 4);
    assert_eq!(bounded.skipped, 7);

    let complete = schedule
        .materialize(
            first_due,
            now,
            MisfirePolicy::CatchUp {
                limit: NonZeroU16::new(64).unwrap(),
            },
        )
        .unwrap();
    assert_eq!(complete.occurrences.len(), 11);
    assert_eq!(complete.skipped, 0, "a policy that keeps up discards nothing");

    let punctual = schedule
        .materialize(now, now - chrono::TimeDelta::seconds(1), MisfirePolicy::Skip)
        .unwrap();
    assert!(punctual.occurrences.is_empty());
    assert_eq!(punctual.skipped, 0, "a schedule that is not due discards nothing");
}

#[test]
fn a_cron_schedule_reports_discarded_occurrences() {
    let schedule = ScheduleDefinition::cron("0 0 * * * *").unwrap();
    let first_due = Utc.with_ymd_and_hms(2026, 8, 15, 0, 0, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 8, 15, 5, 30, 0).unwrap();

    let latest = schedule.materialize(first_due, now, MisfirePolicy::Latest).unwrap();
    assert_eq!(latest.occurrences.len(), 1);
    assert_eq!(latest.skipped, 5, "00:00 through 05:00 are due, one is kept");
}
