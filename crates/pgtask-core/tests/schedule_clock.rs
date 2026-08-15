use std::time::Duration;

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
