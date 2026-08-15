use std::{num::NonZeroU16, str::FromStr, time::Duration};

use chrono::{DateTime, TimeDelta, Utc};
use cron::Schedule as CronSchedule;
use thiserror::Error;

use crate::{EnqueueRequest, ScheduleId, ScheduleName};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleDefinition {
    Interval { every: Duration },
    Cron { expression: String },
}

impl ScheduleDefinition {
    pub fn interval(every: Duration) -> Result<Self, ScheduleError> {
        if every.is_zero() {
            return Err(ScheduleError::ZeroInterval);
        }
        TimeDelta::from_std(every).map_err(|_| ScheduleError::IntervalOutOfRange)?;
        Ok(Self::Interval { every })
    }

    pub fn cron(expression: impl Into<String>) -> Result<Self, ScheduleError> {
        let expression = expression.into();
        parse_cron(&expression)?;
        Ok(Self::Cron { expression })
    }

    pub fn next_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>, ScheduleError> {
        match self {
            Self::Interval { every } => after
                .checked_add_signed(TimeDelta::from_std(*every).map_err(|_| ScheduleError::IntervalOutOfRange)?)
                .ok_or(ScheduleError::DateOutOfRange),
            Self::Cron { expression } => parse_cron(expression)?
                .after(&after)
                .next()
                .ok_or(ScheduleError::NoFutureOccurrence),
        }
    }

    fn latest_due(&self, first_due: DateTime<Utc>, now: DateTime<Utc>) -> Result<DateTime<Utc>, ScheduleError> {
        match self {
            Self::Interval { every } => {
                let every_milliseconds =
                    i64::try_from(every.as_millis()).map_err(|_| ScheduleError::IntervalOutOfRange)?;
                let elapsed_milliseconds = (now - first_due).num_milliseconds();
                let intervals = elapsed_milliseconds / every_milliseconds;
                first_due
                    .checked_add_signed(TimeDelta::milliseconds(intervals * every_milliseconds))
                    .ok_or(ScheduleError::DateOutOfRange)
            }
            Self::Cron { expression } => {
                let schedule = parse_cron(expression)?;
                let mut low = first_due.timestamp().saturating_sub(1);
                let mut high = now.timestamp().saturating_add(1);
                while low + 1 < high {
                    let middle = low + (high - low) / 2;
                    let middle = DateTime::from_timestamp(middle, 0).ok_or(ScheduleError::DateOutOfRange)?;
                    let next = schedule
                        .after(&middle)
                        .next()
                        .ok_or(ScheduleError::NoFutureOccurrence)?;
                    if next <= now {
                        low = middle.timestamp();
                    } else {
                        high = middle.timestamp();
                    }
                }
                let low = DateTime::from_timestamp(low, 0).ok_or(ScheduleError::DateOutOfRange)?;
                schedule
                    .after(&low)
                    .next()
                    .filter(|occurrence| *occurrence <= now)
                    .ok_or(ScheduleError::NoFutureOccurrence)
            }
        }
    }

    pub fn materialize(
        &self,
        next_run_at: DateTime<Utc>,
        now: DateTime<Utc>,
        policy: MisfirePolicy,
    ) -> Result<Materialization, ScheduleError> {
        if next_run_at > now {
            return Ok(Materialization {
                occurrences: Vec::new(),
                next_run_at,
            });
        }

        let occurrences = match policy {
            MisfirePolicy::Skip => vec![next_run_at],
            MisfirePolicy::Latest => vec![self.latest_due(next_run_at, now)?],
            MisfirePolicy::CatchUp { limit } => {
                let mut occurrences = Vec::with_capacity(usize::from(limit.get()));
                let mut occurrence = next_run_at;
                while occurrence <= now && occurrences.len() < usize::from(limit.get()) {
                    occurrences.push(occurrence);
                    occurrence = self.next_after(occurrence)?;
                }
                occurrences
            }
        };
        Ok(Materialization {
            occurrences,
            next_run_at: self.next_after(now)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MisfirePolicy {
    Skip,
    #[default]
    Latest,
    CatchUp {
        limit: NonZeroU16,
    },
}

#[derive(Clone, Debug)]
pub struct ScheduleConfig {
    pub id: ScheduleId,
    pub name: ScheduleName,
    pub definition: ScheduleDefinition,
    pub misfire_policy: MisfirePolicy,
    pub task: EnqueueRequest,
    pub start_at: Option<DateTime<Utc>>,
}

impl ScheduleConfig {
    pub fn new(name: ScheduleName, definition: ScheduleDefinition, task: EnqueueRequest) -> Self {
        Self {
            id: ScheduleId::new(),
            name,
            definition,
            misfire_policy: MisfirePolicy::default(),
            task,
            start_at: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Schedule {
    pub config: ScheduleConfig,
    pub next_run_at: DateTime<Utc>,
    pub paused_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Materialization {
    pub occurrences: Vec<DateTime<Utc>>,
    pub next_run_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("interval must be greater than zero")]
    ZeroInterval,
    #[error("interval exceeds the supported date range")]
    IntervalOutOfRange,
    #[error("cron expression must contain exactly six fields: second minute hour day-of-month month day-of-week")]
    InvalidCronFieldCount,
    #[error("invalid cron expression: {0}")]
    InvalidCron(String),
    #[error("schedule has no future occurrence")]
    NoFutureOccurrence,
    #[error("schedule date exceeds the supported range")]
    DateOutOfRange,
}

fn parse_cron(expression: &str) -> Result<CronSchedule, ScheduleError> {
    if expression.split_whitespace().count() != 6 {
        return Err(ScheduleError::InvalidCronFieldCount);
    }
    CronSchedule::from_str(&format!("{expression} *")).map_err(|error| ScheduleError::InvalidCron(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU16, time::Duration};

    use super::{MisfirePolicy, ScheduleDefinition, ScheduleError};
    use chrono::{TimeZone, Utc};

    #[test]
    fn validates_interval_and_six_field_cron_definitions() {
        assert!(matches!(
            ScheduleDefinition::interval(Duration::ZERO),
            Err(ScheduleError::ZeroInterval)
        ));
        assert!(matches!(
            ScheduleDefinition::cron("0 * * * *"),
            Err(ScheduleError::InvalidCronFieldCount)
        ));
        assert!(matches!(
            ScheduleDefinition::cron("invalid * * * * *"),
            Err(ScheduleError::InvalidCron(_))
        ));
        assert!(matches!(
            ScheduleDefinition::cron("TZ=Europe/Madrid 0 */5 * * * *"),
            Err(ScheduleError::InvalidCronFieldCount)
        ));
        assert!(ScheduleDefinition::cron("0 */5 * * * *").is_ok());
    }

    #[test]
    fn interval_misfire_policies_are_bounded() {
        let definition = ScheduleDefinition::interval(Duration::from_secs(10)).unwrap();
        let first = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let now = first + chrono::TimeDelta::seconds(35);

        let skipped = definition.materialize(first, now, MisfirePolicy::Skip).unwrap();
        assert_eq!(skipped.occurrences, vec![first]);
        assert_eq!(skipped.next_run_at, first + chrono::TimeDelta::seconds(45));

        let latest = definition.materialize(first, now, MisfirePolicy::Latest).unwrap();
        assert_eq!(latest.occurrences, vec![first + chrono::TimeDelta::seconds(30)]);

        let caught_up = definition
            .materialize(
                first,
                now,
                MisfirePolicy::CatchUp {
                    limit: NonZeroU16::new(2).unwrap(),
                },
            )
            .unwrap();
        assert_eq!(
            caught_up.occurrences,
            vec![first, first + chrono::TimeDelta::seconds(10)]
        );
        assert_eq!(caught_up.next_run_at, first + chrono::TimeDelta::seconds(45));
    }

    #[test]
    fn cron_latest_finds_the_last_due_occurrence_without_scanning_backlog() {
        let definition = ScheduleDefinition::cron("0 */5 * * * *").unwrap();
        let first = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 1, 2, 12, 3, 0).unwrap();
        let materialized = definition.materialize(first, now, MisfirePolicy::Latest).unwrap();
        assert_eq!(
            materialized.occurrences,
            vec![Utc.with_ymd_and_hms(2026, 1, 2, 12, 0, 0).unwrap()]
        );
        assert_eq!(
            materialized.next_run_at,
            Utc.with_ymd_and_hms(2026, 1, 2, 12, 5, 0).unwrap()
        );
    }

    #[test]
    fn future_schedule_is_not_materialized() {
        let definition = ScheduleDefinition::interval(Duration::from_secs(10)).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let future = now + chrono::TimeDelta::seconds(10);
        let materialized = definition.materialize(future, now, MisfirePolicy::Latest).unwrap();
        assert!(materialized.occurrences.is_empty());
        assert_eq!(materialized.next_run_at, future);
    }
}
