# 0003: Embed the scheduler in workers

Status: accepted

## Decision

Every enabled worker may materialize schedules. Schedule rows are claimed with `SKIP LOCKED`, and occurrences have database-enforced uniqueness.

## Consequences

There is no singleton scheduler deployment or leader-election mechanism. Scheduling scales and fails over with workers. The materialization transaction must remain bounded.
