---
title: Scheduling without a leader
description: Why every worker can schedule, and how duplicates are prevented.
---

Scheduling runs inside every worker you enable it on. There is no leader, no lock service, and no election.

That sounds like it should produce duplicate work. It does not, and the reason is worth understanding, because it is the
same trick used for every other piece of background maintenance in the system.

## How a schedule becomes tasks

1. A worker claims due schedule definitions with `SKIP LOCKED`. Two workers claiming at the same moment take different
   rows rather than blocking on each other.
2. Rust computes the occurrences for the interval or cron expression.
3. PostgreSQL advances `next_run_at` and inserts the tasks **in one transaction**.

If a worker dies in the middle, the transaction rolls back. The schedule keeps its old `next_run_at` and the next worker
picks it up. There is no partially advanced schedule.

## The guarantee that makes it safe

A unique index on `(schedule_id, scheduled_for)` is what actually prevents duplicates:

```sql
CREATE UNIQUE INDEX tasks_schedule_occurrence_idx
    ON pgtask.tasks (schedule_id, scheduled_for)
    WHERE schedule_id IS NOT NULL;
```

Two workers may both decide that the 09:00 occurrence is due. Only one insert survives. The other conflicts and does
nothing.

This is the difference between *coordinating* and *making duplicates impossible*. A leader election is an attempt to
ensure only one worker tries. A unique key means it does not matter how many try.

:::note[Every worker also does maintenance]
The same `SKIP LOCKED` pattern divides expired-lease recovery, wait-timeout recovery, and retention deletion across
whatever replicas happen to be running. Losing a worker does not stop maintenance; it just leaves fewer hands.
:::

## Misfire policies

A schedule that was due while nothing was running has to decide what "catching up" means. That is a business question,
so you choose:

| Policy | Behaviour | Use it when |
| --- | --- | --- |
| `skip` | Ignore everything missed; run at the next due time | The work is only useful now - a health probe, a cache refresh |
| `latest` | Run once for the most recent missed occurrence | You want current state, not a backlog - a nightly report |
| `catch_up` | Run every missed occurrence, bounded by `catch_up_limit` | Each occurrence is a distinct unit of work - per-hour billing rollups |

`catch_up` is bounded on purpose. An unbounded catch-up after a long outage is how a scheduler turns a recovery into a
second outage.

## Time is UTC

Schedules use six-field UTC cron expressions. Named time zones are deliberately not supported yet.

The reason is that a local-time schedule is ambiguous twice a year. During a fall-back transition, 01:30 happens twice;
during spring-forward, it never happens. Every scheduler that supports named zones has to pick an answer, and the answer
is rarely what the user assumed. Rather than guess, `pgtask` requires you to be explicit in UTC until the behaviour is
specified and tested.

## Prompt changes

The scheduler listens on `pgtask_schedule` and `pgtask_wait` so a created, paused, or resumed schedule takes effect
immediately instead of at the next reconciliation.

As everywhere else, the notification is a latency optimisation. A periodic reconciliation covers a lost listener, and a
schedule created while every worker was down is still found when one starts.
