# 0004: Store logical queues in one task table

Status: accepted

## Decision

Queues are values in one shared task table. The initial schema does not create tables per queue and is not partitioned.

## Consequences

Migrations, retention, cross-queue inspection, and the UI remain simple. Partial indexes isolate the claim path from terminal history. Partitioning remains an evidence-driven optimization.
