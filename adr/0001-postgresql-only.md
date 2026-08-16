# 0001: Require only PostgreSQL

Status: accepted

## Decision

PostgreSQL is the only required external service. Extensions may optimize optional operations but never provide correctness.

## Consequences

Applications can enqueue work in the same transaction as their domain writes. The engine must control polling, retention, connection usage, and table churn carefully because queue load shares database capacity with application traffic.
