# Local retention result - 2026-08-15

This is a development diagnostic on Apple Silicon with PostgreSQL 17 in Docker. It is not a publishable capacity result.

## Workload

- 20,000 no-op tasks
- one Rust worker
- concurrency 100
- terminal retention zero
- cleanup batches of 1,000 rows

## Result

| Measurement | Result |
| --- | ---: |
| Enqueue throughput | 9,005 tasks/s |
| Drain throughput | 2,968 tasks/s |
| Cleanup duration | 0.533 s |
| Rows deleted | 20,000 |
| Peak PostgreSQL container CPU | 153.5% |
| WAL growth | 40,391,711 bytes |
| Lock waits | 0 |
| Deadlocks | 0 |
| Task table and index allocation after cleanup | 16,277,504 bytes |

PostgreSQL can reuse the allocated pages after bounded deletion. The initial migration lowers the task table's autovacuum and analyze scale factor to two percent with a 1,000-row threshold.

## Decision

Keep the shared task table unpartitioned for 1.0. Bounded cleanup completed without lock pressure, and optional partitioning would add migration, routing, and operational complexity without evidence of a current correctness or latency problem. Reopen the decision if the managed PostgreSQL run shows sustained vacuum lag, claim-index locality loss, or retention transactions missing their budget.
