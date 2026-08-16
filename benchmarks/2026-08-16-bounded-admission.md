# Local bounded-admission result - 2026-08-16

This is a development diagnostic on Apple Silicon and local PostgreSQL. It is not a publishable capacity claim.

## Workload

- 1,000 no-op tasks
- batches of 100
- four workers
- concurrency 50 per worker
- new logical queue for each measured configuration
- debug Rust build

## Results

| Queue | Enqueue tasks/s | Drain tasks/s |
| --- | ---: | ---: |
| Unlimited | 5,022 | 7,466 |
| Capacity 1,000 | 4,646 | 2,782 |

The bounded queue admitted exactly 1,000 tasks and drained all 1,000 once. Admission uses an O(1) transactional counter
instead of recounting task rows. State transitions still serialize briefly on the bounded queue's counter, which is the
cost of a hard limit. Keep high-throughput queues unlimited unless database growth requires explicit backpressure.
