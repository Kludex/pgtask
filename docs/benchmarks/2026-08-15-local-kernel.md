# Local queue-kernel baseline

Date: 2026-08-15

This is a development baseline, not a capacity claim. The benchmark process and PostgreSQL ran on the same machine.

## Environment

| Item | Value |
| --- | --- |
| CPU | Apple M3 Max |
| Memory | 64 GiB |
| Architecture | arm64 |
| PostgreSQL | 14.21, Homebrew |
| Tasks | 1,000 |
| Enqueue batch | 100 |
| Build | Rust release profile |

## Result

| Operation | Duration | Throughput |
| --- | ---: | ---: |
| Batch enqueue | 0.070 seconds | 14,289 tasks/second |
| Claim and individual completion | 0.281 seconds | 3,555 tasks/second |

The completion path deliberately uses one database round trip per task. This establishes the initial cost before completion batching, concurrent handlers, notification wake-ups, and connection-pool tuning.

## Command

```console
PGTASK_DATABASE_URL=postgresql://marcelotryle@localhost/pgtask_test \
PGTASK_BENCH_TASKS=1000 \
cargo run --release -p pgtask-bench --bin pgtask-bench
```
