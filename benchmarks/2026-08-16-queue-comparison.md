# Local comparison against Celery and Dramatiq - 2026-08-16

This is a development diagnostic on one laptop. It is not a publishable capacity claim, and it is not
a statement about what any of these systems do on production hardware.

It exists to replace opinion with a measured spread. The engines differ in what they promise, so read
the numbers next to the guarantee each one is paying for.

## Method

Each system enqueues 20,000 trivial tasks one at a time, then drains them with four worker processes.
Handlers do nothing but increment one Redis counter, so completion is detected identically everywhere
and no system gets a cheaper finish line.

The two phases are measured separately.

- **Enqueue** runs with no workers, so it measures only the cost of accepting work.
- **Drain** starts the workers afterwards and times from the first completion to the last. Worker
  startup is excluded deliberately: Celery takes seconds to boot, and that is not a claim about its
  throughput.

Each measurement is the median of three repetitions. The queue is truncated and Redis flushed between
repetitions, so no run inherits a backlog.

The harness is in `benchmarks/comparison`.

## Environment

| Item | Value |
| --- | --- |
| CPU | Apple M3 Max, 16 cores |
| Memory | 64 GiB |
| PostgreSQL | 17.10 in Docker |
| Redis | 7.2.5, local, no persistence |
| Python | 3.14.3 |
| Celery | 5.6.3, Redis broker, results ignored |
| Dramatiq | 2.2.0, Redis broker |
| Tasks | 20,000 |
| Workers | 4 |

## Results

| System | Enqueue/s | Drain/s | Configuration |
| --- | ---: | ---: | --- |
| pgtask | 1,184 | 2,115 | 4 processes, concurrency 25 |
| Celery | 4,093 | 1,825 | prefork, `-c 4` |
| Dramatiq | 7,385 | 6,585 | `-p 4 -t 8` |

Repetitions agreed to within 5 percent for every system, so the ordering is not noise.

## Reading the numbers

**Dramatiq is faster at both ends, by a lot.** It writes to an in-memory broker with no durability
requirement. Nothing pgtask can do at the storage layer will close that gap, because the gap is the
storage layer.

**pgtask enqueue is the slowest number here, at roughly a third of Celery.** Every enqueue is a
committed PostgreSQL write and a round trip. That is exactly what buys transactional enqueue: the
task is in the same transaction as your data. The cost is visible and it is the point.

**pgtask drains faster than Celery.** This is the result worth noticing, because it is the one nobody
would predict from "PostgreSQL is slower than Redis". Claiming a batch under a lease costs fewer round
trips per task than Celery's prefork protocol, so once the work exists, a durable queue is not the
bottleneck people assume.

## What this does not measure

- **One machine.** Producer, broker, database, and workers all contend for the same 16 cores. A real
  deployment separates them and the ratios move.
- **Trivial handlers.** Every task increments a counter. Real handlers do work, and the moment a
  handler takes 50 ms, all three systems are bound by the handler rather than the queue.
- **No failures.** No retries, no lease expiry, no worker death.
- **Default-ish configurations.** Each system runs in a reasonable shape for its own design rather
  than an artificially matched one. The concurrency models are not comparable: prefork processes,
  threads, and async tasks are three different things.
- **Nothing about durability.** Redis here has persistence disabled. Celery and Dramatiq lose queued
  work if it dies; pgtask cannot, because the work is committed. That difference does not appear in a
  throughput table and is usually the reason to choose one of them.

## Finding: the Python client has no batch enqueue

The SQL protocol has `pgtask.enqueue_many`, which inserts a batch in one transaction, and the Rust
benchmark reaches 14,289 tasks per second with it. The Python client exposes only `enqueue`, one task
per call, which is why the enqueue column above is 1,184.

A producer loop in Python therefore pays a round trip per task and cannot reach the throughput the
protocol already supports. Exposing batch enqueue in the Python client is the single largest
available improvement to this number.
