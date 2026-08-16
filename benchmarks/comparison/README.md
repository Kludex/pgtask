# Queue comparison harness

Runs the same workload through pgtask, Celery, and Dramatiq so their throughput can be compared on
one machine.

## Run it

Redis and PostgreSQL must be reachable, and the database must be empty or already migrated:

```console
redis-server --port 6399 --save '' --appendonly no --daemonize yes
export PGTASK_DATABASE_URL=postgresql://postgres@127.0.0.1:5432/bench
uv run python run.py
```

Results print as a table and are written to `results.json`.

## Configure it

Every knob is an environment variable:

| Variable | Default | Meaning |
| --- | --- | --- |
| `BENCH_TASKS` | 5000 | Tasks per repetition |
| `BENCH_WORKERS` | 4 | Worker processes, or prefork children |
| `BENCH_CONCURRENCY` | 25 | Concurrent handlers per pgtask worker |
| `BENCH_REPETITIONS` | 3 | Repetitions; the median is reported |
| `BENCH_REDIS_URL` | `redis://127.0.0.1:6399/0` | Broker and completion counter |
| `BENCH_DRAIN_TIMEOUT` | 300 | Seconds before a drain is called failed |

## How it measures

Enqueue runs with no workers, so it measures only the cost of accepting work. Drain then starts the
workers and times from the first completion to the last, which keeps worker startup out of the
number.

Handlers increment one Redis counter in every system, so completion is detected the same way for all
three. The queue is truncated and Redis flushed between repetitions.

Recorded results live in `../2026-08-16-queue-comparison.md`.
