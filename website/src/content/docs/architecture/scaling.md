---
title: Scaling and deployment
description: What to run, how notifications shard, and what runs out first.
---

A deployment is PostgreSQL plus one or more worker processes. The CLI and web interface are optional.

Each worker serves one logical queue and advertises the `(task_name, handler_version)` pairs it can run. You scale by
adding workers or raising per-worker concurrency, and you isolate workloads by giving them separate queues.

## Two connection paths

This is the deployment detail that surprises people, so it comes first.

| Path | Used for | Requirement |
| --- | --- | --- |
| Pool | Short state transitions - claim, renew, complete | Any pooler, including transaction pooling |
| Listener | `LISTEN` for wake-ups | A **session-capable** endpoint |

`LISTEN` registers interest on a specific backend session. A transaction-pooling proxy hands your next statement to a
different backend, so the registration is lost and notifications never arrive.

The symptom is not an error. It is a system that works, slowly, because every task waits for the reconciliation poll
instead of the notification.

So: point the listener at a direct PostgreSQL endpoint or a PgBouncer **session** pool. One listener connection
multiplexes every channel a process needs - queue, scheduler, wait, and result.

## Sharded notifications

Ready and result notifications each use 64 deterministic channels.

A single global channel would wake every worker for every task, and each one would query, find nothing for itself, and
go back to sleep. That cost grows with the product of workers and task rate.

With shards, a queue runtime receives only its queue's payload, and a result waiter only its task's payload. Wake-ups
stay proportional to relevant work.

They remain hints. Every wake-up is followed by a state read, and bounded reconciliation polling covers disconnects and
missed notifications.

## Concurrency and overload

Two independent control loops manage load, and mixing them up leads to confusing incidents.

**In-process admission control** watches event-loop lag, lease-renewal age, and database failures. When it detects
sustained overload it reduces the effective concurrency limit - which stops the worker claiming *new* work. It never
cancels a running handler and never exceeds your configured concurrency. Recovery is additive, so a worker climbs back
gradually rather than slamming the database again.

**Kubernetes replica scaling** adds or removes whole workers based on queue demand, using ready-task count or
oldest-ready age exposed by `pgtask.queue_demand`.

They are deliberately separate: one manages a single process against the database it can see, the other manages fleet
size against a queue's backlog. Neither is part of task correctness. Turning both off changes throughput, not
guarantees.

:::caution[Do not scale schedulers to zero]
If your autoscaler can remove every replica, make sure it cannot remove every *scheduler-enabled* worker. Schedules only
advance while something is running to advance them.
:::

## Health endpoints

Workers expose pod-local `/livez` and `/readyz`. They answer different questions, and conflating them causes restart
loops.

- `/livez` depends only on supervisor progress. It answers "is this process wedged?" It stays healthy during a database
  outage, because restarting a worker does not fix PostgreSQL being down.
- `/readyz` covers claim admission, database connectivity, listener health, and lease-renewal health. It answers "should
  this worker receive work right now?"

## Draining

On shutdown, a worker stops claiming and lets running handlers finish within the grace period. Handlers that do not
finish are aborted, and their tasks return through lease expiry - which is the ordinary recovery path, not a special
case.

A drain-only mode lets you keep a worker finishing in-flight work without accepting anything new.

## What runs out first

In rough order:

1. **The listener path**, if you route it through a transaction pooler. Everything works but latency is poll-bound.
2. **Connections.** Workers × pool size adds up faster than expected against a default `max_connections` of 100.
3. **PostgreSQL write throughput.** Every transition is a write; retention deletes are writes too.
4. **Table bloat on `tasks`,** if retention is too generous. The claim indexes are partial, which helps, and the table
   carries tuned autovacuum settings for its churn.

Capture PostgreSQL CPU, WAL rate, lock waits, connection count, cache hit ratio, and storage growth before you tune
anything else. The queue is rarely the first thing to break; the database under it is.
