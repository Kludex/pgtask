# pgtask-worker

The runtime that claims tasks, runs your handlers, and keeps their leases alive.

It also brings the parts you would otherwise write yourself: retries with backoff, durable execution with
checkpoints, a scheduler that needs no leader, retention, graceful shutdown, and a health endpoint.

You probably want [`pgtask`](https://crates.io/crates/pgtask), which re-exports this as `pgtask::worker`
and shows a complete worker in a few lines.

## Documentation

[kludex.github.io/pgtask](https://kludex.github.io/pgtask/)
