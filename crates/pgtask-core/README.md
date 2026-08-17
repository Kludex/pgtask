# pgtask-core

The types every layer of `pgtask` agrees on: tasks, queues, schedules, retry policies, and the states a
task can be in.

There is no database access and no runtime here. The crate exists so the storage layer, the worker, and
the telemetry layer share one definition of a task instead of three that drift apart.

You probably want [`pgtask`](https://crates.io/crates/pgtask), which re-exports this as `pgtask::core`.

## Documentation

[kludex.github.io/pgtask](https://kludex.github.io/pgtask/)
