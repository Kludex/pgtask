# pgtask-otel

OpenTelemetry for `pgtask`: span names and attributes, queue and worker metrics, and trace context carried
inside task headers.

Because the context travels with the task, a worker span is a child of the request that enqueued it, even
though the two ran in different processes minutes apart. Without that, a queue is where your traces end.

You probably want [`pgtask`](https://crates.io/crates/pgtask), which re-exports this as `pgtask::otel`.

## Documentation

[kludex.github.io/pgtask](https://kludex.github.io/pgtask/)
