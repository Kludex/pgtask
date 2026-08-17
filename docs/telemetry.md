# OpenTelemetry

Configure W3C trace-context and baggage propagation before you enqueue tasks or start workers:

```rust
fn main() {
    pgtask::otel::configure_propagation();

    // Install your OpenTelemetry tracer and meter providers before starting pgtask.
}
```

`pgtask` uses the global OpenTelemetry providers. It does not select an exporter or telemetry backend. This keeps the engine compatible with OTLP collectors, managed observability services, and application-specific sampling.

Set `service.name`, `service.version`, `deployment.environment`, and a unique `service.instance.id` on the OpenTelemetry resource before you start a worker. Configure your tracer and meter providers before constructing `Worker`, and shut them down when the process exits.

## Traces

The engine creates these spans:

| Span | Kind | Purpose |
| --- | --- | --- |
| `pgtask.enqueue` | Producer | Enqueue one task and inject its trace context |
| `pgtask.enqueue_many` | Producer | Enqueue a batch with one shared producer context |
| `pgtask.claim` | Internal | Claim a bounded group of due tasks |
| `pgtask.execute` | Consumer | Execute one attempt with the producer context as its parent |

Trace context is stored in the task `headers` JSON object. Existing application headers are preserved. The default propagators write `traceparent`, `tracestate`, and `baggage` when those values exist.

## Metrics

| Instrument | Type | Unit |
| --- | --- | --- |
| `pgtask.tasks` | Counter | Tasks |
| `pgtask.lease.renewals` | Counter | Renewals |
| `pgtask.queue.latency` | Histogram | Seconds |
| `pgtask.execution.duration` | Histogram | Seconds |
| `pgtask.schedule.occurrences` | Counter | Tasks |
| `pgtask.schedule.skipped_occurrences` | Counter | Occurrences |
| `pgtask.schedule.lag` | Histogram | Seconds |
| `pgtask.schedule.materialization.duration` | Histogram | Seconds |
| `pgtask.queue.ready.tasks` | Gauge | Tasks |
| `pgtask.queue.unroutable.tasks` | Gauge | Tasks |
| `pgtask.worker.concurrency.configured` | Gauge | Handlers |
| `pgtask.worker.concurrency.effective` | Gauge | Handlers |
| `pgtask.worker.handlers.active` | Gauge | Handlers |
| `pgtask.worker.slots.available` | Gauge | Handlers |
| `pgtask.worker.event_loop.lag` | Gauge | Seconds |
| `pgtask.worker.lease.renewal.age` | Gauge | Seconds |
| `pgtask.worker.admission.limit` | Gauge | Handlers |

Metric attributes include queue name, task name, transition state, execution outcome, and lease-renewal result. Task identifiers, payloads, results, errors, and idempotency keys are excluded to keep cardinality bounded.

Worker-capacity gauges only use `pgtask.queue.name`. Configure a stable `service.instance.id` resource attribute for each process. Backends must retain that resource boundary when aggregating several workers for one queue.

`pgtask.queue.ready.tasks` counts due tasks supported by the process's registered task names and handler versions. Use it for queue autoscaling. `pgtask.queue.unroutable.tasks` counts due tasks with no live, non-draining worker that advertises the required capability. Alert when it remains nonzero. Take the maximum across worker instances for both gauges; every replica observes the same durable queue.

Configured concurrency is the hard process limit. Effective concurrency is the current in-memory admission limit. Available slots are `max(effective - active, 0)`. Lowering the effective limit stops new claims and never cancels an active handler. Event-loop lag measures delay beyond the one-second runtime sampling deadline. Lease-renewal age is the oldest active lease age at the renewal sampling point, or zero when the worker has no active lease.

Do not sum concurrency, handler, or slot gauges across time. Use the latest value per `service.instance.id`, then sum instances when you need queue capacity. Use the maximum event-loop lag and lease-renewal age across instances for alerting.

The admission-limit gauge uses bounded `pgtask.admission.decision` values of `proposed` or `applied`. Reasons are `database_unavailable`, `lease_renewal_late`, `event_loop_lag`, `recovery`, or `manual`. The default detector is observe-only. Sustained runtime lag, an unsafe lease-renewal age, or a database failure changes `WorkerControl.proposed_concurrency` but does not change the effective limit. This makes thresholds measurable before enforcement is enabled.

Set `WorkerConfig.overload_protection.enforce` only after validating thresholds for your workload. `sustained_samples` filters transient event-loop lag. `recovery_samples` requires a stable healthy window before recovery starts. Enforcement halves the effective limit down to `minimum_concurrency` while overload persists. Healthy samples then add one slot until the configured limit is restored. Active handlers are never cancelled, and lease renewal remains independent of claim admission.

PostgreSQL CPU, memory, connections, locks, WAL, cache, and storage are infrastructure metrics. Collect them with the OpenTelemetry Collector Contrib PostgreSQL receiver. Kubernetes node, pod, and workload metrics belong in a Collector Deployment and DaemonSet with the Kubernetes attributes processor. Keep those collectors outside the pgtask worker so worker failure cannot hide database or cluster health.

## Worker health

Set `WorkerConfig.health_address` to serve `/livez` and `/readyz` from the dedicated Rust supervisor thread. `/livez` only proves that the supervisor can respond. `/readyz` requires open claim admission, database connectivity, a healthy notification listener, and safe lease renewal. Dependency failure removes the worker from readiness without asking Kubernetes to restart a process whose supervisor is healthy.
