use std::{sync::OnceLock, time::Duration};

use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Gauge, Histogram},
};

struct KernelMetrics {
    tasks: Counter<u64>,
    lease_renewals: Counter<u64>,
    lease_recovery_failures: Counter<u64>,
    queue_latency: Histogram<f64>,
    execution_duration: Histogram<f64>,
    schedule_occurrences: Counter<u64>,
    schedule_skipped_occurrences: Counter<u64>,
    schedule_lag: Histogram<f64>,
    schedule_materialization_duration: Histogram<f64>,
    queue_ready_tasks: Gauge<u64>,
    workers_live: Gauge<u64>,
    worker_heartbeats: Counter<u64>,
    queue_unroutable_tasks: Gauge<u64>,
    worker_configured_concurrency: Gauge<u64>,
    worker_effective_concurrency: Gauge<u64>,
    worker_active_handlers: Gauge<u64>,
    worker_available_slots: Gauge<u64>,
    worker_event_loop_lag: Gauge<f64>,
    worker_lease_renewal_age: Gauge<f64>,
    worker_admission_limit: Gauge<u64>,
}

fn metrics() -> &'static KernelMetrics {
    static METRICS: OnceLock<KernelMetrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = global::meter("pgtask");
        KernelMetrics {
            tasks: meter
                .u64_counter("pgtask.tasks")
                .with_description("Task state transitions")
                .build(),
            lease_renewals: meter
                .u64_counter("pgtask.lease.renewals")
                .with_description("Lease renewal outcomes")
                .build(),
            lease_recovery_failures: meter
                .u64_counter("pgtask.lease.recovery.failures")
                .with_description("Failed expired-lease recovery batches")
                .build(),
            queue_latency: meter
                .f64_histogram("pgtask.queue.latency")
                .with_description("Time from task creation until execution starts")
                .with_unit("s")
                .build(),
            execution_duration: meter
                .f64_histogram("pgtask.execution.duration")
                .with_description("Task handler execution duration")
                .with_unit("s")
                .build(),
            schedule_occurrences: meter
                .u64_counter("pgtask.schedule.occurrences")
                .with_description("Schedule occurrences materialized as tasks")
                .build(),
            schedule_skipped_occurrences: meter
                .u64_counter("pgtask.schedule.skipped_occurrences")
                .with_description("Due schedule occurrences discarded by the misfire policy")
                .build(),
            schedule_lag: meter
                .f64_histogram("pgtask.schedule.lag")
                .with_description("Time from a logical schedule occurrence until materialization")
                .with_unit("s")
                .build(),
            schedule_materialization_duration: meter
                .f64_histogram("pgtask.schedule.materialization.duration")
                .with_description("Schedule materialization transaction duration")
                .with_unit("s")
                .build(),
            workers_live: meter
                .u64_gauge("pgtask.workers.live")
                .with_description("Workers the database still considers live for this queue")
                .with_unit("{worker}")
                .build(),
            worker_heartbeats: meter
                .u64_counter("pgtask.worker.heartbeats")
                .with_description("Worker heartbeat attempts by outcome")
                .build(),
            queue_ready_tasks: meter
                .u64_gauge("pgtask.queue.ready.tasks")
                .with_description("Due tasks routable to any live, non-draining worker on this queue")
                .with_unit("{task}")
                .build(),
            queue_unroutable_tasks: meter
                .u64_gauge("pgtask.queue.unroutable.tasks")
                .with_description("Due tasks with no live capable worker on this queue")
                .with_unit("{task}")
                .build(),
            worker_configured_concurrency: meter
                .u64_gauge("pgtask.worker.concurrency.configured")
                .with_description("Configured maximum active task handlers")
                .with_unit("{handler}")
                .build(),
            worker_effective_concurrency: meter
                .u64_gauge("pgtask.worker.concurrency.effective")
                .with_description("Current admission limit for active task handlers")
                .with_unit("{handler}")
                .build(),
            worker_active_handlers: meter
                .u64_gauge("pgtask.worker.handlers.active")
                .with_description("Active task handlers")
                .with_unit("{handler}")
                .build(),
            worker_available_slots: meter
                .u64_gauge("pgtask.worker.slots.available")
                .with_description("Task handler slots available under the effective admission limit")
                .with_unit("{handler}")
                .build(),
            worker_event_loop_lag: meter
                .f64_gauge("pgtask.worker.event_loop.lag")
                .with_description("Delay beyond the expected worker runtime health-sampling deadline")
                .with_unit("s")
                .build(),
            worker_lease_renewal_age: meter
                .f64_gauge("pgtask.worker.lease.renewal.age")
                .with_description("Age of the oldest active task lease renewal")
                .with_unit("s")
                .build(),
            worker_admission_limit: meter
                .u64_gauge("pgtask.worker.admission.limit")
                .with_description("Proposed or applied worker admission limit changes")
                .with_unit("{handler}")
                .build(),
        }
    })
}

fn record_task_transition(state: &'static str, queue_name: &str, task_name: Option<&str>, count: u64) {
    let mut attributes = vec![
        KeyValue::new("pgtask.task.state", state),
        KeyValue::new("pgtask.queue.name", queue_name.to_owned()),
    ];
    if let Some(task_name) = task_name {
        attributes.push(KeyValue::new("pgtask.task.name", task_name.to_owned()));
    }
    metrics().tasks.add(count, &attributes);
}

pub fn record_enqueued(queue_name: &str, task_name: &str, count: u64) {
    record_task_transition("enqueued", queue_name, Some(task_name), count);
}

pub fn record_claimed(queue_name: &str, task_name: &str) {
    record_task_transition("claimed", queue_name, Some(task_name), 1);
}

pub fn record_cancelled(queue_name: &str, task_name: &str) {
    record_task_transition("cancelled", queue_name, Some(task_name), 1);
}

pub fn record_succeeded(queue_name: &str, task_name: &str) {
    record_task_transition("succeeded", queue_name, Some(task_name), 1);
}

pub fn record_failed(queue_name: &str, task_name: &str) {
    record_task_transition("failed", queue_name, Some(task_name), 1);
}

pub fn record_retried(queue_name: &str, task_name: &str) {
    record_task_transition("retried", queue_name, Some(task_name), 1);
}

pub fn record_recovered(queue_name: &str, count: u64) {
    record_task_transition("recovered", queue_name, None, count);
}

pub fn record_recovery_failure(queue_name: &str) {
    metrics()
        .lease_recovery_failures
        .add(1, &[KeyValue::new("pgtask.queue.name", queue_name.to_owned())]);
}

pub fn record_lease_lost(queue_name: &str, task_name: &str) {
    record_task_transition("lease_lost", queue_name, Some(task_name), 1);
}

pub fn record_renewed(queue_name: &str, task_name: &str, renewed: bool) {
    metrics().lease_renewals.add(
        1,
        &[
            KeyValue::new("pgtask.queue.name", queue_name.to_owned()),
            KeyValue::new("pgtask.task.name", task_name.to_owned()),
            KeyValue::new("pgtask.lease.renewed", renewed),
        ],
    );
}

pub fn record_queue_latency(queue_name: &str, task_name: &str, duration: Duration) {
    metrics().queue_latency.record(
        duration.as_secs_f64(),
        &[
            KeyValue::new("pgtask.queue.name", queue_name.to_owned()),
            KeyValue::new("pgtask.task.name", task_name.to_owned()),
        ],
    );
}

pub fn record_queue_demand(queue_name: &str, ready_tasks: u64, unroutable_tasks: u64) {
    let attributes = [KeyValue::new("pgtask.queue.name", queue_name.to_owned())];
    metrics().queue_ready_tasks.record(ready_tasks, &attributes);
    metrics().queue_unroutable_tasks.record(unroutable_tasks, &attributes);
}

/// `outcome` is `ok`, `missing` when the registration is gone, or `error` when the call failed.
pub fn record_heartbeat(queue_name: &str, outcome: &'static str) {
    metrics().worker_heartbeats.add(
        1,
        &[
            KeyValue::new("pgtask.queue.name", queue_name.to_owned()),
            KeyValue::new("pgtask.heartbeat.outcome", outcome),
        ],
    );
}

pub fn record_live_workers(queue_name: &str, live: u64) {
    metrics()
        .workers_live
        .record(live, &[KeyValue::new("pgtask.queue.name", queue_name.to_owned())]);
}

pub fn record_execution(queue_name: &str, task_name: &str, outcome: &'static str, duration: Duration) {
    metrics().execution_duration.record(
        duration.as_secs_f64(),
        &[
            KeyValue::new("pgtask.queue.name", queue_name.to_owned()),
            KeyValue::new("pgtask.task.name", task_name.to_owned()),
            KeyValue::new("pgtask.execution.outcome", outcome),
        ],
    );
}

pub fn record_schedule_occurrences(
    queue_name: &str,
    task_name: &str,
    kind: &'static str,
    count: u64,
    skipped: u64,
    lag: Duration,
) {
    let attributes = [
        KeyValue::new("pgtask.queue.name", queue_name.to_owned()),
        KeyValue::new("pgtask.task.name", task_name.to_owned()),
        KeyValue::new("pgtask.schedule.kind", kind),
    ];
    metrics().schedule_occurrences.add(count, &attributes);
    metrics().schedule_skipped_occurrences.add(skipped, &attributes);
    metrics().schedule_lag.record(lag.as_secs_f64(), &attributes);
}

pub fn record_schedule_materialization(duration: Duration) {
    metrics()
        .schedule_materialization_duration
        .record(duration.as_secs_f64(), &[]);
}

pub fn record_worker_capacity(queue_name: &str, configured: u16, effective: u16, active: usize) {
    let attributes = [KeyValue::new("pgtask.queue.name", queue_name.to_owned())];
    let active = u64::try_from(active).unwrap_or(u64::MAX);
    metrics()
        .worker_configured_concurrency
        .record(u64::from(configured), &attributes);
    metrics()
        .worker_effective_concurrency
        .record(u64::from(effective), &attributes);
    metrics().worker_active_handlers.record(active, &attributes);
    metrics()
        .worker_available_slots
        .record(u64::from(effective).saturating_sub(active), &attributes);
}

pub fn record_worker_event_loop_lag(queue_name: &str, duration: Duration) {
    metrics().worker_event_loop_lag.record(
        duration.as_secs_f64(),
        &[KeyValue::new("pgtask.queue.name", queue_name.to_owned())],
    );
}

pub fn record_worker_lease_renewal_age(queue_name: &str, duration: Duration) {
    metrics().worker_lease_renewal_age.record(
        duration.as_secs_f64(),
        &[KeyValue::new("pgtask.queue.name", queue_name.to_owned())],
    );
}

pub fn record_worker_admission_limit(queue_name: &str, decision: &'static str, reason: &'static str, limit: u16) {
    metrics().worker_admission_limit.record(
        u64::from(limit),
        &[
            KeyValue::new("pgtask.queue.name", queue_name.to_owned()),
            KeyValue::new("pgtask.admission.decision", decision),
            KeyValue::new("pgtask.admission.reason", reason),
        ],
    );
}
