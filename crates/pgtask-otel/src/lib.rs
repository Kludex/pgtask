#![doc = "OpenTelemetry conventions and propagation for pgtask."]

mod metrics;
mod propagation;

pub use metrics::{
    record_cancelled, record_claimed, record_enqueued, record_execution, record_failed, record_lease_lost,
    record_queue_latency, record_recovered, record_renewed, record_retried, record_schedule_materialization,
    record_schedule_occurrences, record_succeeded, record_worker_admission_limit, record_worker_capacity,
    record_worker_event_loop_lag, record_worker_lease_renewal_age,
};
pub use propagation::{configure_propagation, inject_context, inject_span_context, set_parent_from_headers};
