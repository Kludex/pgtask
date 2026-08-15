use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use axum::{Router, extract::State, http::StatusCode, routing::get};
use pgtask_core::QueueName;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::{OverloadProtectionConfig, WorkerControl};

#[derive(Clone)]
pub(crate) struct Health {
    inner: Arc<HealthState>,
}

struct HealthState {
    admission: AtomicBool,
    database: AtomicBool,
    listener: AtomicBool,
    lease_renewal: AtomicBool,
    active_leases: AtomicBool,
    runtime_progress: Mutex<Instant>,
    last_lease_renewal: Mutex<Instant>,
}

struct HealthObservation {
    active_leases: bool,
    database: bool,
    lease_renewal: bool,
    lease_renewal_age: Duration,
    runtime_lag: Duration,
}

struct OverloadDetector {
    lag_samples: u16,
    recovery_samples: u16,
}

struct SupervisorSettings {
    address: Option<SocketAddr>,
    control: WorkerControl,
    interval: Duration,
    lease_renewal_threshold: Duration,
    overload: OverloadProtectionConfig,
    queue_name: QueueName,
}

impl Health {
    pub(crate) fn new() -> Self {
        let now = Instant::now();
        Self {
            inner: Arc::new(HealthState {
                admission: AtomicBool::new(false),
                database: AtomicBool::new(false),
                listener: AtomicBool::new(false),
                lease_renewal: AtomicBool::new(true),
                active_leases: AtomicBool::new(false),
                runtime_progress: Mutex::new(now),
                last_lease_renewal: Mutex::new(now),
            }),
        }
    }

    pub(crate) fn set_admission(&self, healthy: bool) {
        self.inner.admission.store(healthy, Ordering::Release);
    }

    pub(crate) fn set_database(&self, healthy: bool) {
        self.inner.database.store(healthy, Ordering::Release);
    }

    pub(crate) fn set_listener(&self, healthy: bool) {
        self.inner.listener.store(healthy, Ordering::Release);
    }

    pub(crate) fn set_active_leases(&self, active: bool) {
        self.inner.active_leases.store(active, Ordering::Release);
        if !active {
            self.inner.lease_renewal.store(true, Ordering::Release);
        }
    }

    pub(crate) fn record_lease_renewal(&self, healthy: bool) {
        self.inner.lease_renewal.store(healthy, Ordering::Release);
        if healthy {
            *self
                .inner
                .last_lease_renewal
                .lock()
                .expect("lease health lock is not poisoned") = Instant::now();
        }
    }

    pub(crate) fn record_runtime_progress(&self) {
        *self
            .inner
            .runtime_progress
            .lock()
            .expect("runtime health lock is not poisoned") = Instant::now();
    }

    fn ready(&self) -> bool {
        self.inner.admission.load(Ordering::Acquire)
            && self.inner.database.load(Ordering::Acquire)
            && self.inner.listener.load(Ordering::Acquire)
            && self.inner.lease_renewal.load(Ordering::Acquire)
    }

    fn record_metrics(&self, queue_name: &QueueName, interval: Duration) -> HealthObservation {
        let runtime_age = self
            .inner
            .runtime_progress
            .lock()
            .expect("runtime health lock is not poisoned")
            .elapsed();
        pgtask_otel::record_worker_event_loop_lag(queue_name.as_str(), runtime_age.saturating_sub(interval));
        let active_leases = self.inner.active_leases.load(Ordering::Acquire);
        let lease_renewal_age = if active_leases {
            self.inner
                .last_lease_renewal
                .lock()
                .expect("lease health lock is not poisoned")
                .elapsed()
        } else {
            Duration::ZERO
        };
        pgtask_otel::record_worker_lease_renewal_age(queue_name.as_str(), lease_renewal_age);
        HealthObservation {
            active_leases,
            database: self.inner.database.load(Ordering::Acquire),
            lease_renewal: self.inner.lease_renewal.load(Ordering::Acquire),
            lease_renewal_age,
            runtime_lag: runtime_age.saturating_sub(interval),
        }
    }
}

pub(crate) struct Supervisor {
    shutdown: CancellationToken,
    thread: Option<thread::JoinHandle<()>>,
}

impl Supervisor {
    pub(crate) fn start(
        health: Health,
        queue_name: QueueName,
        interval: Duration,
        address: Option<SocketAddr>,
        control: WorkerControl,
        overload: OverloadProtectionConfig,
        lease_renewal_threshold: Duration,
    ) -> io::Result<Self> {
        let shutdown = CancellationToken::new();
        let thread_shutdown = shutdown.clone();
        let settings = SupervisorSettings {
            address,
            control,
            interval,
            lease_renewal_threshold,
            overload,
            queue_name,
        };
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let thread = thread::Builder::new()
            .name("pgtask-supervisor".to_owned())
            .spawn(move || {
                runtime.block_on(run_supervisor(health, settings, thread_shutdown, startup_sender));
            })?;
        startup_receiver
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "worker supervisor stopped during startup"))??;
        Ok(Self {
            shutdown,
            thread: Some(thread),
        })
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(thread) = self.thread.take() {
            let _result = thread.join();
        }
    }
}

async fn run_supervisor(
    health: Health,
    settings: SupervisorSettings,
    shutdown: CancellationToken,
    startup_sender: mpsc::SyncSender<io::Result<()>>,
) {
    let listener = match settings.address {
        Some(address) => match TcpListener::bind(address).await {
            Ok(listener) => Some(listener),
            Err(error) => {
                let _result = startup_sender.send(Err(error));
                return;
            }
        },
        None => None,
    };
    startup_sender
        .send(Ok(()))
        .expect("the supervisor waits for its startup result");
    let sampler = sample_health(
        health.clone(),
        settings.queue_name,
        settings.interval,
        settings.control,
        settings.overload,
        settings.lease_renewal_threshold,
        shutdown.clone(),
    );
    let Some(listener) = listener else {
        sampler.await;
        return;
    };
    let application = Router::new()
        .route("/livez", get(live))
        .route("/readyz", get(ready))
        .with_state(health);
    let server_shutdown = shutdown.clone();
    let server_finished = shutdown.clone();
    let server = async move {
        let result = axum::serve(listener, application)
            .with_graceful_shutdown(server_shutdown.cancelled_owned())
            .await;
        server_finished.cancel();
        result.expect("the bound worker health listener remains available");
    };
    tokio::join!(server, sampler);
}

async fn sample_health(
    health: Health,
    queue_name: QueueName,
    interval: Duration,
    control: WorkerControl,
    overload: OverloadProtectionConfig,
    lease_renewal_threshold: Duration,
    shutdown: CancellationToken,
) {
    let mut detector = OverloadDetector {
        lag_samples: 0,
        recovery_samples: 0,
    };
    let mut interval_timer = tokio::time::interval(interval);
    interval_timer.tick().await;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = interval_timer.tick() => {
                let observation = health.record_metrics(&queue_name, interval);
                detector.observe(&control, &overload, lease_renewal_threshold, &observation);
            }
        }
    }
}

impl OverloadDetector {
    fn observe(
        &mut self,
        control: &WorkerControl,
        config: &OverloadProtectionConfig,
        lease_renewal_threshold: Duration,
        observation: &HealthObservation,
    ) {
        if !config.enabled {
            return;
        }
        if observation.runtime_lag >= config.event_loop_lag_threshold {
            self.lag_samples = self.lag_samples.saturating_add(1);
        } else {
            self.lag_samples = 0;
        }
        let reason = if !observation.database {
            Some("database_unavailable")
        } else if observation.active_leases
            && (!observation.lease_renewal || observation.lease_renewal_age >= lease_renewal_threshold)
        {
            Some("lease_renewal_late")
        } else if self.lag_samples >= config.sustained_samples.get() {
            Some("event_loop_lag")
        } else {
            None
        };
        if reason.is_some() {
            self.recovery_samples = 0;
        } else {
            self.recovery_samples = self.recovery_samples.saturating_add(1);
            if self.recovery_samples < config.recovery_samples.get() {
                return;
            }
        }
        let effective = control.effective_concurrency().get();
        let proposed = reason.map_or_else(
            || effective.saturating_add(1).min(control.configured_concurrency().get()),
            |_| (effective / 2).max(config.minimum_concurrency.get()),
        );
        let proposed = std::num::NonZeroU16::new(proposed).expect("the proposed limit is always nonzero");
        let reason = reason.unwrap_or("recovery");
        control.record_proposed_concurrency(proposed, reason);
        if config.enforce {
            control
                .apply_effective_concurrency(proposed, reason)
                .expect("the detector bounds proposals by the configured concurrency");
        }
    }
}

async fn live() -> &'static str {
    "live"
}

async fn ready(State(health): State<Health>) -> (StatusCode, &'static str) {
    if health.ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}
