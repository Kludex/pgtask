#![doc = "Python bindings for pgtask."]

use std::{
    net::SocketAddr,
    num::{NonZeroU16, NonZeroU32},
    sync::{Arc, Mutex},
    time::Duration,
};

use pgtask::{
    core::{
        EnqueueRequest, HandlerVersion, QueueName, RetryPolicy, SignalName, StepName, Task, TaskId, TaskName,
        TaskResult,
    },
    postgres::{Store, StoreConfig, TaskResultWait},
    worker::{HandlerError, HandlerRegistry, TaskContext, Worker, WorkerConfig},
};
use pyo3::{
    create_exception,
    exceptions::{PyException, PyRuntimeError, PyValueError},
    prelude::*,
    types::{PyAny, PyDict, PyModule},
};
use pythonize::{depythonize, pythonize};
use serde::Deserialize;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

pub use pgtask::core::{STORAGE_PROTOCOL_MAX_VERSION, STORAGE_PROTOCOL_MIN_VERSION, STORAGE_PROTOCOL_VERSION};

create_exception!(_native, TaskSuspended, PyException);

#[pyclass(name = "Client")]
struct PythonClient {
    config: StoreConfig,
    store: Store,
}

#[derive(Clone)]
struct PythonHandler {
    function: Arc<Py<PyAny>>,
    name: TaskName,
    retry_policy: RetryPolicy,
    version: HandlerVersion,
}

#[pyclass(name = "Worker")]
struct PythonWorker {
    concurrency: NonZeroU16,
    config: StoreConfig,
    handlers: Mutex<Vec<PythonHandler>>,
    lease_duration: Duration,
    poll_interval: Duration,
    health_address: Option<SocketAddr>,
    queues: Vec<QueueName>,
    shutdown: CancellationToken,
}

#[derive(Deserialize)]
struct PythonWorkerOptions {
    concurrency: u16,
    poll_interval: f64,
    lease_duration: f64,
    health_address: Option<String>,
    listener_url: Option<String>,
    max_query_connections: u32,
    max_listener_connections: u32,
}

struct PythonFutureGuard {
    future: Py<PyAny>,
}

#[pyclass(name = "TaskContext")]
struct PythonTaskContext {
    inner: TaskContext,
}

impl Drop for PythonFutureGuard {
    fn drop(&mut self) {
        Python::attach(|py| {
            let _result = self.future.call_method0(py, "cancel");
        });
    }
}

#[pymethods]
impl PythonClient {
    #[staticmethod]
    #[pyo3(signature = (database_url, *, listener_url=None, max_query_connections=10, max_listener_connections=1))]
    fn connect(
        py: Python<'_>,
        database_url: String,
        listener_url: Option<String>,
        max_query_connections: u32,
        max_listener_connections: u32,
    ) -> PyResult<Bound<'_, PyAny>> {
        let config = store_config(
            database_url,
            listener_url,
            max_query_connections,
            max_listener_connections,
        )?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let store = Store::connect_with_config(&config).await.map_err(runtime_error)?;
            store
                .ensure_storage_protocol(pgtask::core::STORAGE_PROTOCOL_RANGE)
                .await
                .map_err(runtime_error)?;
            Ok(Self { config, store })
        })
    }

    fn migrate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let config = self.config.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(runtime_error)?
                    .block_on(async move {
                        let store = Store::connect_with_config(&config).await.map_err(runtime_error)?;
                        store.migrate().await.map_err(runtime_error)
                    })
            })
            .await
            .map_err(runtime_error)??;
            Ok(())
        })
    }

    fn enqueue<'py>(&self, py: Python<'py>, request: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let request: EnqueueRequest = depythonize(request).map_err(value_error)?;
        let store = self.store.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.enqueue(&request).await.map_err(runtime_error)?;
            Ok((result.task_id.to_string(), result.created))
        })
    }

    fn enqueue_many<'py>(&self, py: Python<'py>, requests: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let requests: Vec<EnqueueRequest> = depythonize(requests).map_err(value_error)?;
        let store = self.store.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let results = store.enqueue_many(&requests).await.map_err(runtime_error)?;
            Ok(results
                .into_iter()
                .map(|result| (result.task_id.to_string(), result.created))
                .collect::<Vec<_>>())
        })
    }

    fn task_result<'py>(&self, py: Python<'py>, task_id: &str) -> PyResult<Bound<'py, PyAny>> {
        let store = self.store.clone();
        let task_id = task_id.parse().map_err(value_error)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store.task_result(task_id).await.map_err(runtime_error)?;
            result_to_python(result)
        })
    }

    fn wait_result<'py>(&self, py: Python<'py>, task_id: &str, timeout: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let store = self.store.clone();
        let task_id = task_id.parse().map_err(value_error)?;
        let timeout = timeout
            .map(|seconds| Duration::try_from_secs_f64(seconds).map_err(value_error))
            .transpose()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = store
                .wait_for_task_result(task_id, timeout)
                .await
                .map_err(runtime_error)?;
            match result {
                TaskResultWait::Ready(result) => result_to_python(Some(result)),
                TaskResultWait::NotFound | TaskResultWait::TimedOut => result_to_python(None),
            }
        })
    }

    fn emit_signal<'py>(
        &self,
        py: Python<'py>,
        task_id: &str,
        name: String,
        occurrence: u32,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.store.clone();
        let task_id: TaskId = task_id.parse().map_err(value_error)?;
        let name = SignalName::new(name).map_err(value_error)?;
        let value: Value = depythonize(value).map_err(value_error)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let signal = store
                .emit_signal(task_id, &name, occurrence, &value)
                .await
                .map_err(runtime_error)?;
            Python::attach(|py| pythonize(py, &signal.value).map(Bound::unbind).map_err(value_error))
        })
    }

    fn cancel<'py>(&self, py: Python<'py>, task_id: &str) -> PyResult<Bound<'py, PyAny>> {
        let store = self.store.clone();
        let task_id = task_id.parse().map_err(value_error)?;
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            async move { store.cancel(task_id).await.map_err(runtime_error) },
        )
    }
}

#[pymethods]
impl PythonTaskContext {
    fn step<'py>(
        &self,
        py: Python<'py>,
        name: String,
        occurrence: u32,
        operation: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if !operation.bind(py).is_callable() {
            return Err(PyValueError::new_err("step operation must be callable"));
        }
        let context = self.inner.clone();
        let step_name = StepName::new(name).map_err(value_error)?;
        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(py)?.copy_context(py)?;
        pyo3_async_runtimes::tokio::future_into_py_with_locals(py, locals.clone(), async move {
            let value = context
                .step(&step_name, occurrence, || run_python_operation(operation, locals))
                .await
                .map_err(|error| handler_error(&error))?;
            Python::attach(|py| pythonize(py, &value).map(Bound::unbind).map_err(value_error))
        })
    }

    fn sleep_for<'py>(
        &self,
        py: Python<'py>,
        name: String,
        occurrence: u32,
        seconds: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let context = self.inner.clone();
        let step_name = StepName::new(name).map_err(value_error)?;
        let duration = Duration::try_from_secs_f64(seconds).map_err(value_error)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            context
                .sleep_for(&step_name, occurrence, duration)
                .await
                .map_err(|error| handler_error(&error))
        })
    }

    fn sleep_until<'py>(
        &self,
        py: Python<'py>,
        name: String,
        occurrence: u32,
        wake_at: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let context = self.inner.clone();
        let step_name = StepName::new(name).map_err(value_error)?;
        let wake_at = wake_at.parse().map_err(value_error)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            context
                .sleep_until(&step_name, occurrence, wake_at)
                .await
                .map_err(|error| handler_error(&error))
        })
    }

    #[pyo3(signature = (step_name, occurrence, signal_name, signal_occurrence=0, timeout=None))]
    fn wait_for_signal<'py>(
        &self,
        py: Python<'py>,
        step_name: String,
        occurrence: u32,
        signal_name: String,
        signal_occurrence: u32,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let context = self.inner.clone();
        let step_name = StepName::new(step_name).map_err(value_error)?;
        let signal_name = SignalName::new(signal_name).map_err(value_error)?;
        let timeout = timeout
            .map(|seconds| Duration::try_from_secs_f64(seconds).map_err(value_error))
            .transpose()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let value = context
                .wait_for_signal(&step_name, occurrence, &signal_name, signal_occurrence, timeout)
                .await
                .map_err(|error| handler_error(&error))?;
            Python::attach(|py| pythonize(py, &value).map(Bound::unbind).map_err(value_error))
        })
    }

    fn spawn<'py>(
        &self,
        py: Python<'py>,
        step_name: String,
        occurrence: u32,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let context = self.inner.clone();
        let step_name = StepName::new(step_name).map_err(value_error)?;
        let request: EnqueueRequest = depythonize(request).map_err(value_error)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            context
                .spawn(&step_name, occurrence, &request)
                .await
                .map(|task_id| task_id.to_string())
                .map_err(|error| handler_error(&error))
        })
    }

    #[pyo3(signature = (step_name, occurrence, task_id, timeout=None))]
    fn wait_for_result<'py>(
        &self,
        py: Python<'py>,
        step_name: String,
        occurrence: u32,
        task_id: &str,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let context = self.inner.clone();
        let step_name = StepName::new(step_name).map_err(value_error)?;
        let task_id = task_id.parse().map_err(value_error)?;
        let timeout = timeout
            .map(|seconds| Duration::try_from_secs_f64(seconds).map_err(value_error))
            .transpose()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let value = context
                .wait_for_result(&step_name, occurrence, task_id, timeout)
                .await
                .map_err(|error| handler_error(&error))?;
            Python::attach(|py| pythonize(py, &value).map(Bound::unbind).map_err(value_error))
        })
    }
}

#[pymethods]
impl PythonWorker {
    #[new]
    fn new(database_url: String, queue_names: Vec<String>, options: &Bound<'_, PyAny>) -> PyResult<Self> {
        let options: PythonWorkerOptions = depythonize(options).map_err(value_error)?;
        Ok(Self {
            concurrency: NonZeroU16::new(options.concurrency)
                .ok_or_else(|| PyValueError::new_err("concurrency must be positive"))?,
            config: store_config(
                database_url,
                options.listener_url,
                options.max_query_connections,
                options.max_listener_connections,
            )?,
            handlers: Mutex::new(Vec::new()),
            lease_duration: Duration::try_from_secs_f64(options.lease_duration).map_err(value_error)?,
            poll_interval: Duration::try_from_secs_f64(options.poll_interval).map_err(value_error)?,
            health_address: options
                .health_address
                .map(|value| value.parse())
                .transpose()
                .map_err(value_error)?,
            queues: queue_names
                .into_iter()
                .map(|name| QueueName::new(&name).map_err(value_error))
                .collect::<PyResult<_>>()?,
            shutdown: CancellationToken::new(),
        })
    }

    #[pyo3(signature = (name, function, handler_version=1, retry_delay=1.0))]
    fn register(
        &self,
        name: &str,
        function: Py<PyAny>,
        handler_version: u32,
        retry_delay: Option<f64>,
    ) -> PyResult<()> {
        let retry_policy = retry_delay
            .map(|seconds| Duration::try_from_secs_f64(seconds).map_err(value_error))
            .transpose()?
            .map_or(RetryPolicy::Never, |delay| RetryPolicy::Fixed { delay });
        let handler = PythonHandler {
            function: Arc::new(function),
            name: TaskName::new(name).map_err(value_error)?,
            retry_policy,
            version: HandlerVersion::new(
                NonZeroU32::new(handler_version)
                    .ok_or_else(|| PyValueError::new_err("handler_version must be positive"))?,
            ),
        };
        self.handlers
            .lock()
            .map_err(|_| PyRuntimeError::new_err("handler registry lock is poisoned"))?
            .push(handler);
        Ok(())
    }

    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let handlers = self
            .handlers
            .lock()
            .map_err(|_| PyRuntimeError::new_err("handler registry lock is poisoned"))?
            .clone();
        if handlers.is_empty() {
            return Err(PyValueError::new_err("at least one handler must be registered"));
        }
        let locals = pyo3_async_runtimes::TaskLocals::with_running_loop(py)?.copy_context(py)?;
        let store_config = self.config.clone();
        let queues = self.queues.clone();
        let concurrency = self.concurrency;
        let lease_duration = self.lease_duration;
        let poll_interval = self.poll_interval;
        let health_address = self.health_address;
        let shutdown = self.shutdown.clone();
        pyo3_async_runtimes::tokio::future_into_py_with_locals(py, locals.clone(), async move {
            let store = Store::connect_with_config(&store_config).await.map_err(runtime_error)?;
            let mut registry = HandlerRegistry::new();
            for handler in handlers {
                let function = Arc::clone(&handler.function);
                let handler_locals = locals.clone();
                registry.register_durable(
                    handler.name,
                    handler.version,
                    handler.retry_policy,
                    move |task, context| {
                        run_python_handler(Arc::clone(&function), handler_locals.clone(), task, context)
                    },
                );
            }
            let mut config = WorkerConfig::with_queues(queues);
            config.concurrency = concurrency;
            config.claim_batch_size = concurrency;
            config.lease_duration = lease_duration;
            config.poll_interval = poll_interval;
            config.health_address = health_address;
            Worker::new(store, registry, config)
                .map_err(runtime_error)?
                .run(shutdown)
                .await
                .map_err(runtime_error)
        })
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

fn store_config(
    database_url: String,
    listener_url: Option<String>,
    max_query_connections: u32,
    max_listener_connections: u32,
) -> PyResult<StoreConfig> {
    let mut config = StoreConfig::new(database_url)
        .with_query_connections(
            NonZeroU32::new(max_query_connections)
                .ok_or_else(|| PyValueError::new_err("max_query_connections must be positive"))?,
        )
        .with_listener_connections(
            NonZeroU32::new(max_listener_connections)
                .ok_or_else(|| PyValueError::new_err("max_listener_connections must be positive"))?,
        );
    if let Some(listener_url) = listener_url {
        config = config.with_listener_url(listener_url);
    }
    Ok(config)
}

async fn run_python_handler(
    function: Arc<Py<PyAny>>,
    locals: pyo3_async_runtimes::TaskLocals,
    mut task: Task,
    context: TaskContext,
) -> Result<Value, HandlerError> {
    task.headers = pgtask::otel::inject_span_context(&task.headers, &tracing::Span::current());
    let (future, _guard) = Python::attach(|py| {
        let handler_locals = locals.clone().with_context(locals.context(py).call_method0("copy")?);
        let value = serde_json::to_value(task).map_err(runtime_error)?;
        let argument = pythonize(py, &value).map_err(value_error)?;
        let durable_context = Py::new(py, PythonTaskContext { inner: context })?;
        let coroutine = handler_locals
            .context(py)
            .call_method1("run", (function.bind(py), argument, durable_context))?;
        let asyncio = py.import("asyncio")?;
        let concurrent = handler_locals.context(py).call_method1(
            "run",
            (
                asyncio.getattr("run_coroutine_threadsafe")?,
                coroutine,
                handler_locals.event_loop(py),
            ),
        );
        let concurrent = concurrent?;
        let keywords = PyDict::new(py);
        keywords.set_item("loop", handler_locals.event_loop(py))?;
        let awaitable = asyncio.call_method("wrap_future", (concurrent.clone(),), Some(&keywords))?;
        let future = pyo3_async_runtimes::into_future_with_locals(&handler_locals, awaitable)?;
        Ok((
            future,
            PythonFutureGuard {
                future: concurrent.unbind(),
            },
        ))
    })
    .map_err(|error: PyErr| HandlerError::retryable(error.to_string()))?;
    let result = future.await.map_err(|error| {
        Python::attach(|py| {
            if error.is_instance_of::<TaskSuspended>(py) {
                HandlerError::suspended()
            } else {
                HandlerError::retryable(error.to_string())
            }
        })
    })?;
    Python::attach(|py| depythonize(result.bind(py)))
        .map_err(|error| HandlerError::terminal(format!("handler returned invalid JSON: {error}")))
}

async fn run_python_operation(
    operation: Py<PyAny>,
    locals: pyo3_async_runtimes::TaskLocals,
) -> Result<Value, HandlerError> {
    let (future, _guard) = Python::attach(|py| {
        let operation_locals = locals.clone().with_context(locals.context(py).call_method0("copy")?);
        let coroutine = operation_locals
            .context(py)
            .call_method1("run", (operation.bind(py),))?;
        let asyncio = py.import("asyncio")?;
        let concurrent = operation_locals.context(py).call_method1(
            "run",
            (
                asyncio.getattr("run_coroutine_threadsafe")?,
                coroutine,
                operation_locals.event_loop(py),
            ),
        );
        let concurrent = concurrent?;
        let keywords = PyDict::new(py);
        keywords.set_item("loop", operation_locals.event_loop(py))?;
        let awaitable = asyncio.call_method("wrap_future", (concurrent.clone(),), Some(&keywords))?;
        let future = pyo3_async_runtimes::into_future_with_locals(&operation_locals, awaitable)?;
        Ok((
            future,
            PythonFutureGuard {
                future: concurrent.unbind(),
            },
        ))
    })
    .map_err(|error: PyErr| HandlerError::retryable(error.to_string()))?;
    let result = future
        .await
        .map_err(|error| HandlerError::retryable(error.to_string()))?;
    Python::attach(|py| depythonize(result.bind(py)))
        .map_err(|error| HandlerError::terminal(format!("step returned invalid JSON: {error}")))
}

fn result_to_python(result: Option<TaskResult>) -> PyResult<Py<PyAny>> {
    Python::attach(|py| match result {
        Some(result) => {
            let value = serde_json::to_value(result).map_err(runtime_error)?;
            pythonize(py, &value).map(Bound::unbind).map_err(value_error)
        }
        None => Ok(py.None()),
    })
}

fn runtime_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn value_error(error: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn handler_error(error: &HandlerError) -> PyErr {
    if error.is_suspended() {
        TaskSuspended::new_err("task suspended")
    } else {
        let message = error
            .error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("task handler failed");
        PyRuntimeError::new_err(message.to_owned())
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    pgtask::otel::configure_propagation();
    module.add_class::<PythonClient>()?;
    module.add_class::<PythonTaskContext>()?;
    module.add_class::<PythonWorker>()?;
    module.add("TaskSuspended", module.py().get_type::<TaskSuspended>())?;
    Ok(())
}
