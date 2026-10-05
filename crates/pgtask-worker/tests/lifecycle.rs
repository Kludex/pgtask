//! A worker that stops hands its unfinished tasks back, and one whose listener connection drops
//! catches up on the wake-ups it missed.

use std::{
    net::SocketAddr,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use pgtask_core::{EnqueueRequest, HandlerVersion, QueueName, RetryPolicy, TaskName, TaskState};
use pgtask_postgres::Store;
use pgtask_worker::{HandlerRegistry, Worker, WorkerConfig};
use serde_json::{Value, json};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, mpsc},
    task::AbortHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_releases_an_unfinished_task_without_charging_an_attempt() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let queue_name = QueueName::new(format!("shutdown-release-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("shutdown-release").unwrap();
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    request.max_attempts = 1;
    let task_id = store.enqueue(&request).await.unwrap().task_id;

    let started = Arc::new(Notify::new());
    let mut registry = HandlerRegistry::new();
    let handler_started = Arc::clone(&started);
    registry.register(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |_| {
            let started = Arc::clone(&handler_started);
            async move {
                started.notify_one();
                std::future::pending().await
            }
        },
    );
    let mut config = WorkerConfig::new(queue_name.clone());
    // Long enough that only a release, not lease expiry, can return the task within the test.
    config.lease_duration = Duration::from_mins(1);
    config.shutdown_grace = Duration::from_millis(100);
    let worker = Worker::new(store.clone(), registry, config.clone()).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    tokio::time::timeout(TEST_TIMEOUT, started.notified()).await.unwrap();
    shutdown.cancel();
    tokio::time::timeout(TEST_TIMEOUT, worker_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let released = store.get_task(task_id).await.unwrap().unwrap();
    assert_eq!(released.state, TaskState::Pending);
    assert_eq!((released.attempt, released.failed_attempts), (1, 0));
    assert_eq!(released.lease_token, None);
    assert_eq!(released.error, None);
    let attempt_state: String = sqlx::query_scalar("SELECT state FROM pgtask.attempt_view WHERE task_id = $1")
        .bind(task_id.as_uuid())
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(attempt_state, "released");

    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name,
        HandlerVersion::default(),
        RetryPolicy::Never,
        |_| async move { Ok(json!("done")) },
    );
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    tokio::time::timeout(TEST_TIMEOUT, async {
        while store.get_task(task_id).await.unwrap().unwrap().state != TaskState::Succeeded {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
    let finished = store.get_task(task_id).await.unwrap().unwrap();
    assert_eq!((finished.attempt, finished.failed_attempts), (2, 0));
    assert_eq!(finished.result, Some(json!("done")));
}

/// A TCP proxy in front of PostgreSQL for the listener connection only. It can stop forwarding
/// what the server sends and then drop every connection, which the client sees as a plain EOF.
struct ListenerProxy {
    address: SocketAddr,
    muted: Arc<AtomicBool>,
    connections: Arc<std::sync::Mutex<Vec<AbortHandle>>>,
}

impl ListenerProxy {
    async fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let muted = Arc::new(AtomicBool::new(false));
        let connections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (accept_muted, accept_connections) = (Arc::clone(&muted), Arc::clone(&connections));
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let server = TcpStream::connect(upstream).await.unwrap();
                let muted = Arc::clone(&accept_muted);
                let connection = tokio::spawn(async move {
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let upstream = tokio::io::copy(&mut client_read, &mut server_write);
                    let downstream = async {
                        let mut buffer = vec![0; 8192];
                        loop {
                            let read = server_read.read(&mut buffer).await?;
                            if read == 0 {
                                return Ok::<_, std::io::Error>(());
                            }
                            if !muted.load(Ordering::SeqCst) {
                                client_write.write_all(&buffer[..read]).await?;
                            }
                        }
                    };
                    let _ = tokio::join!(upstream, downstream);
                });
                accept_connections.lock().unwrap().push(connection.abort_handle());
            }
        });
        Self {
            address,
            muted,
            connections,
        }
    }

    /// Drops what the server sends from now on, so notifications vanish as if the link were down.
    fn mute(&self) {
        self.muted.store(true, Ordering::SeqCst);
    }

    /// Closes every proxied connection. New connections are forwarded normally.
    fn drop_connections(&self) {
        for connection in self.connections.lock().unwrap().drain(..) {
            connection.abort();
        }
        self.muted.store(false, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_task_enqueued_while_the_listener_reconnects_is_claimed_promptly() {
    let Some(database_url) = database_url() else {
        return;
    };
    let options = PgConnectOptions::from_str(&database_url).unwrap();
    let upstream = tokio::net::lookup_host((options.get_host(), options.get_port()))
        .await
        .unwrap()
        .next()
        .unwrap();
    let proxy = ListenerProxy::start(upstream).await;
    let store = Store::from_pools(
        PgPoolOptions::new().connect_with(options.clone()).await.unwrap(),
        PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options.host("127.0.0.1").port(proxy.address.port()))
            .await
            .unwrap(),
    );
    store.migrate().await.unwrap();
    let queue_name = QueueName::new(format!("listener-gap-{}", Uuid::new_v4())).unwrap();
    let task_name = TaskName::new("listener-gap").unwrap();
    let (ran, mut runs) = mpsc::unbounded_channel::<Value>();
    let mut registry = HandlerRegistry::new();
    registry.register(
        task_name.clone(),
        HandlerVersion::default(),
        RetryPolicy::Never,
        move |task| {
            let ran = ran.clone();
            async move {
                ran.send(task.payload).unwrap();
                Ok(json!(null))
            }
        },
    );
    let mut config = WorkerConfig::new(queue_name.clone());
    // Only a notification, or the catch-up after a reconnect, wakes the worker within the test.
    config.poll_interval = Duration::from_mins(1);
    let worker = Worker::new(store.clone(), registry, config).unwrap();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let worker_task = tokio::spawn(async move { worker.run(worker_shutdown).await });
    let enqueue = |payload: &'static str| {
        let mut request = EnqueueRequest::new(task_name.clone(), json!(payload));
        request.queue_name = queue_name.clone();
        let store = store.clone();
        async move { store.enqueue(&request).await.unwrap() }
    };

    enqueue("before").await;
    let before = tokio::time::timeout(TEST_TIMEOUT, runs.recv()).await.unwrap();
    assert_eq!(before, Some(json!("before")));
    tokio::time::sleep(Duration::from_millis(100)).await;

    proxy.mute();
    enqueue("during").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    proxy.drop_connections();
    let during = tokio::time::timeout(Duration::from_secs(5), runs.recv()).await;

    shutdown.cancel();
    worker_task.await.unwrap().unwrap();
    assert_eq!(
        during.expect("the task enqueued during the outage waited for the poll interval"),
        Some(json!("during"))
    );
}
