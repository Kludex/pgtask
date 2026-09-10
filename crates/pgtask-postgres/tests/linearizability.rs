//! Linearizability checking for the lease protocol.
//!
//! The other tests assert invariants: they look at the state after each step and
//! ask whether it looks right. This asks a stronger question. Several clients
//! hammer the same tasks concurrently, every call is recorded with the interval
//! it occupied, and the resulting history is handed to Porcupine, which searches
//! for *some* sequential ordering of those overlapping calls that a correct
//! lease machine could have produced.
//!
//! A stale write that is accepted has no such ordering. Neither does a live
//! write that is rejected, or a claim that skips an attempt number. Those are
//! failures an invariant check can miss, because each individual snapshot can
//! look perfectly legal while the sequence as a whole is impossible.
//!
//! Operations are partitioned per task by the checker, because pgtask makes no
//! cross-task ordering promise -- `claim` uses `SKIP LOCKED` precisely so that
//! two workers get different tasks.
//!
//! The checker is Go (`scripts/linearizability`). Without a Go toolchain the
//! history is still generated, and the check is skipped.
//!
//! Replay a failure with `PGTASK_LINEARIZABILITY_SEED`.

use std::{
    path::Path,
    process::Command,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use pgtask_core::{EnqueueRequest, HandlerVersion, LeaseToken, QueueName, TaskId, TaskName, WorkerId};
use pgtask_postgres::Store;
use serde_json::{Value, json};
use uuid::Uuid;

const MAX_ATTEMPTS: u16 = 3;
const TASKS: usize = 6;
const CLIENTS: usize = 6;
const STEPS_PER_CLIENT: usize = 25;
const CHECKER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/linearizability");

/// xorshift64*, so a failing run replays from its seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            0
        } else {
            usize::try_from(self.next_u64() % bound as u64).unwrap_or(0)
        }
    }
}

/// A lease some client once held. Kept after it is superseded, because
/// replaying a dead lease is the point.
#[derive(Clone, Copy)]
struct Lease {
    task: TaskId,
    attempt: u16,
    token: LeaseToken,
}

struct Recorder {
    started: Instant,
    operations: Mutex<Vec<Value>>,
    leases: Mutex<Vec<Lease>>,
}

impl Recorder {
    fn now(&self) -> i64 {
        i64::try_from(self.started.elapsed().as_nanos()).unwrap_or(i64::MAX)
    }

    fn record(&self, client: usize, task: TaskId, call: i64, ret: i64, input: &Value, output: &Value) {
        self.operations.lock().unwrap().push(json!({
            "client_id": client,
            "task": task.to_string(),
            "call": call,
            "return": ret,
            "input": input,
            "output": output,
        }));
    }

    fn remember(&self, lease: Lease) {
        self.leases.lock().unwrap().push(lease);
    }

    /// Any lease seen so far, live or long dead.
    fn sample(&self, rng: &mut Rng) -> Option<Lease> {
        let leases = self.leases.lock().unwrap();
        if leases.is_empty() {
            return None;
        }
        Some(leases[rng.below(leases.len())])
    }
}

struct Client {
    id: usize,
    store: Arc<Store>,
    recorder: Arc<Recorder>,
    recovery: Arc<tokio::sync::Mutex<()>>,
    queue: QueueName,
    task_name: TaskName,
}

impl Client {
    /// Claim is queue-wide, so it is recorded only when this client came away
    /// with a task; a claim that returned nothing belongs to no task's history.
    async fn claim(&self) {
        let call = self.recorder.now();
        let claimed = self
            .store
            .claim(
                &self.queue,
                WorkerId::new(),
                &[(self.task_name.clone(), HandlerVersion::default())],
                1,
                Duration::from_mins(10),
            )
            .await
            .unwrap();
        let ret = self.recorder.now();
        let Some(task) = claimed.first() else { return };

        let lease = Lease {
            task: task.id,
            attempt: task.attempt,
            token: task.lease_token.unwrap(),
        };
        self.recorder.record(
            self.id,
            task.id,
            call,
            ret,
            &json!({"op": "claim"}),
            &json!({"ok": true, "attempt": task.attempt, "token": lease.token.to_string()}),
        );
        self.recorder.remember(lease);
    }

    async fn complete(&self, lease: Lease) {
        let call = self.recorder.now();
        let ok = self
            .store
            .complete(lease.task, lease.attempt, lease.token, Some(&json!({"ok": true})))
            .await
            .unwrap();
        let ret = self.recorder.now();
        self.recorder.record(
            self.id,
            lease.task,
            call,
            ret,
            &json!({"op": "complete", "attempt": lease.attempt, "token": lease.token.to_string()}),
            &json!({"ok": ok}),
        );
    }

    async fn fail(&self, lease: Lease) {
        let call = self.recorder.now();
        let outcome = self
            .store
            .fail(
                lease.task,
                lease.attempt,
                lease.token,
                &json!({"type": "linearizability"}),
                Some(Duration::ZERO),
            )
            .await
            .unwrap();
        let ret = self.recorder.now();
        self.recorder.record(
            self.id,
            lease.task,
            call,
            ret,
            &json!({"op": "fail", "attempt": lease.attempt, "token": lease.token.to_string()}),
            &json!({
                "ok": outcome.is_some(),
                "state": outcome.map(|state| format!("{state:?}").to_lowercase()),
            }),
        );
    }

    async fn renew(&self, lease: Lease) {
        let call = self.recorder.now();
        let renewed = self
            .store
            .renew_lease(lease.task, lease.attempt, lease.token, Duration::from_mins(10))
            .await
            .unwrap();
        let ret = self.recorder.now();
        self.recorder.record(
            self.id,
            lease.task,
            call,
            ret,
            &json!({"op": "renew", "attempt": lease.attempt, "token": lease.token.to_string()}),
            &json!({"ok": renewed}),
        );
    }

    /// What a dead worker looks like to the database: the lease lapses and
    /// another pass reclaims it. Serialised, because the sweep is queue-wide and
    /// two overlapping expiries would make it ambiguous which task it reclaimed.
    async fn expire_and_recover(&self, lease: Lease) {
        let guard = self.recovery.lock().await;
        sqlx::query(
            "UPDATE pgtask.tasks
             SET lease_expires_at = statement_timestamp() - interval '1 second'
             WHERE id = $1 AND state = 'running'",
        )
        .bind(lease.task.as_uuid())
        .execute(self.store.pool())
        .await
        .unwrap();

        let call = self.recorder.now();
        let recovered = self.store.recover_expired(&self.queue, 100).await.unwrap();
        let ret = self.recorder.now();
        drop(guard);

        self.recorder.record(
            self.id,
            lease.task,
            call,
            ret,
            &json!({"op": "recover"}),
            &json!({"ok": recovered >= 1}),
        );
    }

    async fn run(self, mut rng: Rng) {
        for _ in 0..STEPS_PER_CLIENT {
            let choice = rng.below(100);
            if choice < 35 {
                self.claim().await;
                continue;
            }
            let Some(lease) = self.recorder.sample(&mut rng) else {
                continue;
            };
            match choice {
                35..=57 => self.complete(lease).await,
                58..=76 => self.fail(lease).await,
                77..=89 => self.renew(lease).await,
                _ => self.expire_and_recover(lease).await,
            }
        }
    }
}

fn go_available() -> bool {
    Command::new("go")
        .arg("version")
        .output()
        .is_ok_and(|output| output.status.success())
}

/// Runs the Go checker. `expect_fail` inverts the verdict, for the corruption
/// check below.
fn check(path: &Path, expect_fail: bool) -> (bool, String) {
    let mut command = Command::new("go");
    command.args(["run", "."]);
    if expect_fail {
        command.arg("--expect-fail");
    }
    let output = command
        .arg("--history")
        .arg(path)
        .current_dir(CHECKER)
        .output()
        .expect("the checker runs");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), combined)
}

fn write_history(path: &Path, operations: &[Value]) {
    let history = json!({ "max_attempts": MAX_ATTEMPTS, "operations": operations });
    std::fs::write(path, serde_json::to_vec_pretty(&history).unwrap()).unwrap();
}

/// Flips one rejected write into an accepted one, which is a stale lease being
/// honoured. No ordering explains that, so the checker must reject it.
fn corrupt(operations: &[Value]) -> Vec<Value> {
    let mut corrupted = operations.to_vec();
    let flipped = corrupted.iter_mut().find(|operation| {
        let op = operation["input"]["op"].as_str().unwrap_or_default();
        matches!(op, "complete" | "renew") && operation["output"]["ok"] == json!(false)
    });
    let flipped = flipped.expect("a rejected write to flip; without one the corruption check is vacuous");
    flipped["output"]["ok"] = json!(true);
    corrupted
}

async fn seed_tasks(store: &Store, queue: &QueueName, task_name: &TaskName) {
    for _ in 0..TASKS {
        let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
        request.queue_name = queue.clone();
        request.max_attempts = MAX_ATTEMPTS;
        store.enqueue(&request).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_lease_protocol_is_linearizable() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let seed: u64 = std::env::var("PGTASK_LINEARIZABILITY_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0x11EA_5E11_u64);

    let store = Arc::new(Store::connect(&database_url).await.unwrap());
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue = QueueName::new(format!("lin-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("lin-task-{suffix}")).unwrap();
    seed_tasks(&store, &queue, &task_name).await;

    let recorder = Arc::new(Recorder {
        started: Instant::now(),
        operations: Mutex::new(Vec::new()),
        leases: Mutex::new(Vec::new()),
    });
    let recovery = Arc::new(tokio::sync::Mutex::new(()));

    let mut clients = Vec::new();
    for id in 0..CLIENTS {
        let client = Client {
            id,
            store: Arc::clone(&store),
            recorder: Arc::clone(&recorder),
            recovery: Arc::clone(&recovery),
            queue: queue.clone(),
            task_name: task_name.clone(),
        };
        clients.push(tokio::spawn(
            client.run(Rng::new(seed.wrapping_add(id as u64 * 0x9E37_79B9))),
        ));
    }
    for client in clients {
        client.await.unwrap();
    }

    let operations = recorder.operations.lock().unwrap().clone();
    assert!(
        operations.len() > CLIENTS * 4,
        "only {} operations recorded, too thin a history to prove anything",
        operations.len()
    );

    let path = std::env::temp_dir().join(format!("pgtask-history-{suffix}.json"));
    write_history(&path, &operations);

    if !go_available() {
        eprintln!(
            "go not found; wrote {} operations to {} but skipped the check",
            operations.len(),
            path.display()
        );
        return;
    }

    let (ok, report) = check(&path, false);
    assert!(
        ok,
        "the lease history is not linearizable. Replay with \
         PGTASK_LINEARIZABILITY_SEED={seed}\nhistory: {}\n{report}",
        path.display()
    );
    println!("{}", report.trim());

    // A checker that accepts everything would report exactly the same thing, so
    // prove it has teeth on this very history.
    let corrupted_path = std::env::temp_dir().join(format!("pgtask-history-{suffix}-corrupt.json"));
    write_history(&corrupted_path, &corrupt(&operations));
    let (rejected, corruption_report) = check(&corrupted_path, true);
    assert!(
        rejected,
        "the checker accepted a history in which a stale write succeeded, so it is not \
         actually testing anything.\n{corruption_report}"
    );
    println!("corruption check: {}", corruption_report.trim());

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&corrupted_path);
}
