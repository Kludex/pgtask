//! Linearizability checking for pgtask's concurrent protocols.
//!
//! The other tests assert invariants: they look at the state after each step and
//! ask whether it looks right. This asks a stronger question. Several clients
//! hammer the same rows concurrently, every call is recorded with the interval
//! it occupied, and Porcupine searches for *some* sequential ordering of those
//! overlapping calls that a correct machine could have produced.
//!
//! Failures an invariant check can miss live exactly here, because each
//! individual snapshot can look perfectly legal while the sequence as a whole is
//! impossible: a stale write that is accepted, a live write that is rejected, or
//! a durable step whose recorded value changes between replays.
//!
//! Two protocols are covered, each with its own reference model in
//! `scripts/linearizability`:
//!
//!   * `lease` -- claim, renew, complete, fail, recover
//!   * `register` -- `commit_checkpoint` and `emit_signal`, which both promise
//!     first-write-wins
//!   * `idempotency` -- `enqueue` deduplicating on a key
//!   * `capacity` -- admission against `max_outstanding_tasks`
//!
//! Histories are partitioned per key, because pgtask makes no cross-key ordering
//! promise -- `claim` uses `SKIP LOCKED` precisely so two workers get different
//! tasks. What must hold is the sequence of operations on each key on its own.
//!
//! Without a Go toolchain the histories are still generated and the check is
//! skipped. Replay a run with `PGTASK_LINEARIZABILITY_SEED`.

use std::{
    future::Future,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use pgtask_core::{
    EnqueueRequest, HandlerVersion, LeaseToken, QueueConfig, QueueName, SignalName, StepName, TaskId, TaskName,
    WorkerId,
};
use pgtask_postgres::Store;
use serde_json::{Value, json};
use uuid::Uuid;

const MAX_ATTEMPTS: u16 = 3;
const TASKS: usize = 6;
const CLIENTS: usize = 6;
const STEPS_PER_CLIENT: usize = 25;
const CHECKER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../scripts/linearizability");
/// Outstanding-task limit for the capacity test. Small, so the queue is
/// genuinely full for much of the run.
const CAPACITY: u64 = 3;

/// How often a call is cut off mid-flight, as a percentage.
const FAULT_PERCENT: usize = 45;

/// How long a doomed call is allowed to run before the client abandons it.
///
/// The window straddles a round trip on purpose. Too short and every cut lands
/// before the statement reaches the server, which is an uninteresting fault;
/// too long and nothing is ever cut at all. Spreading it means some writes
/// commit and are never acknowledged, which is the case worth testing.
fn cut_after(rng: &mut Rng) -> Duration {
    Duration::from_micros(50 + (rng.below(900) as u64))
}

/// Runs a call, abandoning it after `cut` if one is given.
///
/// Dropping the future cancels the query from the client's side, but says
/// nothing about the server: the statement may already have committed, may
/// commit moments later, or may never land at all. That is the situation
/// `docs/failure-model.md` describes as "the client treats the transaction
/// outcome as unknown", and it is the one worth testing, because a client that
/// guesses wrong here corrupts state rather than merely stalling.
async fn maybe_cut<T>(work: impl Future<Output = T>, cut: Option<Duration>) -> Option<T> {
    match cut {
        Some(deadline) => tokio::time::timeout(deadline, work).await.ok(),
        None => Some(work.await),
    }
}

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
    fn new() -> Self {
        Self {
            started: Instant::now(),
            operations: Mutex::new(Vec::new()),
            leases: Mutex::new(Vec::new()),
        }
    }

    fn now(&self) -> i64 {
        i64::try_from(self.started.elapsed().as_nanos()).unwrap_or(i64::MAX)
    }

    fn record(&self, client: usize, partition: &str, call: i64, ret: i64, input: &Value, output: &Value) {
        self.operations.lock().unwrap().push(json!({
            "client_id": client,
            "partition": partition,
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

    fn take(&self) -> Vec<Value> {
        self.operations.lock().unwrap().clone()
    }
}

fn go_available() -> bool {
    Command::new("go")
        .arg("version")
        .output()
        .is_ok_and(|output| output.status.success())
}

/// Runs the Go checker. `expect_fail` inverts the verdict, for the corruption
/// checks.
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

fn write_history(path: &Path, model: &str, operations: &[Value]) {
    let history = json!({
        "model": model,
        "max_attempts": MAX_ATTEMPTS,
        "capacity": CAPACITY,
        "operations": operations,
    });
    std::fs::write(path, serde_json::to_vec_pretty(&history).unwrap()).unwrap();
}

fn history_path(name: &str, suffix: Uuid) -> PathBuf {
    std::env::temp_dir().join(format!("pgtask-{name}-{suffix}.json"))
}

fn seed() -> u64 {
    std::env::var("PGTASK_LINEARIZABILITY_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0x11EA_5E11_u64)
}

/// Runs the checker over a history, then proves the checker has teeth by
/// corrupting one operation and requiring the same history to stop linearizing.
/// A checker that accepts everything reports the same thing as one that works.
fn check_and_prove_teeth(name: &str, model: &str, operations: &[Value], corrupt: impl Fn(&mut Value)) {
    assert!(
        operations.len() > CLIENTS * 4,
        "only {} operations recorded, too thin a history to prove anything",
        operations.len()
    );
    let suffix = Uuid::new_v4();
    let path = history_path(name, suffix);
    write_history(&path, model, operations);

    if !go_available() {
        eprintln!(
            "go not found; wrote {} {model} operations to {} but skipped the check",
            operations.len(),
            path.display()
        );
        return;
    }

    let (ok, report) = check(&path, false);
    assert!(
        ok,
        "the {model} history is not linearizable. Replay with \
         PGTASK_LINEARIZABILITY_SEED={}\nhistory: {}\n{report}",
        seed(),
        path.display()
    );
    println!("{}", report.trim());

    let mut corrupted = operations.to_vec();
    let mut applied = false;
    for operation in &mut corrupted {
        let before = operation.clone();
        corrupt(operation);
        if *operation != before {
            applied = true;
            break;
        }
    }
    assert!(
        applied,
        "nothing in the {model} history could be corrupted, so the check is vacuous"
    );

    let corrupted_path = history_path(&format!("{name}-corrupt"), suffix);
    write_history(&corrupted_path, model, &corrupted);
    let (rejected, corruption_report) = check(&corrupted_path, true);
    assert!(
        rejected,
        "the checker accepted a corrupted {model} history, so it is not actually testing \
         anything.\n{corruption_report}"
    );
    println!("corruption check: {}", corruption_report.trim());

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&corrupted_path);
}

// ---------------------------------------------------------------- lease -----

struct LeaseClient {
    id: usize,
    store: Arc<Store>,
    recorder: Arc<Recorder>,
    recovery: Arc<tokio::sync::Mutex<()>>,
    queue: QueueName,
    task_name: TaskName,
}

impl LeaseClient {
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
            &task.id.to_string(),
            call,
            ret,
            &json!({"op": "claim"}),
            &json!({"ok": true, "attempt": task.attempt, "token": lease.token.to_string()}),
        );
        self.recorder.remember(lease);
    }

    async fn complete(&self, lease: Lease, cut: Option<Duration>) {
        let call = self.recorder.now();
        let outcome = maybe_cut(
            self.store
                .complete(lease.task, lease.attempt, lease.token, Some(&json!({"ok": true}))),
            cut,
        )
        .await;
        let ret = self.recorder.now();
        self.recorder.record(
            self.id,
            &lease.task.to_string(),
            call,
            ret,
            &json!({"op": "complete", "attempt": lease.attempt, "token": lease.token.to_string()}),
            &match outcome {
                Some(result) => json!({"ok": result.unwrap()}),
                None => json!({"unknown": true}),
            },
        );
    }

    async fn fail(&self, lease: Lease, cut: Option<Duration>) {
        let call = self.recorder.now();
        let outcome = maybe_cut(
            self.store.fail(
                lease.task,
                lease.attempt,
                lease.token,
                &json!({"type": "linearizability"}),
                Some(Duration::ZERO),
            ),
            cut,
        )
        .await;
        let ret = self.recorder.now();
        self.recorder.record(
            self.id,
            &lease.task.to_string(),
            call,
            ret,
            &json!({"op": "fail", "attempt": lease.attempt, "token": lease.token.to_string()}),
            &match outcome {
                Some(result) => {
                    let state = result.unwrap();
                    json!({
                        "ok": state.is_some(),
                        "state": state.map(|state| format!("{state:?}").to_lowercase()),
                    })
                }
                None => json!({"unknown": true}),
            },
        );
    }

    async fn renew(&self, lease: Lease, cut: Option<Duration>) {
        let call = self.recorder.now();
        let renewed = maybe_cut(
            self.store
                .renew_lease(lease.task, lease.attempt, lease.token, Duration::from_mins(10)),
            cut,
        )
        .await;
        let ret = self.recorder.now();
        self.recorder.record(
            self.id,
            &lease.task.to_string(),
            call,
            ret,
            &json!({"op": "renew", "attempt": lease.attempt, "token": lease.token.to_string()}),
            &match renewed {
                Some(result) => json!({"ok": result.unwrap()}),
                None => json!({"unknown": true}),
            },
        );
    }

    /// What a dead worker looks like to the database: the lease lapses and
    /// another pass reclaims it. Serialised, because the sweep is queue-wide and
    /// two overlapping expiries would make it ambiguous which task it reclaimed.
    async fn expire_and_recover(&self, lease: Lease) {
        let guard = self.recovery.lock().await;
        expire(&self.store, lease.task).await;
        let call = self.recorder.now();
        let recovered = self.store.recover_expired(&self.queue, 100).await.unwrap();
        let ret = self.recorder.now();
        drop(guard);

        self.recorder.record(
            self.id,
            &lease.task.to_string(),
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
                // Never cut a claim off: a claim whose result was lost minted a
                // lease token the client never saw, and no model can say what
                // state that left behind.
                self.claim().await;
                continue;
            }
            let Some(lease) = self.recorder.sample(&mut rng) else {
                continue;
            };
            let cut = (rng.below(100) < FAULT_PERCENT).then(|| cut_after(&mut rng));
            match choice {
                35..=57 => self.complete(lease, cut).await,
                58..=76 => self.fail(lease, cut).await,
                77..=89 => self.renew(lease, cut).await,
                _ => self.expire_and_recover(lease).await,
            }
        }
    }
}

async fn expire(store: &Store, task: TaskId) {
    sqlx::query(
        "UPDATE pgtask.tasks
         SET lease_expires_at = statement_timestamp() - interval '1 second'
         WHERE id = $1 AND state = 'running'",
    )
    .bind(task.as_uuid())
    .execute(store.pool())
    .await
    .unwrap();
}

async fn seed_tasks(store: &Store, queue: &QueueName, task_name: &TaskName, count: usize) -> Vec<TaskId> {
    let mut ids = Vec::new();
    for _ in 0..count {
        let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
        request.queue_name = queue.clone();
        request.max_attempts = MAX_ATTEMPTS;
        ids.push(store.enqueue(&request).await.unwrap().task_id);
    }
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_lease_protocol_is_linearizable() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let store = Arc::new(Store::connect(&database_url).await.unwrap());
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue = QueueName::new(format!("lin-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("lin-task-{suffix}")).unwrap();
    seed_tasks(&store, &queue, &task_name, TASKS).await;

    let recorder = Arc::new(Recorder::new());
    let recovery = Arc::new(tokio::sync::Mutex::new(()));

    let mut clients = Vec::new();
    for id in 0..CLIENTS {
        let client = LeaseClient {
            id,
            store: Arc::clone(&store),
            recorder: Arc::clone(&recorder),
            recovery: Arc::clone(&recovery),
            queue: queue.clone(),
            task_name: task_name.clone(),
        };
        clients.push(tokio::spawn(
            client.run(Rng::new(seed().wrapping_add(id as u64 * 0x9E37_79B9))),
        ));
    }
    for client in clients {
        client.await.unwrap();
    }

    let operations = recorder.take();
    let indeterminate = operations
        .iter()
        .filter(|operation| operation["output"]["unknown"] == json!(true))
        .count();
    assert!(
        indeterminate > 0,
        "no call was cut off mid-flight, so the crash case was never reached and this run \
         says nothing about indeterminate writes. Replay with \
         PGTASK_LINEARIZABILITY_SEED={}",
        seed()
    );
    println!("{indeterminate} call(s) cut off mid-flight");

    // Take one write the database rejected and claim it succeeded. That flip is
    // a stale lease being honoured, which is what fencing exists to prevent.
    check_and_prove_teeth("lease", "lease", &operations, |operation| {
        let op = operation["input"]["op"].as_str().unwrap_or_default();
        if matches!(op, "complete" | "renew") && operation["output"]["ok"] == json!(false) {
            operation["output"]["ok"] = json!(true);
        }
    });
}

// ------------------------------------------------------------- register -----

/// Writes a durable step and reads it back, from whichever lease this client
/// last held. Two attempts writing different values to the same step is the
/// replay case durable execution exists for: the second must be handed the
/// first's value.
struct RegisterClient {
    id: usize,
    store: Arc<Store>,
    recorder: Arc<Recorder>,
    recovery: Arc<tokio::sync::Mutex<()>>,
    queue: QueueName,
    task_name: TaskName,
    step: StepName,
    signal: SignalName,
}

impl RegisterClient {
    async fn claim(&self) {
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
        if let Some(task) = claimed.first() {
            self.recorder.remember(Lease {
                task: task.id,
                attempt: task.attempt,
                token: task.lease_token.unwrap(),
            });
        }
    }

    /// A durable step. The value is unique per write, so first-write-wins is
    /// observable: a second writer handed its own value back would be a bug.
    async fn write_checkpoint(&self, lease: Lease, round: usize, cut: Option<Duration>) {
        let value = json!({"client": self.id, "round": round});
        let call = self.recorder.now();
        let committed = maybe_cut(
            self.store
                .commit_checkpoint(lease.task, lease.attempt, lease.token, &self.step, 0, &value),
            cut,
        )
        .await;
        let ret = self.recorder.now();
        self.recorder.record(
            self.id,
            &format!("ckpt:{}", lease.task),
            call,
            ret,
            &json!({"op": "write", "value": value}),
            &match committed {
                Some(result) => match result.unwrap() {
                    Some(checkpoint) => json!({"ok": true, "value": checkpoint.value}),
                    None => json!({"ok": false}),
                },
                None => json!({"unknown": true}),
            },
        );
    }

    async fn read_checkpoint(&self, lease: Lease) {
        let call = self.recorder.now();
        let found = self
            .store
            .get_checkpoint(lease.task, HandlerVersion::default(), &self.step, 0)
            .await
            .unwrap();
        let ret = self.recorder.now();
        self.recorder.record(
            self.id,
            &format!("ckpt:{}", lease.task),
            call,
            ret,
            &json!({"op": "read"}),
            &match found {
                Some(checkpoint) => json!({"present": true, "value": checkpoint.value}),
                None => json!({"present": false}),
            },
        );
    }

    /// `emit_signal` is not fenced -- anyone may emit -- but it makes the same
    /// first-write-wins promise, so it is the same register.
    async fn emit(&self, lease: Lease, round: usize) {
        let value = json!({"client": self.id, "round": round});
        let call = self.recorder.now();
        let signal = self
            .store
            .emit_signal(lease.task, &self.signal, 0, &value)
            .await
            .unwrap();
        let ret = self.recorder.now();
        self.recorder.record(
            self.id,
            &format!("sig:{}", lease.task),
            call,
            ret,
            &json!({"op": "write", "value": value}),
            &json!({"ok": true, "value": signal.value}),
        );
    }

    async fn expire_and_recover(&self, lease: Lease) {
        let guard = self.recovery.lock().await;
        expire(&self.store, lease.task).await;
        let _ = self.store.recover_expired(&self.queue, 100).await.unwrap();
        drop(guard);
    }

    async fn run(self, mut rng: Rng) {
        for round in 0..STEPS_PER_CLIENT {
            let choice = rng.below(100);
            if choice < 25 {
                self.claim().await;
                continue;
            }
            let Some(lease) = self.recorder.sample(&mut rng) else {
                continue;
            };
            let cut = (rng.below(100) < FAULT_PERCENT).then(|| cut_after(&mut rng));
            match choice {
                25..=54 => self.write_checkpoint(lease, round, cut).await,
                55..=74 => self.read_checkpoint(lease).await,
                75..=89 => self.emit(lease, round).await,
                // Cycling the lease is what puts two different attempts on the
                // same step, which is the case worth checking.
                _ => self.expire_and_recover(lease).await,
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_steps_and_signals_are_linearizable() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let store = Arc::new(Store::connect(&database_url).await.unwrap());
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue = QueueName::new(format!("reg-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("reg-task-{suffix}")).unwrap();
    seed_tasks(&store, &queue, &task_name, TASKS).await;

    let recorder = Arc::new(Recorder::new());
    let recovery = Arc::new(tokio::sync::Mutex::new(()));

    let mut clients = Vec::new();
    for id in 0..CLIENTS {
        let client = RegisterClient {
            id,
            store: Arc::clone(&store),
            recorder: Arc::clone(&recorder),
            recovery: Arc::clone(&recovery),
            queue: queue.clone(),
            task_name: task_name.clone(),
            step: StepName::new("durable-step").unwrap(),
            signal: SignalName::new("go").unwrap(),
        };
        clients.push(tokio::spawn(
            client.run(Rng::new(seed().wrapping_add(id as u64 * 0x51ED_2A15))),
        ));
    }
    for client in clients {
        client.await.unwrap();
    }

    let operations = recorder.take();
    let indeterminate = operations
        .iter()
        .filter(|operation| operation["output"]["unknown"] == json!(true))
        .count();
    assert!(
        indeterminate > 0,
        "no durable write was cut off mid-flight, so the crash case was never reached. \
         Replay with PGTASK_LINEARIZABILITY_SEED={}",
        seed()
    );
    println!("{indeterminate} durable write(s) cut off mid-flight");

    // Change one recorded value. A register whose value differs between two
    // reports has no valid ordering, which is what makes a step's result
    // changing between replays detectable.
    check_and_prove_teeth("register", "register", &operations, |operation| {
        if operation["output"]["value"].is_object() {
            operation["output"]["value"] = json!({"client": 999, "round": 999});
        }
    });

    // A history where every register was written at most once would linearize
    // trivially, and would say nothing about first-write-wins. This is the
    // guard: at least one write has to have been handed back a value that was
    // not its own, which only happens when a second writer lost the race.
    let displaced = operations
        .iter()
        .filter(|operation| {
            operation["input"]["op"] == json!("write")
                && operation["output"]["ok"] == json!(true)
                && operation["output"]["value"] != operation["input"]["value"]
        })
        .count();
    assert!(
        displaced > 0,
        "no write was ever displaced by an earlier one, so first-write-wins was never \
         exercised and this history proves nothing. Replay with \
         PGTASK_LINEARIZABILITY_SEED={}",
        seed()
    );
    println!("first-write-wins exercised by {displaced} displaced write(s)");
}

// ---------------------------------------------------------- idempotency -----

/// Keys raced per run. Few, so every key is contended.
const KEYS: usize = 5;

/// Hammers a handful of idempotency keys from several clients at once.
///
/// `enqueue` resolves a key through an `INSERT ... ON CONFLICT DO UPDATE ...
/// WHERE <the reservation has expired>`, falling back to a separate `SELECT`
/// when that update matches nothing. Two callers racing on one key take
/// different paths through that, which a sequential test never reaches.
struct IdempotencyClient {
    id: usize,
    store: Arc<Store>,
    recorder: Arc<Recorder>,
    queue: QueueName,
    task_name: TaskName,
    keys: Vec<String>,
}

impl IdempotencyClient {
    async fn enqueue(&self, key: &str) {
        let mut request = EnqueueRequest::new(self.task_name.clone(), json!({}));
        request.queue_name = self.queue.clone();
        request.max_attempts = MAX_ATTEMPTS;
        request.idempotency_key = Some(key.to_owned());

        let call = self.recorder.now();
        let result = self.store.enqueue(&request).await;
        let ret = self.recorder.now();

        let output = match &result {
            Ok(enqueued) => json!({
                "ok": true,
                "task_id": enqueued.task_id.to_string(),
                "created": enqueued.created,
            }),
            // A rejected enqueue leaves no reservation, so the model treats it
            // as a no-op rather than guessing what it did.
            Err(_) => json!({"ok": false}),
        };
        self.recorder
            .record(self.id, key, call, ret, &json!({"op": "enqueue"}), &output);
    }

    async fn run(self, mut rng: Rng) {
        for _ in 0..STEPS_PER_CLIENT {
            let key = self.keys[rng.below(self.keys.len())].clone();
            self.enqueue(&key).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotent_enqueue_is_linearizable() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let store = Arc::new(Store::connect(&database_url).await.unwrap());
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue = QueueName::new(format!("idem-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("idem-task-{suffix}")).unwrap();
    let keys: Vec<String> = (0..KEYS).map(|index| format!("key-{suffix}-{index}")).collect();

    let recorder = Arc::new(Recorder::new());
    let mut clients = Vec::new();
    for id in 0..CLIENTS {
        let client = IdempotencyClient {
            id,
            store: Arc::clone(&store),
            recorder: Arc::clone(&recorder),
            queue: queue.clone(),
            task_name: task_name.clone(),
            keys: keys.clone(),
        };
        clients.push(tokio::spawn(
            client.run(Rng::new(seed().wrapping_add(id as u64 * 0x1DE3_7000))),
        ));
    }
    for client in clients {
        client.await.unwrap();
    }

    let operations = recorder.take();

    // Claim a second creation for a key. Two callers both told they created the
    // task means two tasks for one key, which is the duplicate the feature
    // exists to prevent, and no ordering explains it.
    check_and_prove_teeth("idempotency", "idempotency", &operations, |operation| {
        if operation["output"]["created"] == json!(false) {
            operation["output"]["created"] = json!(true);
        }
    });

    // A history where every key was enqueued once would linearize trivially and
    // say nothing. The guard: some caller has to have lost a race and been told
    // it did not create the task.
    let losers = operations
        .iter()
        .filter(|operation| operation["output"]["created"] == json!(false))
        .count();
    assert!(
        losers > 0,
        "no enqueue ever lost a race for its key, so deduplication was never exercised. \
         Replay with PGTASK_LINEARIZABILITY_SEED={}",
        seed()
    );
    println!("deduplication exercised by {losers} losing enqueue(s)");
}

// ------------------------------------------------------------- capacity -----

/// Fills and drains a capacity-limited queue from several clients at once.
///
/// Admission does not count rows: a running `capacity_outstanding_tasks` on the
/// queue row is maintained by trigger. Denormalised state can drift, and a
/// drifting counter is invisible in the tasks table because the tasks table is
/// not what admission consults.
struct CapacityClient {
    id: usize,
    store: Arc<Store>,
    recorder: Arc<Recorder>,
    queue: QueueName,
    task_name: TaskName,
    claimed: Arc<Mutex<Vec<(TaskId, u16, LeaseToken)>>>,
}

impl CapacityClient {
    async fn enqueue(&self) {
        let mut request = EnqueueRequest::new(self.task_name.clone(), json!({}));
        request.queue_name = self.queue.clone();
        request.max_attempts = MAX_ATTEMPTS;

        let call = self.recorder.now();
        let result = self.store.enqueue(&request).await;
        let ret = self.recorder.now();
        self.recorder.record(
            self.id,
            "queue",
            call,
            ret,
            &json!({"op": "enqueue"}),
            &json!({"ok": result.is_ok()}),
        );
    }

    /// Completing frees a slot, which is the only way the queue drains.
    async fn complete(&self) {
        let lease = {
            let mut claimed = self.claimed.lock().unwrap();
            claimed.pop()
        };
        let Some((task, attempt, token)) = lease else {
            // Nothing in hand, so pick something up first. Claiming does not
            // change the outstanding count, so it is not recorded.
            let picked = self
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
            if let Some(task) = picked.first() {
                self.claimed
                    .lock()
                    .unwrap()
                    .push((task.id, task.attempt, task.lease_token.unwrap()));
            }
            return;
        };

        let call = self.recorder.now();
        let ok = self
            .store
            .complete(task, attempt, token, Some(&json!({})))
            .await
            .unwrap();
        let ret = self.recorder.now();
        self.recorder.record(
            self.id,
            "queue",
            call,
            ret,
            &json!({"op": "complete"}),
            &json!({"ok": ok}),
        );
    }

    async fn run(self, mut rng: Rng) {
        for _ in 0..STEPS_PER_CLIENT {
            if rng.below(100) < 55 {
                self.enqueue().await;
            } else {
                self.complete().await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_admission_is_linearizable() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let store = Arc::new(Store::connect(&database_url).await.unwrap());
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4();
    let queue = QueueName::new(format!("cap-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("cap-task-{suffix}")).unwrap();

    let mut config = QueueConfig::new(queue.clone());
    config.max_outstanding_tasks = Some(std::num::NonZeroU64::new(CAPACITY).unwrap());
    store.put_queue(&config).await.unwrap();

    let recorder = Arc::new(Recorder::new());
    let claimed = Arc::new(Mutex::new(Vec::new()));

    let mut clients = Vec::new();
    for id in 0..CLIENTS {
        let client = CapacityClient {
            id,
            store: Arc::clone(&store),
            recorder: Arc::clone(&recorder),
            queue: queue.clone(),
            task_name: task_name.clone(),
            claimed: Arc::clone(&claimed),
        };
        clients.push(tokio::spawn(
            client.run(Rng::new(seed().wrapping_add(id as u64 * 0x0CAC_11E0_u64))),
        ));
    }
    for client in clients {
        client.await.unwrap();
    }

    let operations = recorder.take();

    // Claim an admission that was refused actually succeeded: that is one more
    // task outstanding than the limit allows, and no ordering explains it.
    check_and_prove_teeth("capacity", "capacity", &operations, |operation| {
        if operation["input"]["op"] == json!("enqueue") && operation["output"]["ok"] == json!(false) {
            operation["output"]["ok"] = json!(true);
        }
    });

    // If the queue never filled, admission control was never asked a question.
    let rejected = operations
        .iter()
        .filter(|operation| operation["input"]["op"] == json!("enqueue") && operation["output"]["ok"] == json!(false))
        .count();
    assert!(
        rejected > 0,
        "the queue never rejected an enqueue, so the limit was never reached and this \
         history proves nothing. Replay with PGTASK_LINEARIZABILITY_SEED={}",
        seed()
    );
    println!("admission control exercised by {rejected} rejection(s)");
}
