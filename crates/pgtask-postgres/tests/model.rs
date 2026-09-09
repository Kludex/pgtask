//! Stateful model test for the claim/lease/retry/recovery kernel.
//!
//! A reference model tracks what each task's state, attempt count and lease
//! ownership are supposed to be. A random schedule of operations is applied to
//! both the model and a real PostgreSQL database, and after every single step
//! the two are compared. Anything the model and the database disagree about is
//! a bug in one of them.
//!
//! The schedule comes from a seeded PRNG, so a failure is reproducible: the
//! panic message carries the exact command to replay it.
//!
//! Two things this is really looking for:
//!
//!   * fencing. Every claim mints a new attempt and lease token, and the
//!     previous pair is kept around and periodically replayed. A stale write
//!     must never be accepted.
//!   * lease recovery. A running task whose lease expired must come back as
//!     `pending` while it still has attempts, and `failed` once it does not,
//!     without ever losing the task.
//!
//!     `PGTASK_MODEL_SEED=12345 cargo test -p pgtask-postgres --test model`
//!     `PGTASK_MODEL_RUNS=200 cargo test -p pgtask-postgres --test model`
//!
//! Give this its own database. It drives far more load than the rest of the
//! suite, and several existing tests assert on timeouts or on notification
//! shards, which start failing when this runs against the same database at the
//! same time.

use std::{collections::BTreeMap, fmt::Write as _, time::Duration};

use pgtask_core::{EnqueueRequest, HandlerVersion, LeaseToken, QueueName, TaskId, TaskName, TaskState, WorkerId};
use pgtask_postgres::Store;
use serde_json::json;
use uuid::Uuid;

const MAX_ATTEMPTS: u16 = 3;

fn env_var<T: std::str::FromStr>(name: &str, fallback: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

/// xorshift64*: tiny, and the seed alone fixes the whole schedule.
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
            return 0;
        }
        usize::try_from(self.next_u64() % bound as u64).expect("a value below bound fits usize")
    }
}

/// What the model believes about one task.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelTask {
    state: TaskState,
    attempt: u16,
    /// The lease the model believes is live, if the task is running.
    live_lease: Option<(u16, LeaseToken)>,
    /// A lease that has been superseded. Replaying it must always be rejected.
    stale_lease: Option<(u16, LeaseToken)>,
}

impl ModelTask {
    fn claimable(&self) -> bool {
        self.state == TaskState::Pending && self.attempt < MAX_ATTEMPTS
    }
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Enqueue,
    Claim,
    Complete { stale: bool },
    FailWithRetry { stale: bool },
    FailTerminally,
    ExpireAndRecover,
    Cancel,
}

fn choose_op(rng: &mut Rng) -> Op {
    match rng.below(100) {
        0..=17 => Op::Enqueue,
        18..=43 => Op::Claim,
        44..=57 => Op::Complete { stale: false },
        58..=64 => Op::Complete { stale: true },
        65..=76 => Op::FailWithRetry { stale: false },
        77..=81 => Op::FailWithRetry { stale: true },
        82..=86 => Op::FailTerminally,
        87..=96 => Op::ExpireAndRecover,
        _ => Op::Cancel,
    }
}

struct World {
    store: Store,
    queue: QueueName,
    task_name: TaskName,
    ids: Vec<TaskId>,
    model: Vec<ModelTask>,
    history: Vec<String>,
}

impl World {
    fn new(store: Store, seed: u64, run: usize) -> Self {
        let suffix = Uuid::new_v4();
        let queue = QueueName::new(format!("model-{suffix}")).unwrap();
        let task_name = TaskName::new(format!("model-task-{suffix}")).unwrap();
        Self {
            store,
            queue,
            task_name,
            ids: Vec::new(),
            model: Vec::new(),
            history: vec![format!("seed={seed} run={run}")],
        }
    }

    fn note(&mut self, line: String) {
        self.history.push(line);
    }

    fn report(&self) -> String {
        let mut out = String::new();
        for line in &self.history {
            let _ = writeln!(out, "  {line}");
        }
        out
    }

    /// Indices of tasks the model says are in a given state.
    fn indices_where(&self, predicate: impl Fn(&ModelTask) -> bool) -> Vec<usize> {
        self.model
            .iter()
            .enumerate()
            .filter(|(_, t)| predicate(t))
            .map(|(i, _)| i)
            .collect()
    }

    async fn enqueue(&mut self) {
        let mut request = EnqueueRequest::new(self.task_name.clone(), json!({}));
        request.queue_name = self.queue.clone();
        request.max_attempts = MAX_ATTEMPTS;
        let id = self.store.enqueue(&request).await.unwrap().task_id;
        self.ids.push(id);
        self.model.push(ModelTask {
            state: TaskState::Pending,
            attempt: 0,
            live_lease: None,
            stale_lease: None,
        });
        self.note(format!("enqueue -> #{}", self.model.len() - 1));
    }

    async fn claim(&mut self) {
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

        let claimable = self.indices_where(ModelTask::claimable);
        let Some(task) = claimed.first() else {
            assert!(
                claimable.is_empty(),
                "claim returned nothing while the model had {} claimable task(s)\n{}",
                claimable.len(),
                self.report()
            );
            self.note("claim -> nothing".to_owned());
            return;
        };

        let index = self
            .ids
            .iter()
            .position(|id| *id == task.id)
            .expect("claimed an unknown task");
        assert!(
            claimable.contains(&index),
            "claim returned #{index}, which the model says is {:?} at attempt {}\n{}",
            self.model[index].state,
            self.model[index].attempt,
            self.report()
        );

        let model_attempt = {
            let entry = &mut self.model[index];
            entry.state = TaskState::Running;
            entry.attempt += 1;
            // The lease being replaced becomes the stale one to replay later.
            entry.stale_lease = entry.live_lease.take().or(entry.stale_lease);
            entry.live_lease = Some((task.attempt, task.lease_token.unwrap()));
            entry.attempt
        };
        assert_eq!(
            task.attempt,
            model_attempt,
            "attempt drifted on claim\n{}",
            self.report()
        );
        self.note(format!("claim -> #{index} attempt={model_attempt}"));
    }

    /// Picks the lease to present: the live one, or a superseded one when
    /// `stale` is set. Returns None when there is nothing suitable.
    fn lease_for(&self, index: usize, stale: bool) -> Option<(u16, LeaseToken)> {
        let entry = &self.model[index];
        if stale { entry.stale_lease } else { entry.live_lease }
    }

    async fn complete(&mut self, rng: &mut Rng, stale: bool) {
        let candidates = self.indices_where(|t| t.state == TaskState::Running);
        if candidates.is_empty() {
            return;
        }
        let index = candidates[rng.below(candidates.len())];
        let Some((attempt, token)) = self.lease_for(index, stale) else {
            return;
        };
        let accepted = self
            .store
            .complete(self.ids[index], attempt, token, Some(&json!({"ok": true})))
            .await
            .unwrap();

        if stale {
            assert!(
                !accepted,
                "a superseded lease completed #{index}: fencing failed\n{}",
                self.report()
            );
            self.note(format!("complete #{index} STALE -> rejected"));
        } else {
            assert!(
                accepted,
                "the live lease could not complete #{index}\n{}",
                self.report()
            );
            let entry = &mut self.model[index];
            entry.state = TaskState::Succeeded;
            entry.stale_lease = entry.live_lease.take();
            self.note(format!("complete #{index} -> succeeded"));
        }
    }

    async fn fail(&mut self, rng: &mut Rng, stale: bool, retry: bool) {
        let candidates = self.indices_where(|t| t.state == TaskState::Running);
        if candidates.is_empty() {
            return;
        }
        let index = candidates[rng.below(candidates.len())];
        let Some((attempt, token)) = self.lease_for(index, stale) else {
            return;
        };
        let retry_after = retry.then(|| Duration::from_millis(0));
        let outcome = self
            .store
            .fail(self.ids[index], attempt, token, &json!({"type": "model"}), retry_after)
            .await
            .unwrap();

        if stale {
            assert!(
                outcome.is_none(),
                "a superseded lease failed #{index}: fencing failed\n{}",
                self.report()
            );
            self.note(format!("fail #{index} STALE -> rejected"));
            return;
        }

        let attempt_before = self.model[index].attempt;
        // fail_task retries only while attempts remain, otherwise it is terminal.
        let expected = if retry && attempt_before < MAX_ATTEMPTS {
            TaskState::Pending
        } else {
            TaskState::Failed
        };
        assert_eq!(
            outcome,
            Some(expected),
            "fail #{index} at attempt {attempt_before}/{MAX_ATTEMPTS} returned the wrong state\n{}",
            self.report()
        );
        let entry = &mut self.model[index];
        entry.state = expected;
        entry.stale_lease = entry.live_lease.take();
        self.note(format!("fail #{index} retry={retry} -> {expected:?}"));
    }

    /// Expires a lease outright and runs the recovery sweep, which is what
    /// happens when a worker dies mid-handler.
    async fn expire_and_recover(&mut self, rng: &mut Rng) {
        let candidates = self.indices_where(|t| t.state == TaskState::Running);
        if candidates.is_empty() {
            return;
        }
        let index = candidates[rng.below(candidates.len())];
        sqlx::query(
            "UPDATE pgtask.tasks SET lease_expires_at = statement_timestamp() - interval '1 second'
             WHERE id = $1",
        )
        .bind(self.ids[index].as_uuid())
        .execute(self.store.pool())
        .await
        .unwrap();

        let recovered = self.store.recover_expired(&self.queue, 100).await.unwrap();
        assert!(
            recovered >= 1,
            "recovery skipped an expired lease on #{index}\n{}",
            self.report()
        );
        // The task whose lease was expired must specifically have been reclaimed.
        // Without this, the reconciliation loop below would silently skip it if
        // recovery had left it running.
        assert_ne!(
            self.store.get_task(self.ids[index]).await.unwrap().unwrap().state,
            TaskState::Running,
            "recovery left #{index} running on an expired lease\n{}",
            self.report()
        );

        // Recovery may sweep other tasks expired earlier in this run too, so
        // reconcile every running task the model knows about.
        for other in self.indices_where(|t| t.state == TaskState::Running) {
            let actual = self.store.get_task(self.ids[other]).await.unwrap().unwrap();
            if actual.state == TaskState::Running {
                continue;
            }
            let attempt_before = self.model[other].attempt;
            let expected = if attempt_before < MAX_ATTEMPTS {
                TaskState::Pending
            } else {
                TaskState::Failed
            };
            assert_eq!(
                actual.state,
                expected,
                "recovery put #{other} at attempt {attempt_before}/{MAX_ATTEMPTS} into the wrong state\n{}",
                self.report()
            );
            let entry = &mut self.model[other];
            entry.state = expected;
            entry.stale_lease = entry.live_lease.take();
        }
        self.note(format!("expire+recover #{index} -> {:?}", self.model[index].state));
    }

    async fn cancel(&mut self, rng: &mut Rng) {
        let candidates = self.indices_where(|t| t.state == TaskState::Pending);
        if candidates.is_empty() {
            return;
        }
        let index = candidates[rng.below(candidates.len())];
        let cancelled = self.store.cancel(self.ids[index]).await.unwrap();
        assert!(
            cancelled,
            "a pending task refused cancellation: #{index}\n{}",
            self.report()
        );
        self.model[index].state = TaskState::Cancelled;
        self.note(format!("cancel #{index}"));
    }

    /// The whole model against the whole database, after every step.
    async fn check(&self) {
        for (index, expected) in self.model.iter().enumerate() {
            let actual = self.store.get_task(self.ids[index]).await.unwrap().unwrap();
            assert_eq!(
                actual.state,
                expected.state,
                "state diverged for #{index}\n{}",
                self.report()
            );
            assert_eq!(
                actual.attempt,
                expected.attempt,
                "attempt diverged for #{index}\n{}",
                self.report()
            );
            assert!(
                actual.attempt <= MAX_ATTEMPTS,
                "#{index} ran {} times with a budget of {MAX_ATTEMPTS}\n{}",
                actual.attempt,
                self.report()
            );
            // The table's own CHECK says a running task always holds a lease.
            assert_eq!(
                actual.state == TaskState::Running,
                actual.lease_token.is_some(),
                "#{index} is {:?} but its lease is {:?}\n{}",
                actual.state,
                actual.lease_token,
                self.report()
            );
        }
    }

    /// No task ever silently disappears, and terminal counts only grow.
    fn census(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for task in &self.model {
            *counts.entry(format!("{:?}", task.state)).or_insert(0) += 1;
        }
        counts
    }
}

#[tokio::test]
async fn the_database_agrees_with_the_model() {
    let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
        return;
    };
    let store = Store::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let seed: u64 = env_var("PGTASK_MODEL_SEED", 0x5EED_0000_0000_0001);
    let runs: usize = env_var("PGTASK_MODEL_RUNS", 12);
    let steps: usize = env_var("PGTASK_MODEL_STEPS", 40);

    for run in 0..runs {
        // Each run gets its own derived seed so a single run can be replayed.
        let run_seed = seed.wrapping_add((run as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let mut rng = Rng::new(run_seed);
        let mut world = World::new(store.clone(), run_seed, run);

        for _ in 0..steps {
            match choose_op(&mut rng) {
                Op::Enqueue => world.enqueue().await,
                Op::Claim => world.claim().await,
                Op::Complete { stale } => world.complete(&mut rng, stale).await,
                Op::FailWithRetry { stale } => world.fail(&mut rng, stale, true).await,
                Op::FailTerminally => world.fail(&mut rng, false, false).await,
                Op::ExpireAndRecover => world.expire_and_recover(&mut rng).await,
                Op::Cancel => world.cancel(&mut rng).await,
            }
            world.check().await;
        }

        let census = world.census();
        assert_eq!(
            census.values().sum::<usize>(),
            world.model.len(),
            "a task went missing in run {run}; replay with PGTASK_MODEL_SEED={run_seed}"
        );
    }
}
