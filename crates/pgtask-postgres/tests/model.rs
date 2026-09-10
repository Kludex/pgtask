//! Stateful model test for the claim/lease/retry/recovery kernel.
//!
//! A reference model tracks what each task's state, attempt count and lease
//! ownership are supposed to be. `proptest` generates sequences of operations,
//! applies each to both the model and a real PostgreSQL database, and compares
//! them after every step. Anything they disagree about is a bug in one of them.
//!
//! The reason this is a `proptest` state machine rather than a plain seeded loop
//! is shrinking. A failure in a forty-step schedule is nearly unreadable; given
//! one, proptest replays shorter and simpler prefixes until it has the smallest
//! sequence that still fails, which is usually two or three steps and tells you
//! what broke on its own.
//!
//! Two things this is really looking for:
//!
//!   * fencing. Every claim mints a new attempt and lease token, and the
//!     previous pair is kept and periodically replayed. A stale write must never
//!     be accepted.
//!   * lease recovery. A running task whose lease expired must come back as
//!     `pending` while it still has attempts, and `failed` once it does not,
//!     without ever losing the task.
//!
//! Each case builds its own queue, so cases cannot see each other's rows. Give
//! this its own database all the same: it drives far more load than the rest of
//! the suite, and several existing tests assert on timeouts or notification
//! shards, which start failing when this runs alongside them.
//!
//!     `PGTASK_MODEL_CASES=64 cargo test -p pgtask-postgres --test model`

use std::{collections::HashMap, sync::OnceLock, time::Duration};

use pgtask_core::{EnqueueRequest, HandlerVersion, LeaseToken, QueueName, TaskId, TaskName, TaskState, WorkerId};
use pgtask_postgres::Store;
use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use serde_json::json;
use tokio::runtime::Runtime;
use uuid::Uuid;

const MAX_ATTEMPTS: u16 = 3;
/// Tasks a case may create. Small, so operations collide on the same rows.
const MAX_TASKS: usize = 4;

fn cases() -> u32 {
    std::env::var("PGTASK_MODEL_CASES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(24)
}

/// One runtime and one migrated store for the whole file. Building either per
/// case would dominate the run, and proptest replays cases many times while
/// shrinking.
fn store() -> Option<&'static (Runtime, Store)> {
    static STORE: OnceLock<Option<(Runtime, Store)>> = OnceLock::new();
    STORE
        .get_or_init(|| {
            let Ok(database_url) = std::env::var("PGTASK_DATABASE_URL") else {
                return None;
            };
            let runtime = Runtime::new().expect("failed to create the Tokio runtime");
            let store = runtime
                .block_on(Store::connect(&database_url))
                .expect("failed to connect to PGTASK_DATABASE_URL");
            runtime
                .block_on(store.migrate())
                .expect("failed to migrate the database");
            Some((runtime, store))
        })
        .as_ref()
}

/// Which lease to present: the live one, or one that has been superseded.
#[derive(Clone, Copy, Debug)]
enum Which {
    Live,
    Stale,
}

#[derive(Clone, Debug)]
enum Transition {
    Enqueue,
    Claim,
    Complete { task: usize, which: Which },
    Fail { task: usize, which: Which, retry: bool },
    ExpireAndRecover { task: usize },
    Cancel { task: usize },
}

/// What the model believes about one task.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelTask {
    state: TaskState,
    attempt: u16,
    /// Whether a superseded lease exists to replay.
    has_stale: bool,
}

impl ModelTask {
    fn claimable(&self) -> bool {
        self.state == TaskState::Pending && self.attempt < MAX_ATTEMPTS
    }
}

#[derive(Clone, Debug)]
struct Model {
    tasks: Vec<ModelTask>,
}

impl Model {
    fn indices_where(&self, predicate: impl Fn(&ModelTask) -> bool) -> Vec<usize> {
        self.tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| predicate(task))
            .map(|(index, _)| index)
            .collect()
    }
}

struct ModelMachine;

impl ReferenceStateMachine for ModelMachine {
    type State = Model;
    type Transition = Transition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(Model { tasks: Vec::new() }).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        let running = state.indices_where(|task| task.state == TaskState::Running);
        let pending = state.indices_where(|task| task.state == TaskState::Pending);

        let mut options: Vec<BoxedStrategy<Transition>> = Vec::new();
        if state.tasks.len() < MAX_TASKS {
            options.push(Just(Transition::Enqueue).boxed());
        }
        options.push(Just(Transition::Claim).boxed());
        if !running.is_empty() {
            let indices = running.clone();
            options.push(
                (proptest::sample::select(indices.clone()), any::<bool>())
                    .prop_map(|(task, stale)| Transition::Complete {
                        task,
                        which: if stale { Which::Stale } else { Which::Live },
                    })
                    .boxed(),
            );
            options.push(
                (proptest::sample::select(indices.clone()), any::<bool>(), any::<bool>())
                    .prop_map(|(task, stale, retry)| Transition::Fail {
                        task,
                        which: if stale { Which::Stale } else { Which::Live },
                        retry,
                    })
                    .boxed(),
            );
            options.push(
                proptest::sample::select(indices)
                    .prop_map(|task| Transition::ExpireAndRecover { task })
                    .boxed(),
            );
        }
        if !pending.is_empty() {
            options.push(
                proptest::sample::select(pending)
                    .prop_map(|task| Transition::Cancel { task })
                    .boxed(),
            );
        }
        proptest::strategy::Union::new(options).boxed()
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            Transition::Enqueue => state.tasks.len() < MAX_TASKS,
            Transition::Claim => true,
            Transition::Complete { task, which } | Transition::Fail { task, which, .. } => {
                state.tasks.get(*task).is_some_and(|entry| {
                    entry.state == TaskState::Running && (matches!(which, Which::Live) || entry.has_stale)
                })
            }
            Transition::ExpireAndRecover { task } => state
                .tasks
                .get(*task)
                .is_some_and(|entry| entry.state == TaskState::Running),
            Transition::Cancel { task } => state
                .tasks
                .get(*task)
                .is_some_and(|entry| entry.state == TaskState::Pending),
        }
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            Transition::Enqueue => state.tasks.push(ModelTask {
                state: TaskState::Pending,
                attempt: 0,
                has_stale: false,
            }),
            Transition::Claim => {
                // The lowest-numbered claimable task, which the descending
                // priorities assigned at enqueue make the database's choice too.
                if let Some(index) = state.indices_where(ModelTask::claimable).first().copied() {
                    let entry = &mut state.tasks[index];
                    entry.state = TaskState::Running;
                    entry.attempt += 1;
                    entry.has_stale = true;
                }
            }
            Transition::Complete { task, which } => {
                if matches!(which, Which::Live) {
                    state.tasks[*task].state = TaskState::Succeeded;
                }
            }
            Transition::Fail { task, which, retry } => {
                if matches!(which, Which::Live) {
                    let entry = &mut state.tasks[*task];
                    entry.state = if *retry && entry.attempt < MAX_ATTEMPTS {
                        TaskState::Pending
                    } else {
                        TaskState::Failed
                    };
                }
            }
            Transition::ExpireAndRecover { task } => {
                let entry = &mut state.tasks[*task];
                entry.state = if entry.attempt < MAX_ATTEMPTS {
                    TaskState::Pending
                } else {
                    TaskState::Failed
                };
            }
            Transition::Cancel { task } => state.tasks[*task].state = TaskState::Cancelled,
        }
        state
    }
}

/// The live system: a queue of its own, and the leases seen so far.
struct Sut {
    queue: QueueName,
    task_name: TaskName,
    ids: Vec<TaskId>,
    live: HashMap<usize, (u16, LeaseToken)>,
    stale: HashMap<usize, (u16, LeaseToken)>,
}

impl Sut {
    fn lease(&self, task: usize, which: Which) -> Option<(u16, LeaseToken)> {
        match which {
            Which::Live => self.live.get(&task).copied(),
            Which::Stale => self.stale.get(&task).copied(),
        }
    }

    /// The lease just used is now the superseded one, ready to be replayed.
    fn supersede(&mut self, task: usize) {
        if let Some(previous) = self.live.remove(&task) {
            self.stale.insert(task, previous);
        }
    }

    async fn enqueue(&mut self, store: &Store) {
        let mut request = EnqueueRequest::new(self.task_name.clone(), json!({}));
        request.queue_name = self.queue.clone();
        request.max_attempts = MAX_ATTEMPTS;
        // Descending priority by index, so `claim` -- which orders by priority
        // first -- always takes the lowest-numbered claimable task. Without this
        // the order depends on `run_at`, which both recovery and a retry reset,
        // and the model cannot predict which task the database will pick.
        request.priority = i16::try_from(MAX_TASKS - self.ids.len()).unwrap();
        self.ids.push(store.enqueue(&request).await.unwrap().task_id);
    }

    async fn claim(&mut self, store: &Store) {
        // The pre-state is read from the database rather than the model,
        // because `reference` is the state AFTER the transition. It also makes
        // this check independent of the model; whether the two agree is
        // check_invariants' job.
        let mut before = Vec::new();
        for id in &self.ids {
            let task = store.get_task(*id).await.unwrap().unwrap();
            before.push((task.state, task.attempt));
        }
        let claimable: Vec<usize> = before
            .iter()
            .enumerate()
            .filter(|(_, (state, attempt))| *state == TaskState::Pending && *attempt < MAX_ATTEMPTS)
            .map(|(index, _)| index)
            .collect();

        let claimed = store
            .claim(
                &self.queue,
                WorkerId::new(),
                &[(self.task_name.clone(), HandlerVersion::default())],
                1,
                Duration::from_mins(10),
            )
            .await
            .unwrap();
        match claimed.first() {
            Some(task) => {
                let index = self.ids.iter().position(|id| *id == task.id).expect("a known task");
                assert!(
                    claimable.contains(&index),
                    "claim returned #{index}, which was {:?} at attempt {} beforehand",
                    before[index].0,
                    before[index].1
                );
                assert_eq!(
                    task.attempt,
                    before[index].1 + 1,
                    "attempt drifted on claim for #{index}"
                );
                self.supersede(index);
                self.live.insert(index, (task.attempt, task.lease_token.unwrap()));
            }
            None => assert!(
                claimable.is_empty(),
                "claim returned nothing while {} task(s) were claimable",
                claimable.len()
            ),
        }
    }

    async fn complete(&mut self, store: &Store, task: usize, which: Which) {
        let Some((attempt, token)) = self.lease(task, which) else {
            return;
        };
        let accepted = store
            .complete(self.ids[task], attempt, token, Some(&json!({"ok": true})))
            .await
            .unwrap();
        match which {
            Which::Live => {
                assert!(accepted, "the live lease could not complete #{task}");
                self.supersede(task);
            }
            Which::Stale => assert!(!accepted, "a superseded lease completed #{task}: fencing failed"),
        }
    }

    async fn fail(&mut self, store: &Store, task: usize, which: Which, retry: bool, attempt_now: u16) {
        let Some((attempt, token)) = self.lease(task, which) else {
            return;
        };
        let outcome = store
            .fail(
                self.ids[task],
                attempt,
                token,
                &json!({"type": "model"}),
                retry.then_some(Duration::ZERO),
            )
            .await
            .unwrap();
        match which {
            Which::Live => {
                let expected = if retry && attempt_now < MAX_ATTEMPTS {
                    TaskState::Pending
                } else {
                    TaskState::Failed
                };
                assert_eq!(
                    outcome,
                    Some(expected),
                    "fail on #{task} at attempt {attempt_now}/{MAX_ATTEMPTS} returned the wrong state"
                );
                self.supersede(task);
            }
            Which::Stale => assert!(outcome.is_none(), "a superseded lease failed #{task}: fencing failed"),
        }
    }

    async fn expire_and_recover(&mut self, store: &Store, task: usize) {
        sqlx::query(
            "UPDATE pgtask.tasks
             SET lease_expires_at = statement_timestamp() - interval '1 second'
             WHERE id = $1",
        )
        .bind(self.ids[task].as_uuid())
        .execute(store.pool())
        .await
        .unwrap();
        let recovered = store.recover_expired(&self.queue, 100).await.unwrap();
        assert!(recovered >= 1, "recovery skipped the expired lease on #{task}");
        self.supersede(task);
    }
}

struct ModelTest;

impl StateMachineTest for ModelTest {
    type SystemUnderTest = Sut;
    type Reference = ModelMachine;

    fn init_test(_reference: &<Self::Reference as ReferenceStateMachine>::State) -> Self::SystemUnderTest {
        let suffix = Uuid::new_v4();
        Sut {
            queue: QueueName::new(format!("model-{suffix}")).unwrap(),
            task_name: TaskName::new(format!("model-task-{suffix}")).unwrap(),
            ids: Vec::new(),
            live: HashMap::new(),
            stale: HashMap::new(),
        }
    }

    fn apply(
        mut sut: Self::SystemUnderTest,
        reference: &<Self::Reference as ReferenceStateMachine>::State,
        transition: Transition,
    ) -> Self::SystemUnderTest {
        let Some((runtime, store)) = store() else { return sut };
        runtime.block_on(async {
            match transition {
                Transition::Enqueue => sut.enqueue(store).await,
                Transition::Claim => sut.claim(store).await,
                Transition::Complete { task, which } => sut.complete(store, task, which).await,
                Transition::Fail { task, which, retry } => {
                    sut.fail(store, task, which, retry, reference.tasks[task].attempt).await;
                }
                Transition::ExpireAndRecover { task } => sut.expire_and_recover(store, task).await,
                Transition::Cancel { task } => {
                    assert!(
                        store.cancel(sut.ids[task]).await.unwrap(),
                        "a pending task refused cancellation: #{task}"
                    );
                }
            }
        });
        sut
    }

    /// The whole model against the whole database, after every step.
    fn check_invariants(sut: &Self::SystemUnderTest, reference: &<Self::Reference as ReferenceStateMachine>::State) {
        let Some((runtime, store)) = store() else { return };

        runtime.block_on(async {
            for (index, expected) in reference.tasks.iter().enumerate() {
                let Some(id) = sut.ids.get(index) else { continue };
                let actual = store.get_task(*id).await.unwrap().unwrap();
                assert_eq!(actual.state, expected.state, "state diverged for #{index}");
                assert_eq!(actual.attempt, expected.attempt, "attempt diverged for #{index}");
                assert!(
                    actual.attempt <= MAX_ATTEMPTS,
                    "#{index} ran {} times with a budget of {MAX_ATTEMPTS}",
                    actual.attempt
                );
                // The table's own CHECK says a running task always holds a lease.
                assert_eq!(
                    actual.state == TaskState::Running,
                    actual.lease_token.is_some(),
                    "#{index} is {:?} but its lease is {:?}",
                    actual.state,
                    actual.lease_token
                );
            }
        });
    }
}

prop_state_machine! {
    #![proptest_config(ProptestConfig {
        cases: cases(),
        // Each step is a database round trip, so an unbounded shrink search
        // would take longer than the run that found the failure.
        max_shrink_iters: 512,
        ..ProptestConfig::default()
    })]

    #[test]
    fn the_database_agrees_with_the_model(sequential 1..30 => ModelTest);
}
