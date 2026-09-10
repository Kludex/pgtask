//! Concurrency regression tests for the wait-registration protocol.
//!
//! A waiter reads the thing it is about to wait on, then registers the wait and
//! parks the task. Whatever wakes the waiter has to see that registration, so
//! the two transactions must serialise. These tests drive both sides
//! concurrently and assert a task never parks with its wake-up already gone.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use pgtask_core::{
    EnqueueRequest, HandlerVersion, QueueName, SignalName, StepName, Task, TaskId, TaskName, TaskState, WorkerId,
};
use pgtask_postgres::{ResultWait, ResultWaitRequest, SignalWait, SignalWaitRequest, SpawnRequest, Store, StoreConfig};
use serde_json::json;
use uuid::Uuid;

const PAIRS: usize = 400;
/// Pairs raced by the signal test, which runs in CI on every change. Serialising
/// is not probabilistic — a lock either conflicts or it does not — so this only
/// has to be large enough to notice. For comparison, the unserialised path parks
/// roughly nine in ten registrations.
const SIGNAL_PAIRS: usize = 120;
const TIMED_PAIRS: usize = 60;
/// Pairs in flight at once. Each pair holds two pooled connections.
const CONCURRENCY: usize = 12;
const POOL: u32 = 32;

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

async fn connect(database_url: String) -> Arc<Store> {
    let config = StoreConfig::new(database_url).with_query_connections(NonZeroU32::new(POOL).unwrap());
    let store = Arc::new(Store::connect_with_config(&config).await.unwrap());
    store.migrate().await.unwrap();
    store
}

fn request(task_name: &TaskName, queue_name: &QueueName) -> EnqueueRequest {
    let mut request = EnqueueRequest::new(task_name.clone(), json!({}));
    request.queue_name = queue_name.clone();
    request
}

async fn claim_n(store: &Store, queue_name: &QueueName, task_name: &TaskName, wanted: usize) -> Vec<Task> {
    let mut claimed = Vec::new();
    while claimed.len() < wanted {
        let batch = store
            .claim(
                queue_name,
                WorkerId::new(),
                &[(task_name.clone(), HandlerVersion::default())],
                100,
                Duration::from_mins(10),
            )
            .await
            .unwrap();
        assert!(!batch.is_empty(), "ran out of claimable tasks");
        claimed.extend(batch);
    }
    claimed
}

/// Spread the waker across the window the waiter's transaction occupies, so the
/// interleavings sweep the whole registration rather than always racing from
/// the same instant.
fn jitter(index: usize) -> Duration {
    Duration::from_micros((index as u64 * 17) % 1200)
}

/// A parent running its handler, and the child it spawned, both already claimed.
struct Pair {
    parent: Task,
    child: Task,
}

/// The step a parent spawns its child under. It must differ from the step the
/// parent then waits on: sharing one makes the spawn's own checkpoint satisfy
/// the wait, and the race is never run.
fn spawn_step() -> StepName {
    StepName::new("spawn-child").unwrap()
}

fn wait_step() -> StepName {
    StepName::new("await-child").unwrap()
}

/// Builds `count` parent/child pairs up front, so the raced calls are the only
/// work in flight once the test starts.
async fn build_pairs(store: &Store, queue: &QueueName, prefix: &str, count: usize) -> Vec<Pair> {
    let parent_name = TaskName::new(format!("{prefix}-parent")).unwrap();
    let child_name = TaskName::new(format!("{prefix}-child")).unwrap();

    for _ in 0..count {
        store.enqueue(&request(&parent_name, queue)).await.unwrap();
    }
    let parents = claim_n(store, queue, &parent_name, count).await;

    let mut spawned = Vec::new();
    for parent in parents {
        let child_id = store
            .spawn_task(SpawnRequest {
                parent_task_id: parent.id,
                parent_attempt: parent.attempt,
                parent_lease_token: parent.lease_token.unwrap(),
                step_name: &spawn_step(),
                occurrence: 0,
                task: &request(&child_name, queue),
            })
            .await
            .unwrap()
            .unwrap()
            .task_id;
        spawned.push((parent, child_id));
    }

    let children = claim_n(store, queue, &child_name, count).await;
    spawned
        .into_iter()
        .map(|(parent, child_id)| Pair {
            parent,
            child: children.iter().find(|task| task.id == child_id).unwrap().clone(),
        })
        .collect()
}

/// Registers a result wait and completes the child concurrently, returning what
/// the wait call reported.
async fn race_result_wait(
    store: &Arc<Store>,
    pair: &Pair,
    timeout: Option<Duration>,
    delay: Duration,
) -> Option<ResultWait> {
    let parent = pair.parent.clone();
    let child = pair.child.clone();
    let child_id = child.id;

    let waiter = {
        let store = Arc::clone(store);
        tokio::spawn(async move {
            store
                .wait_for_result(ResultWaitRequest {
                    task_id: parent.id,
                    attempt: parent.attempt,
                    lease_token: parent.lease_token.unwrap(),
                    step_name: &wait_step(),
                    occurrence: 0,
                    result_task_id: child_id,
                    timeout,
                })
                .await
                .unwrap()
        })
    };
    let completer = {
        let store = Arc::clone(store);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            store
                .complete(
                    child.id,
                    child.attempt,
                    child.lease_token.unwrap(),
                    Some(&json!({"ok": true})),
                )
                .await
                .unwrap()
        })
    };

    let outcome = waiter.await.unwrap();
    assert!(completer.await.unwrap(), "the child's completion must be accepted");
    outcome
}

/// A parent that waits for a child must be woken even when the child reaches a
/// terminal state while the parent is still registering the wait.
#[tokio::test]
async fn a_child_finishing_during_registration_still_wakes_its_parent() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = connect(database_url).await;
    let queue = QueueName::new(format!("race-result-{}", Uuid::new_v4())).unwrap();
    let pairs = build_pairs(&store, &queue, "race-result", PAIRS).await;

    let mut registered = 0_usize;
    let mut stuck: Vec<(TaskId, TaskId)> = Vec::new();

    for (chunk_index, chunk) in pairs.chunks(CONCURRENCY).enumerate() {
        let mut handles = Vec::new();
        for (offset, pair) in chunk.iter().enumerate() {
            let store = Arc::clone(&store);
            let parent_id = pair.parent.id;
            let child_id = pair.child.id;
            let pair = Pair {
                parent: pair.parent.clone(),
                child: pair.child.clone(),
            };
            let delay = jitter(chunk_index * CONCURRENCY + offset);
            handles.push(tokio::spawn(async move {
                let outcome = race_result_wait(&store, &pair, None, delay).await;
                (parent_id, child_id, outcome)
            }));
        }

        for handle in handles {
            let (parent_id, child_id, outcome) = handle.await.unwrap();
            if outcome != Some(ResultWait::Waiting) {
                continue;
            }
            registered += 1;
            // The wake-up is synchronous with the child's completion, and both
            // transactions have committed, so the parent's fate is decided.
            if store.get_task(parent_id).await.unwrap().unwrap().state == TaskState::Waiting {
                stuck.push((parent_id, child_id));
            }
        }
    }

    assert!(
        registered > 0,
        "no pair actually registered a wait, so the race was never exercised"
    );
    assert!(
        stuck.is_empty(),
        "{}/{registered} parents that registered a wait parked in `waiting` after their child had already \
         reached a terminal state. Nothing recovers this: recover_result_wait_timeouts only visits waits with \
         a timeout, and recover_expired only visits `running` tasks. First stuck pair: {:?}",
        stuck.len(),
        stuck.first()
    );
}

/// With a timeout the lost wake-up stops being a hang and becomes a wrong answer.
///
/// A parked parent is eventually picked up by the timeout sweep, which writes it
/// a `timeout` checkpoint and resumes it. But the child succeeded. The handler
/// therefore replays believing its child never finished.
#[tokio::test]
async fn a_timed_wait_never_reports_timeout_for_a_child_that_succeeded() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = connect(database_url).await;
    let queue = QueueName::new(format!("race-timeout-{}", Uuid::new_v4())).unwrap();
    let pairs = build_pairs(&store, &queue, "race-timeout", TIMED_PAIRS).await;
    let timeout = Duration::from_millis(500);

    for (index, pair) in pairs.iter().enumerate() {
        let outcome = race_result_wait(&store, pair, Some(timeout), jitter(index)).await;
        let expected = json!({"state": "succeeded", "result": {"ok": true}, "error": null});
        assert!(matches!(outcome, Some(ResultWait::Waiting | ResultWait::Ready(_))));
        let checkpoint = store
            .get_checkpoint(pair.parent.id, HandlerVersion::default(), &wait_step(), 0)
            .await
            .unwrap()
            .expect("child completion must save the result before returning");
        assert_eq!(checkpoint.value, expected);
        let parent = store.get_task(pair.parent.id).await.unwrap().unwrap();
        assert_eq!(
            parent.state,
            if outcome == Some(ResultWait::Waiting) {
                TaskState::Pending
            } else {
                TaskState::Running
            },
        );
    }
}

/// The same race for signals. `wait_for_signal` reads `pgtask.signals` before
/// registering, and `emit_signal` writes it.
///
/// This passes today, but only because `emit_signal`'s insert into
/// `pgtask.signals` needs a key-share lock on the task row for its foreign key,
/// and that conflicts with the waiter's `FOR UPDATE`. Nothing records that the
/// constraint is load-bearing, so this test is what would catch it being relaxed.
#[tokio::test]
async fn a_signal_emitted_during_registration_still_wakes_its_waiter() {
    let Some(database_url) = database_url() else {
        return;
    };
    let store = connect(database_url).await;

    let suffix = Uuid::new_v4();
    let queue = QueueName::new(format!("race-signal-{suffix}")).unwrap();
    let task_name = TaskName::new(format!("race-signal-task-{suffix}")).unwrap();
    let signal_name = SignalName::new("go").unwrap();

    for _ in 0..SIGNAL_PAIRS {
        store.enqueue(&request(&task_name, &queue)).await.unwrap();
    }
    let tasks = claim_n(&store, &queue, &task_name, SIGNAL_PAIRS).await;

    let mut registered = 0_usize;
    let mut stuck: Vec<TaskId> = Vec::new();

    for (chunk_index, chunk) in tasks.chunks(CONCURRENCY).enumerate() {
        let mut handles = Vec::new();
        for (offset, task) in chunk.iter().enumerate() {
            let task = task.clone();
            let store = Arc::clone(&store);
            let signal_name = signal_name.clone();
            let delay = jitter(chunk_index * CONCURRENCY + offset);
            handles.push(tokio::spawn(async move {
                let waiter = {
                    let store = Arc::clone(&store);
                    let signal_name = signal_name.clone();
                    let task = task.clone();
                    tokio::spawn(async move {
                        store
                            .wait_for_signal(SignalWaitRequest {
                                task_id: task.id,
                                attempt: task.attempt,
                                lease_token: task.lease_token.unwrap(),
                                step_name: &wait_step(),
                                occurrence: 0,
                                signal_name: &signal_name,
                                signal_occurrence: 0,
                                timeout: None,
                            })
                            .await
                            .unwrap()
                    })
                };
                let emitter = tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    store
                        .emit_signal(task.id, &signal_name, 0, &json!({"v": 1}))
                        .await
                        .unwrap()
                });
                let outcome = waiter.await.unwrap();
                emitter.await.unwrap();
                (task.id, outcome)
            }));
        }

        for handle in handles {
            let (task_id, outcome) = handle.await.unwrap();
            if outcome != Some(SignalWait::Waiting) {
                continue;
            }
            registered += 1;
            if store.get_task(task_id).await.unwrap().unwrap().state == TaskState::Waiting {
                stuck.push(task_id);
            }
        }
    }

    assert!(
        registered > 0,
        "no task actually registered a wait, so the race was never exercised"
    );
    assert!(
        stuck.is_empty(),
        "{}/{registered} tasks parked in `waiting` after their signal had already been emitted. \
         First stuck task: {:?}",
        stuck.len(),
        stuck.first()
    );
}
