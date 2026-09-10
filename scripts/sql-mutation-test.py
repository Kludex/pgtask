#!/usr/bin/env python3
"""Mutation-test the PL/pgSQL kernel.

`cargo-mutants` reaches the Rust crates, but pgtask's state machine lives in
`0001_initial.sql`, so nothing was measuring how much of it the test suite
actually pins down.

Every rule in the kernel is a predicate inside a `SECURITY DEFINER` function.
That makes mutation cheap: migrate a fresh database normally, then
`CREATE OR REPLACE` one function with a single predicate changed. No Rust
recompilation is involved, and `sqlx` migrations are idempotent so the suite's
own `migrate()` call leaves the mutant in place.

A mutant that the suite still passes is a rule nobody is testing.

    ./scripts/sql-mutation-test.py                 # run every mutant
    ./scripts/sql-mutation-test.py --list          # show them
    ./scripts/sql-mutation-test.py -k fencing      # run a subset
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import pathlib
import shutil
import subprocess
import sys
import time

REPO = pathlib.Path(__file__).resolve().parent.parent
CONTAINER = os.environ.get("PGTASK_PG_CONTAINER", "pgtask-verify")
ADMIN_URL = os.environ.get("PGTASK_ADMIN_DB", "postgres")
MUTANT_DB = "pgtask_mutant"

# Parked for the duration of the run so the score reflects the suite that
# already existed, rather than the tests added alongside this harness. Set
# PGTASK_MUTANTS_KEEP_MODEL=1 to measure what they add.
#
# Regression tests for known-unfixed bugs do not need parking: they carry
# `#[ignore]` and so do not run by default.
EXCLUDED_TESTS: list[str] = []
if not os.environ.get("PGTASK_MUTANTS_KEEP_MODEL"):
    EXCLUDED_TESTS.append("crates/pgtask-postgres/tests/model.rs")


def parked_tests(only_target: str | None) -> list[str]:
    """Parking the target under --only-test would leave nothing to run."""
    if only_target is None:
        return EXCLUDED_TESTS
    return [path for path in EXCLUDED_TESTS if not path.endswith(f"/{only_target}.rs")]


@dataclasses.dataclass(frozen=True)
class Mutant:
    name: str
    signature: str
    old: str
    new: str
    rule: str
    """The guarantee this mutant breaks, in the words of the docs."""
    occurrences: int = 1
    """How many copies of `old` to replace. A predicate repeated across CTEs has
    to be removed from all of them, or the untouched copy still enforces the
    rule and the mutant survives for the wrong reason."""


MUTANTS: list[Mutant] = [
    # ---- fencing, removed wholesale ----
    #
    # Every claim increments the attempt AND mints a new lease token, so the
    # two checks are individually redundant: drop one and the other still
    # rejects a stale write. A mutant that removes only one can therefore
    # survive without proving anything about the tests. These remove the whole
    # fence, so only a test that actually replays a superseded lease kills them.
    Mutant(
        "fencing-complete-unfenced",
        "pgtask.complete_task(uuid, integer, uuid, jsonb)",
        "AND attempt = p_attempt AND lease_token = p_lease_token",
        "",
        "A stale worker cannot complete a task another worker claimed.",
    ),
    Mutant(
        "fencing-fail-unfenced",
        "pgtask.fail_task(uuid, integer, uuid, jsonb, bigint)",
        "AND attempt = p_attempt AND lease_token = p_lease_token",
        "",
        "A stale worker cannot fail a task another worker claimed.",
    ),
    # ---- fencing: only the current lease holder may write ----
    Mutant(
        "fencing-complete-ignores-lease-token",
        "pgtask.complete_task(uuid, integer, uuid, jsonb)",
        "AND lease_token = p_lease_token",
        "",
        "A stale worker cannot complete a task another worker claimed.",
    ),
    Mutant(
        "fencing-complete-ignores-attempt",
        "pgtask.complete_task(uuid, integer, uuid, jsonb)",
        "AND attempt = p_attempt",
        "",
        "A mutation applies only while the attempt number still matches.",
    ),
    Mutant(
        "fencing-complete-ignores-state",
        "pgtask.complete_task(uuid, integer, uuid, jsonb)",
        "AND state = 'running'",
        "",
        "A mutation applies only while the task is still running.",
    ),
    Mutant(
        "fencing-fail-ignores-lease-token",
        "pgtask.fail_task(uuid, integer, uuid, jsonb, bigint)",
        "AND lease_token = p_lease_token",
        "",
        "A stale worker cannot fail a task another worker claimed.",
    ),
    Mutant(
        "fencing-renew-ignores-lease-token",
        "pgtask.renew_leases(uuid[], integer[], uuid[], bigint)",
        "AND tasks.lease_token = requested.lease_token",
        "",
        "A stale worker cannot extend a lease it no longer holds.",
    ),
    Mutant(
        "fencing-suspend-ignores-lease-token",
        "pgtask.suspend_task(uuid, integer, uuid, text, integer, timestamptz, bigint)",
        "AND lease_token = p_lease_token",
        "",
        "Sleeping is a lease-owned transition.",
    ),
    Mutant(
        "fencing-wait-signal-ignores-lease-token",
        "pgtask.wait_for_signal(uuid, integer, uuid, text, integer, text, integer, bigint)",
        "AND lease_token = p_lease_token",
        "",
        "Registering a signal wait is a lease-owned transition.",
    ),
    Mutant(
        "fencing-wait-result-ignores-lease-token",
        "pgtask.wait_for_result(uuid, integer, uuid, text, integer, uuid, bigint)",
        "AND lease_token = p_lease_token",
        "",
        "Registering a result wait is a lease-owned transition.",
    ),
    Mutant(
        "fencing-spawn-ignores-lease-token",
        "pgtask.spawn_task(uuid, integer, uuid, text, integer, text, jsonb, text, integer, timestamptz, smallint, integer, jsonb)",
        "AND tasks.lease_token = p_parent_lease_token",
        "",
        "Spawning a child is a lease-owned transition.",
    ),
    # ---- cancellation ----
    Mutant(
        "cancel-renew-ignores-cancellation",
        "pgtask.renew_leases(uuid[], integer[], uuid[], bigint)",
        "AND tasks.cancel_requested_at IS NULL",
        "",
        "A heartbeat cancels the handler once cancellation is requested.",
    ),
    Mutant(
        "cancel-children-ignores-state",
        "pgtask.cancel_owned_children()",
        "AND state IN ('pending', 'running', 'waiting')",
        "",
        "A parent's terminal transition cancels its unfinished descendants.",
    ),
    # ---- attempt budget ----
    Mutant(
        "attempts-claim-ignores-budget",
        "pgtask.claim(text, uuid, text[], integer[], integer, bigint)",
        "AND tasks.attempt < tasks.max_attempts",
        "",
        "claim filters out tasks that have exhausted their attempts.",
        # Once in the starvation CTE and once in the priority CTE.
        occurrences=2,
    ),
    Mutant(
        "attempts-fail-off-by-one",
        "pgtask.fail_task(uuid, integer, uuid, jsonb, bigint)",
        "attempt < max_attempts",
        "attempt <= max_attempts",
        "A task retries only while attempts remain.",
    ),
    Mutant(
        "attempts-recover-off-by-one",
        "pgtask.recover_expired(text, integer)",
        "tasks.attempt < tasks.max_attempts",
        "tasks.attempt <= tasks.max_attempts",
        "Recovery fails a task when no attempts remain.",
    ),
    # ---- scheduling ----
    Mutant(
        "schedule-claim-ignores-run-at",
        "pgtask.claim(text, uuid, text[], integer[], integer, bigint)",
        "AND tasks.run_at <= statement_timestamp()\n            AND tasks.attempt",
        "AND tasks.attempt",
        "A task does not run before its run_at time.",
    ),
    Mutant(
        "schedule-claim-ignores-priority",
        "pgtask.claim(text, uuid, text[], integer[], integer, bigint)",
        "ORDER BY tasks.priority DESC, tasks.run_at, tasks.id",
        "ORDER BY tasks.priority ASC, tasks.run_at, tasks.id",
        "Higher priority is claimed first.",
    ),
    Mutant(
        "schedule-claim-ignores-paused-queue",
        "pgtask.claim(text, uuid, text[], integer[], integer, bigint)",
        "WHERE queues.name = p_queue_name AND queues.paused_at IS NULL",
        "WHERE queues.name = p_queue_name",
        "A paused queue hands out no work.",
    ),
    Mutant(
        "schedule-claim-ignores-capability",
        "pgtask.claim(text, uuid, text[], integer[], integer, bigint)",
        "WHERE handlers.task_name = tasks.task_name\n                    AND handlers.handler_version = tasks.handler_version",
        "WHERE handlers.task_name = tasks.task_name",
        "An unknown handler version waits instead of consuming an attempt.",
    ),
    # ---- lease recovery ----
    Mutant(
        "recovery-recovers-live-leases",
        "pgtask.recover_expired(text, integer)",
        "AND tasks.lease_expires_at <= statement_timestamp()",
        "",
        "Recovery only reclaims leases that have actually expired.",
    ),
    # ---- signals ----
    Mutant(
        "signal-last-write-wins",
        "pgtask.emit_signal(uuid, text, integer, jsonb)",
        "DO UPDATE SET value = signals.value",
        "DO UPDATE SET value = EXCLUDED.value",
        "The first committed signal payload wins.",
    ),
    # ---- admission control ----
    Mutant(
        "capacity-off-by-one",
        "pgtask.enforce_queue_capacity()",
        "AND capacity_outstanding_tasks < max_outstanding_tasks",
        "AND capacity_outstanding_tasks <= max_outstanding_tasks",
        "Admission above max_outstanding_tasks fails with PT001.",
    ),
    # ---- idempotency ----
    Mutant(
        "idempotency-expiry-ignored",
        "pgtask.enqueue(text, jsonb, text, integer, timestamptz, smallint, integer, text, jsonb)",
        "WHERE idempotency_keys.expires_at IS NOT NULL\n            AND idempotency_keys.expires_at <= statement_timestamp()",
        "WHERE true",
        "An active reservation keeps returning the original identifier.",
    ),
]


def psql(sql: str, database: str, capture: bool = True) -> str:
    result = subprocess.run(
        ["docker", "exec", "-i", "-e", "PGPASSWORD=pgtask", CONTAINER,
         "psql", "-U", "pgtask", "-d", database, "-v", "ON_ERROR_STOP=1", "-q", "-t", "-A"],
        input=sql, text=True, capture_output=capture,
    )
    if result.returncode != 0:
        raise RuntimeError(f"psql failed: {result.stderr}")
    return result.stdout


def reset_database() -> str:
    psql(
        f"DROP DATABASE IF EXISTS {MUTANT_DB} WITH (FORCE); CREATE DATABASE {MUTANT_DB};",
        ADMIN_URL,
    )
    url = f"postgresql://pgtask:pgtask@localhost:54329/{MUTANT_DB}"
    subprocess.run(
        ["cargo", "run", "-q", "-p", "pgtask-cli", "--bin", "pgtask", "--",
         "--database-url", url, "migrate"],
        cwd=REPO, check=True, capture_output=True, text=True,
    )
    return url


def apply_mutant(mutant: Mutant) -> None:
    definition = psql(
        f"SELECT pg_get_functiondef('{mutant.signature}'::regprocedure);", MUTANT_DB
    )
    if mutant.old not in definition:
        raise RuntimeError(
            f"mutant {mutant.name!r} does not apply: pattern not found in {mutant.signature}.\n"
            f"Pattern: {mutant.old!r}"
        )
    found = definition.count(mutant.old)
    if found < mutant.occurrences:
        raise RuntimeError(
            f"mutant {mutant.name!r} expects {mutant.occurrences} occurrence(s) of its "
            f"pattern in {mutant.signature}, found {found}"
        )
    mutated = definition.replace(mutant.old, mutant.new, mutant.occurrences)
    psql(mutated, MUTANT_DB)


# Tests whose timing budget is too tight to survive a loaded machine. Running
# 23 suites back to back is exactly that, and a flaky failure would be scored
# as a kill for a mutation it never actually detected.
FLAKY_TESTS = [
    # All three fail intermittently on an unmutated tree against a fresh
    # database, so leaving them in would score a flake as a kill for a mutation
    # nothing actually detected. See #25.
    "blocked_demand_sampling_does_not_delay_heartbeats_or_shutdown",
    "task_transitions_only_notify_their_deterministic_shards",
    "concurrent_workers_elect_one_queue_demand_sampler_per_interval",
]


class BuildFailure(RuntimeError):
    """The tree does not compile, so no mutant can be scored."""


def run_suite(url: str, timeout: int = 900, only_target: str | None = None) -> tuple[bool, str]:
    """Return (suite_passed, tail_of_output).

    A mutation is only "killed" when a test asserted something. A tree that
    fails to compile also makes cargo exit non-zero, which would score every
    mutant as killed and produce a meaningless 100%, so that is raised rather
    than counted.
    """
    env = {**os.environ, "PGTASK_DATABASE_URL": url}
    if only_target:
        # Answers "does this one test catch it?" rather than "does the suite?".
        command = ["cargo", "test", "-p", "pgtask-postgres", "--test", only_target, "--"]
    else:
        command = ["cargo", "test", "--workspace", "--all-features", "--"]
        for test in FLAKY_TESTS:
            command += ["--skip", test]
    result = subprocess.run(
        command, cwd=REPO, env=env, capture_output=True, text=True, timeout=timeout,
    )
    output = result.stdout + result.stderr
    if result.returncode != 0 and ("error: could not compile" in output or "error[E" in output):
        raise BuildFailure(output[-3000:])
    # A real kill always leaves a test-result line behind.
    if result.returncode != 0 and "test result: FAILED" not in output:
        raise BuildFailure(
            "cargo exited non-zero without a failing test; this is not a mutation kill:\n"
            + output[-3000:]
        )
    return result.returncode == 0, output[-2500:]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", action="store_true", help="list mutants and exit")
    parser.add_argument("-k", dest="filter", default="", help="only mutants whose name contains this")
    parser.add_argument(
        "--only-test",
        default=None,
        metavar="TARGET",
        help="run just this pgtask-postgres test target instead of the whole suite, "
        "to ask which mutants that one test catches",
    )
    args = parser.parse_args()

    selected = [m for m in MUTANTS if args.filter in m.name]
    if args.list:
        for mutant in selected:
            print(f"{mutant.name:45} {mutant.signature.split('(')[0]}")
        return 0

    # Park regression tests that are red on an unmutated tree; they would kill
    # every mutant regardless of the mutation.
    parked: list[tuple[pathlib.Path, pathlib.Path]] = []
    for relative in parked_tests(args.only_test):
        source = REPO / relative
        if source.exists():
            destination = source.with_suffix(".rs.parked")
            shutil.move(source, destination)
            parked.append((source, destination))

    killed: list[Mutant] = []
    survived: list[Mutant] = []
    inapplicable: list[tuple[Mutant, str]] = []

    try:
        print("Establishing the baseline (unmutated).")
        url = reset_database()
        try:
            baseline_passed, tail = run_suite(url, only_target=args.only_test)
        except BuildFailure as error:
            print(f"Baseline does not build:\n{error}")
            return 1
        if not baseline_passed:
            print("Baseline suite is RED. Mutation scores would be meaningless.\n")
            print(tail)
            return 1
        print("Baseline is green.\n")

        for index, mutant in enumerate(selected, start=1):
            print(f"[{index}/{len(selected)}] {mutant.name}", flush=True)
            started = time.monotonic()
            url = reset_database()
            try:
                apply_mutant(mutant)
            except RuntimeError as error:
                print(f"    SKIPPED - {error}\n", flush=True)
                inapplicable.append((mutant, str(error)))
                continue
            try:
                passed, tail = run_suite(url, only_target=args.only_test)
            except BuildFailure as error:
                print(f"    ABORTING - the tree stopped building mid-run:\n{error}")
                return 1
            elapsed = time.monotonic() - started
            if passed:
                survived.append(mutant)
                print(f"    SURVIVED in {elapsed:.0f}s - nothing tests: {mutant.rule}", flush=True)
            else:
                killed.append(mutant)
                failing = [line for line in tail.splitlines() if line.startswith("test ") and "FAILED" in line]
                first = failing[0].strip() if failing else "suite failed"
                print(f"    killed in {elapsed:.0f}s by {first}", flush=True)
    finally:
        for source, destination in parked:
            shutil.move(destination, source)

    total = len(killed) + len(survived)
    print("\n" + "=" * 72)
    print(f"Mutation score: {len(killed)}/{total} killed")
    if survived:
        print(f"\n{len(survived)} SURVIVING mutants - each is an untested rule:\n")
        for mutant in survived:
            print(f"  {mutant.name}")
            print(f"      function: {mutant.signature.split('(')[0]}")
            print(f"      unenforced: {mutant.rule}\n")
    if inapplicable:
        print(f"{len(inapplicable)} mutants could not be applied (pattern drift):")
        for mutant, _ in inapplicable:
            print(f"  {mutant.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
