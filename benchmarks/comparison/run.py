"""Run the same workload through pgtask, Celery, and Dramatiq and report the spread.

Two phases are measured separately so neither hides the other.

Enqueue runs with no workers, so it measures only the cost of accepting work. Drain then starts the
workers and times from the first completion to the last, which keeps worker startup out of the
number: Celery takes seconds to boot and that is not a claim about its throughput.

Handlers increment one Redis counter, so completion is detected identically for every system.
"""

from __future__ import annotations

import json
import os
import signal
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass

import redis

from bench.config import CONCURRENCY, COUNTER_KEY, DATABASE_URL, REDIS_URL, TASKS, WORKERS

DRAIN_TIMEOUT = float(os.environ.get("BENCH_DRAIN_TIMEOUT", "300"))
REPETITIONS = int(os.environ.get("BENCH_REPETITIONS", "3"))


@dataclass
class Run:
    enqueue_seconds: float
    drain_seconds: float


@dataclass
class Result:
    system: str
    runs: list[Run]
    configuration: str

    @property
    def enqueue_rate(self) -> float:
        return TASKS / statistics.median(r.enqueue_seconds for r in self.runs)

    @property
    def drain_rate(self) -> float:
        return TASKS / statistics.median(r.drain_seconds for r in self.runs)


def counter() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL)


def spawn(command: list[str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        command,
        env=dict(os.environ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def stop(processes: list[subprocess.Popen[bytes]]) -> None:
    for process in processes:
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        except ProcessLookupError:
            continue
    for process in processes:
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            process.wait(timeout=10)


def enqueue(command: list[str]) -> float:
    started = time.perf_counter()
    completed = subprocess.run(command, capture_output=True, text=True, env=dict(os.environ), check=True)
    elapsed = time.perf_counter() - started
    for line in completed.stdout.splitlines():
        if line.startswith("{"):
            return float(json.loads(line)["enqueue_seconds"])
    return elapsed


def drain(client: redis.Redis) -> float:
    """Time from the first completion to the last, so worker startup is excluded."""
    deadline = time.perf_counter() + DRAIN_TIMEOUT
    first: float | None = None
    while time.perf_counter() < deadline:
        done = int(client.get(COUNTER_KEY) or 0)
        if first is None and done >= 1:
            first = time.perf_counter()
        if done >= TASKS:
            return time.perf_counter() - (first or time.perf_counter())
        time.sleep(0.005)
    raise TimeoutError(f"drained only {int(client.get(COUNTER_KEY) or 0)} of {TASKS}")


def measure(system: str, worker_command: list[str], worker_count: int, enqueue_command: list[str], label: str) -> Result:
    client = counter()
    runs: list[Run] = []
    for repetition in range(REPETITIONS):
        reset()
        client.delete(COUNTER_KEY)
        enqueue_seconds = enqueue(enqueue_command)
        workers = [spawn(worker_command) for _ in range(worker_count)]
        try:
            drain_seconds = drain(client)
        finally:
            stop(workers)
        runs.append(Run(enqueue_seconds, drain_seconds))
        print(
            f"  {system} run {repetition + 1}: enqueue {TASKS / enqueue_seconds:,.0f}/s, "
            f"drain {TASKS / drain_seconds:,.0f}/s"
        )
    return Result(system, runs, label)


def reset() -> None:
    """Each repetition starts from an empty queue so a backlog never carries over."""
    subprocess.run([sys.executable, "-m", "bench.reset"], check=True, stdout=subprocess.DEVNULL)


def main() -> None:
    print(f"tasks={TASKS} workers={WORKERS} concurrency={CONCURRENCY} repetitions={REPETITIONS}")
    print(f"database={DATABASE_URL.rsplit('@', 1)[-1]} redis={REDIS_URL}\n")
    subprocess.run([sys.executable, "-m", "bench.migrate"], check=True)

    results = [
        measure(
            "pgtask",
            [sys.executable, "-m", "bench.pgtask_worker"],
            WORKERS,
            [sys.executable, "-m", "bench.pgtask_producer"],
            f"{WORKERS} processes, concurrency {CONCURRENCY}",
        ),
        measure(
            "Celery",
            ["celery", "-A", "bench.celery_app", "worker", "-c", str(WORKERS), "--loglevel", "ERROR",
             "--without-gossip", "--without-mingle", "--without-heartbeat"],
            1,
            [sys.executable, "-m", "bench.celery_producer"],
            f"prefork, -c {WORKERS}",
        ),
        measure(
            "Dramatiq",
            ["dramatiq", "bench.dramatiq_app", "-p", str(WORKERS), "-t", "8", "--log-file", os.devnull],
            1,
            [sys.executable, "-m", "bench.dramatiq_producer"],
            f"-p {WORKERS} -t 8",
        ),
    ]

    width = max(len(r.system) for r in results)
    print(f"\n{'system'.ljust(width)}  {'enqueue/s':>10}  {'drain/s':>10}  configuration")
    for result in results:
        print(
            f"{result.system.ljust(width)}  {result.enqueue_rate:>10,.0f}  {result.drain_rate:>10,.0f}  "
            f"{result.configuration}"
        )

    with open("results.json", "w") as handle:
        json.dump(
            {
                "tasks": TASKS,
                "workers": WORKERS,
                "concurrency": CONCURRENCY,
                "repetitions": REPETITIONS,
                "results": [
                    {
                        "system": r.system,
                        "configuration": r.configuration,
                        "enqueue_per_second": round(r.enqueue_rate, 1),
                        "drain_per_second": round(r.drain_rate, 1),
                        "runs": [
                            {"enqueue_seconds": round(run.enqueue_seconds, 3), "drain_seconds": round(run.drain_seconds, 3)}
                            for run in r.runs
                        ],
                    }
                    for r in results
                ],
            },
            handle,
            indent=2,
        )


if __name__ == "__main__":
    main()
