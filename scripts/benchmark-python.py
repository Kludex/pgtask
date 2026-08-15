from __future__ import annotations

import asyncio
import json
import os
import sys
import time

from pgtask import Client, Task, TaskRegistry, Worker


async def run() -> None:
    database_url = os.environ["PGTASK_DATABASE_URL"]
    task_count = int(os.getenv("PGTASK_BENCH_TASKS", "1000"))
    concurrency = int(os.getenv("PGTASK_BENCH_CONCURRENCY", "100"))
    if task_count <= 0 or concurrency <= 0:
        raise ValueError("task count and concurrency must be positive")
    client = await Client.connect(database_url)
    await client.migrate()
    tasks = TaskRegistry(queue_name=f"python-bench-{os.urandom(8).hex()}")
    completed = 0
    completion = asyncio.Event()

    @tasks.task("benchmark.python-noop")
    async def noop(task: Task, payload: dict[str, int]) -> None:
        nonlocal completed
        assert task.id
        assert payload["sequence"] >= 0
        completed += 1
        if completed == task_count:
            completion.set()

    handles = [await client.enqueue(noop.request({"sequence": sequence})) for sequence in range(task_count)]
    worker = Worker(database_url, tasks, concurrency=concurrency)
    running = asyncio.create_task(worker.run())
    started = time.perf_counter()
    try:
        await completion.wait()
        while True:
            results = [await handle.inspect() for handle in handles]
            if all(result is not None and result.state == "succeeded" for result in results):
                break
            await asyncio.sleep(0.01)
    finally:
        worker.shutdown()
        await running
    elapsed = time.perf_counter() - started
    report = {
        "scenario": "python-noop",
        "tasks": task_count,
        "concurrency_per_worker": concurrency,
        "drain_seconds": elapsed,
        "drain_tasks_per_second": task_count / elapsed,
    }
    sys.stdout.write(f"{json.dumps(report, indent=2)}\n")


if __name__ == "__main__":
    asyncio.run(run())
