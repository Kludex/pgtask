from __future__ import annotations

import asyncio

import redis
from pgtask import Task, TaskRegistry, Worker

from bench.config import CONCURRENCY, COUNTER_KEY, DATABASE_URL, REDIS_URL

tasks = TaskRegistry(queue_name="bench")
_counter = redis.Redis.from_url(REDIS_URL)


@tasks.task("bench.noop")
async def noop(task: Task, request: dict) -> dict:
    _counter.incr(COUNTER_KEY)
    return {}


async def main() -> None:
    await Worker(DATABASE_URL, tasks, concurrency=CONCURRENCY).run()


if __name__ == "__main__":
    asyncio.run(main())
