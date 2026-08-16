"""Clear queued state between repetitions so a backlog never carries over."""

from __future__ import annotations

import asyncio

import psycopg
import redis

from bench.config import DATABASE_URL, REDIS_URL


async def main() -> None:
    async with await psycopg.AsyncConnection.connect(DATABASE_URL, autocommit=True) as connection:
        await connection.execute("TRUNCATE pgtask.tasks CASCADE")
    client = redis.Redis.from_url(REDIS_URL)
    client.flushdb()


asyncio.run(main())
