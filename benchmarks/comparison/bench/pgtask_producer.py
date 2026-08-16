from __future__ import annotations

import asyncio
import json
import sys
import time

from pgtask import Client

from bench.config import DATABASE_URL, TASKS
from bench.pgtask_worker import noop


async def main() -> None:
    client = await Client.connect(DATABASE_URL)
    await client.migrate()
    started = time.perf_counter()
    for n in range(TASKS):
        await client.enqueue(noop.request({"n": n}))
    print(json.dumps({"enqueue_seconds": time.perf_counter() - started}))
    sys.stdout.flush()


asyncio.run(main())
