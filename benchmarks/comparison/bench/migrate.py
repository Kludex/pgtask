from __future__ import annotations

import asyncio

from pgtask import Client

from bench.config import DATABASE_URL


async def main() -> None:
    client = await Client.connect(DATABASE_URL)
    await client.migrate()


asyncio.run(main())
