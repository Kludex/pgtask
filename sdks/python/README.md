# `pgtask`

```python
from __future__ import annotations

import asyncio
import os

from pgtask import Client, EnqueueRequest


async def main() -> None:
    client = await Client.connect(os.environ["PGTASK_DATABASE_URL"])
    task = await client.enqueue(EnqueueRequest("reports.render", {"report_id": "report-123"}))
    print(await task.result(timeout=30.0))


asyncio.run(main())
```

This package provides the Python producer client and worker runtime for `pgtask`. See the
[Python SDK documentation](https://github.com/Kludex/pgtask/blob/main/docs/sdk/python.md) for workers, transactions,
durable execution, signals, cancellation, and OpenTelemetry propagation.
