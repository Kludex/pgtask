from __future__ import annotations

import json
import time

from bench.celery_app import noop
from bench.config import TASKS

started = time.perf_counter()
for n in range(TASKS):
    noop.delay(n)
print(json.dumps({"enqueue_seconds": time.perf_counter() - started}))
