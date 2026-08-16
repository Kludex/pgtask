from __future__ import annotations

import json
import time

from bench.config import TASKS
from bench.dramatiq_app import noop

started = time.perf_counter()
for n in range(TASKS):
    noop.send(n)
print(json.dumps({"enqueue_seconds": time.perf_counter() - started}))
