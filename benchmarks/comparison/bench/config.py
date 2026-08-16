"""Shared benchmark configuration.

Every system runs the same workload: enqueue TASKS trivial tasks, then drain them with WORKERS
processes. Handlers increment one Redis counter so completion is detected identically everywhere.
"""

from __future__ import annotations

import os

TASKS = int(os.environ.get("BENCH_TASKS", "5000"))
WORKERS = int(os.environ.get("BENCH_WORKERS", "4"))
CONCURRENCY = int(os.environ.get("BENCH_CONCURRENCY", "25"))
REDIS_URL = os.environ.get("BENCH_REDIS_URL", "redis://127.0.0.1:6399/0")
DATABASE_URL = os.environ["PGTASK_DATABASE_URL"]
COUNTER_KEY = "bench:done"
