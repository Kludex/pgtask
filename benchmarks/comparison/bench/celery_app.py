from __future__ import annotations

import redis
from celery import Celery

from bench.config import COUNTER_KEY, REDIS_URL

app = Celery("bench", broker=REDIS_URL)
app.conf.task_ignore_result = True
app.conf.worker_prefetch_multiplier = 4

_counter = redis.Redis.from_url(REDIS_URL)


@app.task(name="bench.noop")
def noop(n: int) -> None:
    _counter.incr(COUNTER_KEY)
