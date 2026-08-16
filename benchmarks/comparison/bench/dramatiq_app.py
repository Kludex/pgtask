from __future__ import annotations

import dramatiq
import redis
from dramatiq.brokers.redis import RedisBroker

from bench.config import COUNTER_KEY, REDIS_URL

dramatiq.set_broker(RedisBroker(url=REDIS_URL))

_counter = redis.Redis.from_url(REDIS_URL)


@dramatiq.actor(max_retries=0)
def noop(n: int) -> None:
    _counter.incr(COUNTER_KEY)
