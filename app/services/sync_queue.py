from __future__ import annotations

import os

from redis import Redis
from rq import Queue

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SYNC_QUEUE_NAME = os.getenv("SYNC_QUEUE_NAME", "sheet-sync")


def get_redis() -> Redis:
    return Redis.from_url(REDIS_URL)


def get_sync_queue() -> Queue:
    return Queue(SYNC_QUEUE_NAME, connection=get_redis())


def acquire_user_enqueue_lock(user_id: str, ttl_seconds: int = 120) -> bool:
    key = f"sync:enqueue:lock:{user_id}"
    return bool(get_redis().set(key, "1", ex=ttl_seconds, nx=True))


def release_user_enqueue_lock(user_id: str) -> None:
    key = f"sync:enqueue:lock:{user_id}"
    get_redis().delete(key)
