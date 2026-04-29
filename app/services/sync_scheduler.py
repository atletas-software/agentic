from __future__ import annotations

import asyncio
import os

from rq import Retry

from app.core.logger import error, info
from app.db import SessionLocal
from app.services.sheet_sync import get_due_user_ids_for_sync
from app.services.sync_queue import acquire_user_enqueue_lock, get_sync_queue


class SyncScheduler:
    def __init__(self) -> None:
        tick_value = os.getenv("SYNC_POLL_TICK_SECONDS") or os.getenv("SYNC_POLL_INTERVAL_SECONDS") or "30"
        self._tick_seconds = int(tick_value)
        self._enabled = os.getenv("SYNC_POLL_ENABLED", "true").lower() == "true"
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    def start(self) -> None:
        if not self._enabled:
            info("sync_scheduler_disabled")
            return
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())
        info("sync_scheduler_started", tick_seconds=self._tick_seconds)

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        await self._task
        info("sync_scheduler_stopped")

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.to_thread(self._run_once)
            except Exception as exc:  # noqa: BLE001
                error("sync_scheduler_iteration_failed", error=str(exc))
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._tick_seconds)
            except TimeoutError:
                continue

    def _run_once(self) -> None:
        db = SessionLocal()
        try:
            due_user_ids = get_due_user_ids_for_sync(db=db)
            queue = get_sync_queue()
            enqueued = 0
            for user_id in due_user_ids:
                if not acquire_user_enqueue_lock(user_id=user_id):
                    continue
                queue.enqueue(
                    "app.workers.sync_worker.process_user_sync_job",
                    user_id,
                    retry=Retry(max=3, interval=[30, 120, 300]),
                    job_timeout=600,
                )
                enqueued += 1
            info("sync_scheduler_iteration_complete", due_users=len(due_user_ids), enqueued=enqueued)
        finally:
            db.close()
