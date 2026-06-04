from __future__ import annotations

import json
import os
from typing import Any

import httpx

from backendapi.models.workspace import AgentJob
from backendapi.services.sync_queue import get_redis

_CANCEL_KEY = "workspace:agent_job_cancel:{}"


def set_agent_job_cancel_requested(agent_job_id: int) -> None:
    """Worker and HTTP handlers should treat this as a user-initiated stop."""
    ttl = int((os.getenv("AGENT_JOB_CANCEL_KEY_TTL_SECONDS") or "86400").strip() or "86400")
    get_redis().set(_CANCEL_KEY.format(agent_job_id), "1", ex=max(60, ttl))


def clear_agent_job_cancel_requested(agent_job_id: int) -> None:
    get_redis().delete(_CANCEL_KEY.format(agent_job_id))


def agent_job_cancel_requested(agent_job_id: int) -> bool:
    return bool(get_redis().get(_CANCEL_KEY.format(agent_job_id)))


def merge_agent_job_result_json(job: AgentJob, patch: dict[str, Any]) -> None:
    try:
        cur = json.loads(job.result_json or "{}")
    except json.JSONDecodeError:
        cur = {}
    cur.update(patch)
    job.result_json = json.dumps(cur, ensure_ascii=True)


def request_feedback_agent_cancel(review_id: str) -> None:
    """Ask the feedback agent to stop its background thread for this review (writes cancel_requested)."""
    rid = (review_id or "").strip()
    if not rid:
        return
    base = (os.getenv("FEEDBACK_AGENT_BASE_URL") or "").strip().rstrip("/")
    if not base:
        return
    try:
        httpx.post(f"{base}/api/reviews/{rid}/cancel", timeout=20.0)
    except Exception:  # noqa: BLE001
        pass
