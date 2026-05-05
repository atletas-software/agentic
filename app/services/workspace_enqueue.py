from __future__ import annotations

from rq import Retry

from app.core.logger import info
from app.services.sync_queue import get_workspace_queue


def enqueue_workspace_context_refresh(user_id: str) -> bool:
    """Enqueue destination snapshot for workspace context. Returns False if enqueue failed."""
    try:
        q = get_workspace_queue()
        q.enqueue(
            "app.workers.workspace_worker.process_workspace_context_job",
            user_id,
            job_timeout=300,
            retry=Retry(max=2, interval=[10, 30]),
        )
        return True
    except Exception as exc:  # noqa: BLE001
        info("workspace_context_enqueue_failed", user_id=user_id, error=str(exc))
        return False


def enqueue_feedback_delegate_job(agent_job_id: int) -> bool:
    try:
        q = get_workspace_queue()
        q.enqueue(
            "app.workers.workspace_worker.process_feedback_delegate_job",
            agent_job_id,
            job_timeout=600,
            retry=Retry(max=1, interval=[30]),
        )
        return True
    except Exception as exc:  # noqa: BLE001
        info("feedback_delegate_enqueue_failed", agent_job_id=agent_job_id, error=str(exc))
        return False


def enqueue_video_processing_stub_job(agent_job_id: int) -> bool:
    try:
        q = get_workspace_queue()
        q.enqueue(
            "app.workers.workspace_worker.process_video_processing_stub_job",
            agent_job_id,
            job_timeout=120,
        )
        return True
    except Exception as exc:  # noqa: BLE001
        info("video_stub_enqueue_failed", agent_job_id=agent_job_id, error=str(exc))
        return False
