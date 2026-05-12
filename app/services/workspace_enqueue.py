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


def enqueue_destination_snapshot_embed_job(workspace_id: int) -> bool:
    try:
        q = get_workspace_queue()
        q.enqueue(
            "app.workers.player_memory_worker.process_destination_snapshot_embed_job",
            workspace_id,
            job_timeout=600,
            retry=Retry(max=1, interval=[20]),
        )
        return True
    except Exception as exc:  # noqa: BLE001
        info("snapshot_embed_enqueue_failed", workspace_id=workspace_id, error=str(exc))
        return False


def enqueue_sql_player_sync_job(workspace_id: int) -> bool:
    try:
        q = get_workspace_queue()
        q.enqueue(
            "app.workers.player_memory_worker.process_sql_player_sync_job",
            workspace_id,
            job_timeout=900,
            retry=Retry(max=1, interval=[30]),
        )
        return True
    except Exception as exc:  # noqa: BLE001
        info("sql_sync_enqueue_failed", workspace_id=workspace_id, error=str(exc))
        return False


def enqueue_sql_player_sync_single_job(
    workspace_id: int,
    player_id: int,
    first_name: str,
    last_name: str,
) -> bool:
    try:
        q = get_workspace_queue()
        q.enqueue(
            "app.workers.player_memory_worker.process_sql_player_sync_single_job",
            workspace_id,
            player_id,
            first_name,
            last_name,
            job_timeout=900,
            retry=Retry(max=1, interval=[30]),
        )
        return True
    except Exception as exc:  # noqa: BLE001
        info("sql_sync_single_enqueue_failed", workspace_id=workspace_id, player_id=player_id, error=str(exc))
        return False


def enqueue_feedback_review_embed_job(agent_job_id: int) -> bool:
    try:
        q = get_workspace_queue()
        q.enqueue(
            "app.workers.player_memory_worker.process_feedback_review_embed_job",
            agent_job_id,
            job_timeout=300,
            retry=Retry(max=1, interval=[15]),
        )
        return True
    except Exception as exc:  # noqa: BLE001
        info("feedback_embed_enqueue_failed", agent_job_id=agent_job_id, error=str(exc))
        return False
