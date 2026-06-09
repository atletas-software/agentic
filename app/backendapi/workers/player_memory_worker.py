from __future__ import annotations

from backendapi.core.logger import error, info
from backendapi.db import SessionLocal
from backendapi.models.workspace import AgentJob
from backendapi.services.feedback_review_embed import embed_feedback_review_for_agent_job
from backendapi.services.snapshot_embed import embed_destination_snapshot_changes
from backendapi.services.sql_player_sync import sync_sql_context_for_workspace


def process_destination_snapshot_embed_job(workspace_id: int) -> dict[str, object]:
    db = SessionLocal()
    try:
        result = embed_destination_snapshot_changes(db=db, workspace_id=workspace_id)
        info("destination_snapshot_embed_done", workspace_id=workspace_id, **{k: result[k] for k in result if k != "ok"})
        return dict(result)
    except Exception as exc:  # noqa: BLE001
        error("destination_snapshot_embed_failed", workspace_id=workspace_id, error=str(exc))
        raise
    finally:
        db.close()


def process_sql_player_sync_job(workspace_id: int) -> dict[str, object]:
    db = SessionLocal()
    try:
        result = sync_sql_context_for_workspace(db=db, workspace_id=workspace_id)
        info("sql_player_sync_done", workspace_id=workspace_id, **{k: result[k] for k in result})
        return dict(result)
    except Exception as exc:  # noqa: BLE001
        error("sql_player_sync_failed", workspace_id=workspace_id, error=str(exc))
        raise
    finally:
        db.close()


def process_sql_player_sync_single_job(
    workspace_id: int,
    player_id: int,
    first_name: str,
    last_name: str,
) -> dict[str, object]:
    db = SessionLocal()
    try:
        result = sync_sql_context_for_workspace(
            db=db,
            workspace_id=workspace_id,
            single_player={
                "player_id": player_id,
                "first_name": first_name,
                "last_name": last_name,
            },
        )
        info(
            "sql_player_sync_single_done",
            workspace_id=workspace_id,
            player_id=player_id,
            **{k: result[k] for k in result},
        )
        return dict(result)
    except Exception as exc:  # noqa: BLE001
        error(
            "sql_player_sync_single_failed",
            workspace_id=workspace_id,
            player_id=player_id,
            error=str(exc),
        )
        raise
    finally:
        db.close()


def process_feedback_review_embed_job(agent_job_id: int) -> dict[str, object]:
    """Fetch completed review JSON from feedback agent and embed into vector memory."""
    db = SessionLocal()
    try:
        job = db.get(AgentJob, agent_job_id)
        if job is None:
            return {"ok": False, "error": "job_not_found"}
        result = embed_feedback_review_for_agent_job(db=db, job=job)
        if result.get("ok"):
            info(
                "feedback_review_embed_done",
                agent_job_id=agent_job_id,
                chunks=result.get("chunks_written"),
            )
        return result
    except Exception as exc:  # noqa: BLE001
        error("feedback_review_embed_failed", agent_job_id=agent_job_id, error=str(exc))
        raise
    finally:
        db.close()
