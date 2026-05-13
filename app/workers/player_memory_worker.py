from __future__ import annotations

import json
import os
import time

import httpx

from app.core.logger import error, info
from app.db import SessionLocal
from app.models.workspace import AgentJob
from app.services.feedback_review_embed import embed_completed_feedback_review
from app.services.snapshot_embed import embed_destination_snapshot_changes
from app.services.sql_player_sync import sync_sql_context_for_workspace


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
        if job.status != "SUCCESS":
            return {"ok": False, "skipped": True, "reason": "job_not_success"}
        payload = json.loads(job.payload_json or "{}")
        player_key = str(payload.get("player_key") or "").strip()
        if not player_key:
            return {"ok": False, "skipped": True, "reason": "no_player_key"}
        review_id = str(job.external_ref or "").strip()
        if not review_id:
            return {"ok": False, "skipped": True, "reason": "no_review_id"}

        base = (os.getenv("FEEDBACK_AGENT_BASE_URL") or "").rstrip("/")
        if not base:
            return {"ok": False, "skipped": True, "reason": "no_feedback_agent"}

        timeout = float(os.getenv("FEEDBACK_AGENT_HTTP_TIMEOUT_SECONDS", "30"))
        review: dict | None = None
        last_status = 0
        for attempt in range(5):
            with httpx.Client(timeout=timeout) as client:
                resp = client.get(f"{base}/api/reviews/{review_id}")
            last_status = resp.status_code
            if resp.status_code == 404 and attempt < 4:
                time.sleep(2.0)
                continue
            if resp.status_code >= 400:
                return {"ok": False, "error": f"fetch_review_http_{resp.status_code}"}
            review = resp.json()
            break
        if review is None:
            return {"ok": False, "error": f"fetch_review_http_{last_status}"}
        if review.get("error"):
            return {"ok": False, "error": "review_response_error"}

        n = embed_completed_feedback_review(
            db=db,
            workspace_id=job.workspace_id,
            player_key=player_key,
            review=review,
            review_id=review_id,
        )
        info("feedback_review_embed_done", agent_job_id=agent_job_id, chunks=n)
        return {"ok": True, "chunks_written": n}
    except Exception as exc:  # noqa: BLE001
        error("feedback_review_embed_failed", agent_job_id=agent_job_id, error=str(exc))
        raise
    finally:
        db.close()
