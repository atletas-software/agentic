from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import httpx

from app.core.logger import error, info
from app.db import SessionLocal
from app.models.workspace import AgentJob
from app.services.workspace_service import (
    append_destination_snapshot_if_changed,
    build_destination_snapshot_payload,
    ensure_workspace,
)


def process_workspace_context_job(user_id: str) -> dict[str, str | bool]:
    """Refresh workspace from destination sheet (not source)."""
    db = SessionLocal()
    try:
        ws = ensure_workspace(user_id=user_id, db=db)
        payload = build_destination_snapshot_payload(user_id=user_id, db=db)
        if payload is None:
            info("workspace_context_skipped", user_id=user_id, reason="no_destination_or_user")
            return {"ok": True, "changed": False, "reason": "no_destination_or_user"}
        changed = append_destination_snapshot_if_changed(workspace_id=ws.id, payload=payload, db=db)
        info("workspace_context_refreshed", user_id=user_id, workspace_id=ws.id, changed=changed)
        return {"ok": True, "changed": changed}
    except Exception as exc:  # noqa: BLE001
        error("workspace_context_job_failed", user_id=user_id, error=str(exc))
        raise
    finally:
        db.close()


def process_video_processing_stub_job(agent_job_id: int) -> dict[str, str]:
    """Placeholder for future video pipeline (transcripts, embeddings)."""
    db = SessionLocal()
    try:
        job = db.get(AgentJob, agent_job_id)
        if job is None:
            return {"ok": False, "error": "job_not_found"}
        job.status = "RUNNING"
        job.started_at = datetime.now(UTC)
        db.commit()
        result = {
            "note": "VIDEO_PROCESSING is a stub. Implement ffmpeg/OpenAI pipeline or delegate to a service.",
            "workspace_id": job.workspace_id,
        }
        job.status = "SUCCESS"
        job.result_json = json.dumps(result)
        job.completed_at = datetime.now(UTC)
        db.commit()
        info("video_processing_stub_complete", agent_job_id=agent_job_id)
        return {"ok": True}
    except Exception as exc:  # noqa: BLE001
        error("video_processing_stub_failed", agent_job_id=agent_job_id, error=str(exc))
        raise
    finally:
        db.close()


def process_feedback_delegate_job(agent_job_id: int) -> dict[str, str | bool]:
    """
    Delegates to the Feedback Agent FastAPI service when FEEDBACK_AGENT_BASE_URL is set.
    POST {base}/api/reviews with video_url and optional fields from job payload.
    """
    db = SessionLocal()
    try:
        job = db.get(AgentJob, agent_job_id)
        if job is None:
            return {"ok": False, "error": "job_not_found"}
        base = (os.getenv("FEEDBACK_AGENT_BASE_URL") or "").rstrip("/")
        job.status = "RUNNING"
        job.started_at = datetime.now(UTC)
        db.commit()

        if not base:
            job.status = "SKIPPED"
            job.error_message = (
                "FEEDBACK_AGENT_BASE_URL is not set. Run the feedback agent "
                "(PYTHONPATH=. uvicorn agents.feedback.main:app --port 5055) or set the URL."
            )
            job.completed_at = datetime.now(UTC)
            db.commit()
            return {"ok": True, "delegated": False}

        payload = json.loads(job.payload_json or "{}")
        video_url = (payload.get("video_url") or "").strip()
        if not video_url:
            job.status = "FAILED"
            job.error_message = "video_url missing in job payload"
            job.completed_at = datetime.now(UTC)
            db.commit()
            return {"ok": False, "error": "video_url_required"}

        body = {
            "video_url": video_url,
            "player_focus": (payload.get("player_focus") or "").strip(),
            "sport": (payload.get("sport") or "Soccer").strip(),
            "analysis_scope": (payload.get("analysis_scope") or "").strip(),
            "coaching_focus": (payload.get("coaching_focus") or "").strip(),
        }
        url = f"{base}/api/reviews"
        timeout = float(os.getenv("FEEDBACK_AGENT_HTTP_TIMEOUT_SECONDS", "30"))
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(url, json=body)
        if resp.status_code >= 400:
            job.status = "FAILED"
            job.error_message = f"Feedback agent HTTP {resp.status_code}: {resp.text[:2000]}"
            job.completed_at = datetime.now(UTC)
            db.commit()
            return {"ok": False, "status_code": resp.status_code}

        data = resp.json()
        job.status = "SUCCESS"
        job.external_ref = str(data.get("id") or "")
        job.result_json = json.dumps(data, ensure_ascii=True)
        job.completed_at = datetime.now(UTC)
        db.commit()
        info("feedback_delegate_complete", agent_job_id=agent_job_id, external_ref=job.external_ref)
        return {"ok": True, "delegated": True, "review_id": job.external_ref}
    except Exception as exc:  # noqa: BLE001
        err = str(exc)
        error("feedback_delegate_job_failed", agent_job_id=agent_job_id, error=err)
        try:
            job = db.get(AgentJob, agent_job_id)
            if job is not None:
                job.status = "FAILED"
                job.error_message = err
                job.completed_at = datetime.now(UTC)
                db.commit()
        except Exception:  # noqa: BLE001
            pass
        raise
    finally:
        db.close()
