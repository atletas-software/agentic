from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

import httpx

from app.core.logger import error, info
from app.db import SessionLocal
from app.models.workspace import AgentJob
from app.services.feedback_memory import retrieve_player_memory_context
from app.services.sheet_sync import SYNC_DESTINATION_HEADERS
from app.services.shared_feedback_context_sheet import fetch_shared_feedback_context_text
from app.services.player_directory import resolve_player_key_by_name
from app.services.workspace_enqueue import (
    enqueue_destination_snapshot_embed_job,
    enqueue_feedback_review_embed_job,
)
from app.services.workspace_service import (
    append_destination_snapshot_if_changed,
    build_destination_snapshot_payload,
    ensure_workspace,
)

_SYNC_SHEET_KV_HEADERS_LOWER = frozenset(h.strip().lower() for h in SYNC_DESTINATION_HEADERS)


def _strip_sync_destination_kv_lines_from_scope(scope: str) -> str:
    """Drop destination-sheet sync/trace `Header: value` lines from analysis_scope."""
    if not scope or ":" not in scope:
        return scope
    out: list[str] = []
    for line in scope.splitlines():
        if ":" not in line:
            out.append(line)
            continue
        head = line.split(":", 1)[0].strip().lower()
        if head in _SYNC_SHEET_KV_HEADERS_LOWER:
            continue
        out.append(line)
    return "\n".join(out)


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
        if changed:
            enqueue_destination_snapshot_embed_job(ws.id)
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
        scope_raw = (payload.get("analysis_scope") or "").strip()
        scope = _strip_sync_destination_kv_lines_from_scope(scope_raw)
        payload["analysis_scope"] = scope
        text_only = bool(payload.get("text_only"))
        video_url = (payload.get("video_url") or "").strip()
        if not text_only and not video_url:
            job.status = "FAILED"
            job.error_message = "video_url missing in job payload (set text_only=true for link-only / sheet coaching)"
            job.completed_at = datetime.now(UTC)
            db.commit()
            return {"ok": False, "error": "video_url_required"}

        body: dict[str, Any] = {
            "text_only": text_only,
            "video_url": video_url,
            "player_focus": (payload.get("player_focus") or "").strip(),
            "sport": (payload.get("sport") or "Soccer").strip(),
            "analysis_scope": scope,
            "coaching_focus": (payload.get("coaching_focus") or "").strip(),
        }
        shared, shared_sheet_debug = fetch_shared_feedback_context_text(
            max_chars=int(os.getenv("FEEDBACK_SHARED_CONTEXT_MAX_CHARS", "14000")),
        )
        if shared:
            body["shared_context"] = shared
        body["shared_context_sheet_debug"] = shared_sheet_debug

        pk = (payload.get("player_key") or "").strip()
        if not pk:
            first_name = str(payload.get("first_name") or "").strip()
            last_name = str(payload.get("last_name") or "").strip()
            if first_name or last_name:
                resolved = resolve_player_key_by_name(
                    db=db,
                    workspace_id=job.workspace_id,
                    first_name=first_name,
                    last_name=last_name,
                )
                if resolved:
                    pk = resolved
        mem_debug: dict[str, Any] = {"outcome": "skipped_no_player_key"}
        if pk:
            mem, mem_debug = retrieve_player_memory_context(
                db=db,
                workspace_id=job.workspace_id,
                player_key=pk,
                payload=payload,
            )
            if mem:
                body["player_memory_context"] = mem
        body["player_memory_retrieval_debug"] = mem_debug

        info(
            "feedback_delegate_context",
            agent_job_id=agent_job_id,
            text_only=text_only,
            shared_context_attached=bool(shared),
            shared_context_chars=len(shared or ""),
            shared_context_sheet_outcome=str((shared_sheet_debug or {}).get("outcome") or ""),
            player_memory_attached=bool(body.get("player_memory_context")),
            player_memory_chars=len(str(body.get("player_memory_context") or "")),
        )

        url = f"{base}/api/reviews"
        _timeout_key = "FEEDBACK_AGENT_HTTP_TIMEOUT_SECONDS_TEXT" if text_only else "FEEDBACK_AGENT_HTTP_TIMEOUT_SECONDS"
        _timeout_default = "180" if text_only else "30"
        timeout = float(os.getenv(_timeout_key) or _timeout_default)
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
        enqueue_feedback_review_embed_job(agent_job_id)
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
