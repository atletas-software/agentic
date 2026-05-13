from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

from app.core.logger import error, info
from app.db import SessionLocal
from app.models.workspace import AgentJob
from app.services.agent_job_cancel import agent_job_cancel_requested, clear_agent_job_cancel_requested
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


def feedback_agent_poll_progress_hint(result_json: str | None) -> str | None:
    """Short line for job lists while FEEDBACK_DELEGATE is polling the feedback agent."""
    if not result_json:
        return None
    try:
        obj = json.loads(result_json)
    except json.JSONDecodeError:
        return None
    poll = obj.get("feedback_agent_poll")
    if not isinstance(poll, dict):
        return None
    detail = poll.get("progress_detail")
    if detail:
        return str(detail)[:220]
    phase = poll.get("phase")
    if phase:
        return f"phase: {phase}"
    st = poll.get("status")
    if st:
        return f"feedback job: {st}"
    return None


def _poll_feedback_review_until_done(
    base: str,
    review_id: str,
    *,
    text_only: bool,
    agent_job_id: int | None = None,
    on_status: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[str, str | None]:
    """
    The feedback agent returns immediately from POST /api/reviews while processing runs in a thread.
    Poll GET /api/reviews/{id}/status until completed, failed, or timeout.
    Returns: ("completed", None) | ("failed", err) | ("timeout", err) | ("cancelled", err).
    """
    poll_timeout = float(
        os.getenv(
            "FEEDBACK_AGENT_REVIEW_POLL_TIMEOUT_SECONDS",
            "900" if text_only else "10800",
        )
    )
    poll_interval = float(os.getenv("FEEDBACK_AGENT_REVIEW_POLL_INTERVAL_SECONDS", "2"))
    status_url = f"{base}/api/reviews/{review_id}/status"
    deadline = time.monotonic() + poll_timeout
    last_transport: str | None = None
    last_db_flush = -1e30
    flush_every = float(os.getenv("FEEDBACK_AGENT_POLL_DB_UPDATE_INTERVAL_SEC", "10"))
    with httpx.Client(timeout=60.0) as client:
        while time.monotonic() < deadline:
            if agent_job_id is not None and agent_job_cancel_requested(agent_job_id):
                return "cancelled", "Cancelled by user (workspace stop)."
            try:
                sr = client.get(status_url)
            except httpx.RequestError as exc:
                last_transport = str(exc)
                time.sleep(poll_interval)
                continue
            if sr.status_code == 404:
                time.sleep(poll_interval)
                continue
            if sr.status_code != 200:
                return "failed", f"status poll HTTP {sr.status_code}: {sr.text[:1200]}"
            payload = sr.json()
            now_m = time.monotonic()
            if on_status is not None and (now_m - last_db_flush) >= flush_every:
                try:
                    on_status(payload)
                except Exception:  # noqa: BLE001
                    pass
                last_db_flush = now_m
            st = str(payload.get("status") or "")
            if st == "completed":
                return "completed", None
            if st == "failed":
                return "failed", str(payload.get("error") or "review_failed")
            if st in {"queued", "running"}:
                time.sleep(poll_interval)
                continue
            last_transport = f"unknown_status:{st}"
            time.sleep(poll_interval)
    extra = f" Last error: {last_transport}" if last_transport else ""
    return "timeout", f"No completed review after {poll_timeout:.0f}s.{extra}"


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

        if agent_job_cancel_requested(agent_job_id):
            job.status = "FAILED"
            job.error_message = "Cancelled by user (stopped before work began)."
            job.completed_at = datetime.now(UTC)
            db.commit()
            clear_agent_job_cancel_requested(agent_job_id)
            return {"ok": False, "error": "cancelled"}

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
            "first_name": str(payload.get("first_name") or "").strip(),
            "last_name": str(payload.get("last_name") or "").strip(),
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

        if agent_job_cancel_requested(agent_job_id):
            job.status = "FAILED"
            job.error_message = "Cancelled by user."
            job.completed_at = datetime.now(UTC)
            db.commit()
            clear_agent_job_cancel_requested(agent_job_id)
            return {"ok": False, "error": "cancelled"}

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
        review_id = str(data.get("id") or "").strip()
        if not review_id:
            job.status = "FAILED"
            job.error_message = "Feedback agent POST did not return a review id"
            job.completed_at = datetime.now(UTC)
            db.commit()
            return {"ok": False, "error": "no_review_id"}

        job.external_ref = review_id
        try:
            boot = json.loads(job.result_json or "{}")
        except json.JSONDecodeError:
            boot = {}
        boot["feedback_agent_poll"] = {
            "review_id": review_id,
            "status": "submitted",
            "progress_detail": "Feedback agent is processing video (this can take many minutes).",
            "updated_at": datetime.now(UTC).isoformat(),
        }
        job.result_json = json.dumps(boot, ensure_ascii=True)
        db.commit()

        def _flush_feedback_status(status_payload: dict[str, Any]) -> None:
            j = db.get(AgentJob, agent_job_id)
            if j is None:
                return
            try:
                prev = json.loads(j.result_json or "{}")
            except json.JSONDecodeError:
                prev = {}
            prev["feedback_agent_poll"] = {
                "review_id": review_id,
                "status": status_payload.get("status"),
                "phase": status_payload.get("phase"),
                "progress_detail": status_payload.get("progress_detail"),
                "probe_current": status_payload.get("probe_current"),
                "probe_estimate": status_payload.get("probe_estimate"),
                "segment_current": status_payload.get("segment_current"),
                "segment_total": status_payload.get("segment_total"),
                "updated_at": datetime.now(UTC).isoformat(),
            }
            j.external_ref = review_id
            j.result_json = json.dumps(prev, ensure_ascii=True)
            db.commit()

        poll_out, poll_err = _poll_feedback_review_until_done(
            base,
            review_id,
            text_only=text_only,
            agent_job_id=agent_job_id,
            on_status=_flush_feedback_status,
        )
        if poll_out == "cancelled":
            job.status = "FAILED"
            job.error_message = poll_err or "Cancelled by user."
            job.external_ref = review_id
            job.completed_at = datetime.now(UTC)
            db.commit()
            return {"ok": False, "error": "cancelled"}
        if poll_out == "failed":
            job.status = "FAILED"
            job.error_message = poll_err or "feedback_review_failed"
            job.external_ref = review_id
            job.completed_at = datetime.now(UTC)
            db.commit()
            return {"ok": False, "error": "review_failed"}
        if poll_out == "timeout":
            job.status = "FAILED"
            job.error_message = poll_err or "feedback_review_poll_timeout"
            job.external_ref = review_id
            job.completed_at = datetime.now(UTC)
            db.commit()
            return {"ok": False, "error": "poll_timeout"}

        job.status = "SUCCESS"
        job.external_ref = review_id
        job.result_json = json.dumps(
            {"id": review_id, "create_response": data, "feedback_poll": "completed"},
            ensure_ascii=True,
        )
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
        try:
            clear_agent_job_cancel_requested(agent_job_id)
        except Exception:  # noqa: BLE001
            pass
        db.close()
