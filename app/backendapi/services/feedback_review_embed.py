from __future__ import annotations

import json
import os
from typing import Any

import httpx
from sqlalchemy.orm import Session

from backendapi.services.player_memory_service import insert_chunks


def feedback_review_document_chunks(review: dict[str, Any], review_id: str) -> list[tuple[str, str, dict[str, Any]]]:
    """Turn stored review JSON into embeddable parts."""
    out: list[tuple[str, str, dict[str, Any]]] = []
    oa = review.get("overall_assessment") or {}
    strengths = oa.get("strengths") or []
    improvements = oa.get("improvements") or []
    next_focus = oa.get("next_focus") or []
    lines = ["Overall assessment:"]
    if strengths:
        lines.append("Strengths: " + "; ".join(str(s) for s in strengths))
    if improvements:
        lines.append("Improvements: " + "; ".join(str(s) for s in improvements))
    if next_focus:
        lines.append("Next focus: " + "; ".join(str(s) for s in next_focus))
    overall_text = "\n".join(lines)
    if overall_text.strip() != "Overall assessment:":
        out.append((overall_text, f"feedback:{review_id}:overall", {"review_id": review_id}))

    letter = str(review.get("coach_narrative") or "").strip()
    if letter:
        out.append((letter, f"feedback:{review_id}:letter", {"review_id": review_id, "kind": "coach_narrative"}))

    for idx, marker in enumerate(review.get("markers") or []):
        note = str(marker.get("coaching_note") or "").strip()
        if not note:
            continue
        ts = marker.get("timestamp_sec")
        cat = marker.get("category") or ""
        block = f"Moment @ {ts}s ({cat}): {note}"
        out.append((block, f"feedback:{review_id}:m:{idx}", {"review_id": review_id, "marker_index": idx}))
    return out


def embed_completed_feedback_review(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    review: dict[str, Any],
    review_id: str,
) -> int:
    parts = feedback_review_document_chunks(review, review_id)
    if not parts:
        return 0
    return insert_chunks(
        db=db,
        workspace_id=workspace_id,
        player_key=player_key,
        source_type="feedback_review",
        texts_with_refs=parts,
    )


def embed_feedback_review_for_agent_job(
    *,
    db: Session,
    job: Any,
    player_key: str | None = None,
) -> dict[str, Any]:
    """Fetch review from feedback-agent and embed into player personal memory (manual admin action)."""
    from backendapi.models.workspace import AgentJob
    from backendapi.services.workspace_service import ensure_workspace

    if not isinstance(job, AgentJob):
        raise TypeError("job must be AgentJob")

    if (job.status or "").upper() != "SUCCESS":
        return {"ok": False, "error": "job_not_success", "status": job.status}

    payload = json.loads(job.payload_json or "{}")
    pk = (player_key or payload.get("player_key") or "").strip()
    if not pk:
        return {"ok": False, "error": "no_player_key"}

    review_id = str(job.external_ref or "").strip()
    if not review_id:
        return {"ok": False, "error": "no_review_id"}

    base = (os.getenv("FEEDBACK_AGENT_BASE_URL") or "").rstrip("/")
    if not base:
        return {"ok": False, "error": "no_feedback_agent"}

    timeout = float(os.getenv("FEEDBACK_AGENT_HTTP_TIMEOUT_SECONDS", "30"))
    with httpx.Client(timeout=timeout) as client:
        resp = client.get(f"{base}/api/reviews/{review_id}")
    if resp.status_code >= 400:
        return {"ok": False, "error": f"fetch_review_http_{resp.status_code}"}

    review = resp.json()
    if review.get("error"):
        return {"ok": False, "error": "review_response_error"}

    ws = ensure_workspace(user_id="0", db=db)
    n = embed_completed_feedback_review(
        db=db,
        workspace_id=ws.id,
        player_key=pk,
        review=review,
        review_id=review_id,
    )
    return {"ok": True, "chunks_written": n, "player_key": pk, "review_id": review_id}
