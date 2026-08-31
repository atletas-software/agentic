from __future__ import annotations

import json
import os
from typing import Any

import httpx
from sqlalchemy.orm import Session

from backendapi.services.player_memory_service import insert_chunks


def _base_chunk_metadata(
    *,
    review_id: str,
    video_url: str | None = None,
    agent_job_id: int | None = None,
    player_key: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "review_id": review_id,
        "source_kind": "feedback_review",
    }
    if video_url:
        meta["video_url"] = video_url
    if agent_job_id is not None:
        meta["agent_job_id"] = agent_job_id
    if player_key:
        meta["player_key"] = player_key
    if extra:
        meta.update(extra)
    return meta


def feedback_review_document_chunks(
    review: dict[str, Any],
    review_id: str,
    *,
    video_url: str | None = None,
    agent_job_id: int | None = None,
    player_key: str | None = None,
) -> list[tuple[str, str, dict[str, Any]]]:
    """Turn stored review JSON into embeddable parts linked to the source video."""
    resolved_video = (video_url or str(review.get("video_url") or "")).strip() or None
    base_meta = _base_chunk_metadata(
        review_id=review_id,
        video_url=resolved_video,
        agent_job_id=agent_job_id,
        player_key=player_key,
    )

    out: list[tuple[str, str, dict[str, Any]]] = []
    oa = review.get("overall_assessment") or {}
    strengths = oa.get("strengths") or []
    improvements = oa.get("improvements") or []
    next_focus = oa.get("next_focus") or []
    lines = ["Overall assessment:"]
    if resolved_video:
        lines.append(f"Source video: {resolved_video}")
    if strengths:
        lines.append("Strengths: " + "; ".join(str(s) for s in strengths))
    if improvements:
        lines.append("Improvements: " + "; ".join(str(s) for s in improvements))
    if next_focus:
        lines.append("Next focus: " + "; ".join(str(s) for s in next_focus))
    overall_text = "\n".join(lines)
    if overall_text.strip() != "Overall assessment:":
        out.append(
            (
                overall_text,
                f"feedback:{review_id}:overall",
                {**base_meta, "chunk_kind": "overall"},
            )
        )

    letter = str(review.get("coach_narrative") or "").strip()
    if letter:
        letter_text = letter
        if resolved_video:
            letter_text = f"Source video: {resolved_video}\n\n{letter}"
        out.append(
            (
                letter_text,
                f"feedback:{review_id}:letter",
                {**base_meta, "chunk_kind": "coach_narrative"},
            )
        )

    for idx, marker in enumerate(review.get("markers") or []):
        note = str(marker.get("coaching_note") or "").strip()
        if not note:
            continue
        ts = marker.get("timestamp_sec")
        cat = marker.get("category") or ""
        block_lines = []
        if resolved_video:
            block_lines.append(f"Source video: {resolved_video}")
        block_lines.append(f"Moment @ {ts}s ({cat}): {note}")
        block = "\n".join(block_lines)
        out.append(
            (
                block,
                f"feedback:{review_id}:m:{idx}",
                {
                    **base_meta,
                    "chunk_kind": "marker",
                    "marker_index": idx,
                    "timestamp_sec": ts,
                    "category": cat,
                },
            )
        )
    return out


def embed_completed_feedback_review(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    review: dict[str, Any],
    review_id: str,
    video_url: str | None = None,
    agent_job_id: int | None = None,
) -> int:
    parts = feedback_review_document_chunks(
        review,
        review_id,
        video_url=video_url,
        agent_job_id=agent_job_id,
        player_key=player_key,
    )
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

    from backendapi.services.feedback_runner import load_review_json

    review = load_review_json(review_id)
    if review is None:
        base = (os.getenv("FEEDBACK_AGENT_BASE_URL") or "").rstrip("/")
        if not base:
            return {"ok": False, "error": "no_feedback_agent_or_local_review"}

        timeout = float(os.getenv("FEEDBACK_AGENT_HTTP_TIMEOUT_SECONDS", "30"))
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(f"{base}/api/reviews/{review_id}")
        if resp.status_code >= 400:
            return {"ok": False, "error": f"fetch_review_http_{resp.status_code}"}
        review = resp.json()
    if review.get("error"):
        return {"ok": False, "error": "review_response_error"}

    video_url = (payload.get("video_url") or review.get("video_url") or "").strip() or None

    ws = ensure_workspace(user_id="0", db=db)
    n = embed_completed_feedback_review(
        db=db,
        workspace_id=ws.id,
        player_key=pk,
        review=review,
        review_id=review_id,
        video_url=video_url,
        agent_job_id=int(job.id),
    )
    return {
        "ok": True,
        "chunks_written": n,
        "player_key": pk,
        "review_id": review_id,
        "video_url": video_url,
    }
