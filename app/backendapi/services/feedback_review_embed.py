from __future__ import annotations

import json
from typing import Any

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
