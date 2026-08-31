from __future__ import annotations

import json
from typing import Any

from backendapi.services.evaluation_text_clean import clean_embedding_text


def _scalar(row: dict[str, str], *keys: str) -> str:
    for k in keys:
        v = str(row.get(k) or "").strip()
        if v:
            return v
    return ""


def _parse_json_field(raw: str) -> Any:
    s = (raw or "").strip()
    if not s or s in ("null", "NULL"):
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return None


def _normalize_videos(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        summary = str(item.get("summary") or "").strip() or None
        desc_raw = item.get("description")
        description = str(desc_raw).strip() if desc_raw is not None and str(desc_raw).strip() else None
        if summary is None and description is None:
            continue
        dedupe_key = f"{summary or ''}|{description or ''}".lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append({"summary": summary, "description": description})
    return out


def _normalize_feedback(raw: Any) -> dict[str, list[str]]:
    notes: list[str] = []
    annotations: list[str] = []
    if isinstance(raw, dict):
        n = raw.get("notes")
        if isinstance(n, list):
            notes = [str(x).strip() for x in n if str(x).strip()]
        a = raw.get("video_annotations")
        if isinstance(a, list):
            annotations = [str(x).strip() for x in a if str(x).strip()]
    return {"notes": notes, "video_annotations": annotations}


def document_from_sql_row(row: dict[str, str]) -> dict[str, Any]:
    """
    Canonical personal-context document shape (matches Sportal SQL SELECT).

    Fields: player_user_id, player_name, club_name, profile_text, videos[], feedback{}
    """
    player_id_raw = _scalar(row, "reviewee_id", "player_user_id", "player_id")
    player_user_id = int(player_id_raw) if str(player_id_raw).isdigit() else player_id_raw
    player_name = clean_embedding_text(_scalar(row, "player_name", "reviewee_name"), min_length=0)
    club_name = clean_embedding_text(_scalar(row, "club_name"), min_length=0)
    profile_text = _scalar(row, "profile_text")

    videos = _normalize_videos(_parse_json_field(_scalar(row, "videos")))
    feedback = _normalize_feedback(_parse_json_field(_scalar(row, "feedback")))

    return {
        "player_user_id": player_user_id,
        "player_name": player_name,
        "club_name": club_name,
        "profile_text": profile_text,
        "videos": videos,
        "feedback": feedback,
    }


def document_to_sql_row_dict(doc: dict[str, Any]) -> dict[str, str]:
    """Row dict for structured_chunks_from_player_row."""
    videos = doc.get("videos")
    feedback = doc.get("feedback")
    return {
        "player_user_id": str(doc.get("player_user_id") or ""),
        "player_name": str(doc.get("player_name") or ""),
        "club_name": str(doc.get("club_name") or ""),
        "profile_text": str(doc.get("profile_text") or ""),
        "videos": json.dumps(videos if isinstance(videos, list) else [], ensure_ascii=False),
        "feedback": json.dumps(
            feedback if isinstance(feedback, dict) else {"notes": [], "video_annotations": []},
            ensure_ascii=False,
        ),
    }


def embedding_eligibility_report(row: dict[str, str]) -> dict[str, Any]:
    """Counts what SQL returned vs what passes embedding filters (explains missing chunks)."""
    from backendapi.services.evaluation_text_clean import clean_embedding_text

    doc = document_from_sql_row(row)
    profile_raw = _scalar(row, "profile_text")
    profile_embeddable = bool(clean_embedding_text(profile_raw, min_length=1))

    videos_raw = _parse_json_field(_scalar(row, "videos"))
    videos_in_sql = len(_normalize_videos(videos_raw))
    videos_embeddable = 0
    if isinstance(videos_raw, list):
        seen: set[str] = set()
        for item in videos_raw:
            if not isinstance(item, dict):
                continue
            summary = clean_embedding_text(str(item.get("summary") or ""))
            desc = clean_embedding_text(str(item.get("description") or ""))
            if not summary and not desc:
                continue
            key = "|".join(p for p in (summary, desc) if p).lower()
            if key in seen:
                continue
            seen.add(key)
            videos_embeddable += 1

    feedback = doc.get("feedback") if isinstance(doc.get("feedback"), dict) else {}
    notes_raw = feedback.get("notes") if isinstance(feedback.get("notes"), list) else []
    anns_raw = feedback.get("video_annotations") if isinstance(feedback.get("video_annotations"), list) else []
    notes_embeddable = sum(1 for n in notes_raw if clean_embedding_text(str(n or "")))
    anns_embeddable = sum(1 for a in anns_raw if clean_embedding_text(str(a or "")))

    return {
        "profile_in_sql": bool(profile_raw.strip()),
        "profile_embeddable": profile_embeddable,
        "videos_in_sql": videos_in_sql,
        "videos_embeddable": videos_embeddable,
        "notes_in_sql": len(notes_raw),
        "notes_embeddable": notes_embeddable,
        "annotations_in_sql": len(anns_raw),
        "annotations_embeddable": anns_embeddable,
        "expected_chunk_count": (
            (1 if profile_embeddable else 0)
            + videos_embeddable
            + notes_embeddable
            + anns_embeddable
        ),
    }


def format_context_inventory_snapshot(
    doc: dict[str, Any],
    *,
    eligibility: dict[str, Any] | None = None,
) -> str:
    """Single readable chunk listing all SQL fields (including empty) and embed counts."""
    name = doc.get("player_name") or doc.get("player_user_id")
    club = doc.get("club_name") or ""
    header = f"Player: {name}"
    if club:
        header += f" | Club: {club}"
    lines = [
        header,
        "",
        "=== Personal context inventory (full SQL document) ===",
        "",
        "Profile text:",
        str(doc.get("profile_text") or "").strip() or "(empty in SQL / overlay)",
        "",
    ]
    videos = doc.get("videos") if isinstance(doc.get("videos"), list) else []
    lines.append(f"Videos ({len(videos)} in document):")
    if not videos:
        lines.append("  (none)")
    else:
        for i, v in enumerate(videos, start=1):
            if not isinstance(v, dict):
                continue
            lines.append(
                f"  {i}. summary={v.get('summary')!r} description={v.get('description')!r}"
            )

    feedback = doc.get("feedback") if isinstance(doc.get("feedback"), dict) else {}
    notes = feedback.get("notes") if isinstance(feedback.get("notes"), list) else []
    anns = feedback.get("video_annotations") if isinstance(feedback.get("video_annotations"), list) else []
    lines.extend(["", f"Coach evaluation notes ({len(notes)}):"])
    if not notes:
        lines.append("  (none)")
    else:
        for i, n in enumerate(notes, start=1):
            text = str(n or "").strip()
            preview = text[:240] + ("…" if len(text) > 240 else "")
            lines.append(f"  {i}. {preview or '(empty)'}")

    lines.extend(["", f"Video coaching annotations ({len(anns)}):"])
    if not anns:
        lines.append("  (none)")
    else:
        for i, a in enumerate(anns, start=1):
            text = str(a or "").strip()
            preview = text[:240] + ("…" if len(text) > 240 else "")
            lines.append(f"  {i}. {preview or '(empty)'}")

    if eligibility:
        lines.extend(
            [
                "",
                "=== Embeddable chunks (after text cleaning) ===",
                f"Profile: {'yes' if eligibility.get('profile_embeddable') else 'no'}",
                f"Videos: {eligibility.get('videos_embeddable')} of {eligibility.get('videos_in_sql')}",
                f"Notes: {eligibility.get('notes_embeddable')} of {eligibility.get('notes_in_sql')}",
                f"Annotations: {eligibility.get('annotations_embeddable')} of {eligibility.get('annotations_in_sql')}",
                f"Expected vector chunks (incl. this inventory): {eligibility.get('expected_chunk_count')}",
                "",
                "Individual chunks use types: profile, video_N, note_N, annotation_N.",
                "Use admin chunk list pagination or re-run SQL sync if counts do not match.",
            ]
        )
    return "\n".join(lines)


def merge_personal_documents(
    sql_doc: dict[str, Any],
    overlay: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Merge admin overlay onto a fresh SQL document.

    Empty overlay lists do not wipe SQL videos/feedback (fixes losing chunks after a
    sparse manual create). Non-empty overlay lists replace SQL for that section.
    """
    if not overlay:
        return dict(sql_doc)
    out = dict(sql_doc)
    for key in ("player_user_id", "player_name", "club_name"):
        if key in overlay and overlay[key] is not None:
            val = overlay[key]
            if key == "player_user_id" or str(val).strip():
                out[key] = val
    if "profile_text" in overlay and overlay["profile_text"] is not None:
        out["profile_text"] = str(overlay["profile_text"])
    if "videos" in overlay and overlay["videos"] is not None:
        ov_videos = _normalize_videos(overlay["videos"])
        if ov_videos:
            out["videos"] = ov_videos
    if "feedback" in overlay and overlay["feedback"] is not None:
        sql_fb = out.get("feedback") if isinstance(out.get("feedback"), dict) else {}
        sql_notes = list(sql_fb.get("notes") or [])
        sql_anns = list(sql_fb.get("video_annotations") or [])
        ov_fb = _normalize_feedback(overlay["feedback"])
        out["feedback"] = {
            "notes": ov_fb["notes"] if ov_fb["notes"] else sql_notes,
            "video_annotations": ov_fb["video_annotations"] if ov_fb["video_annotations"] else sql_anns,
        }
    return out
