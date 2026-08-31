from __future__ import annotations

import json
from typing import Any

from backendapi.services.evaluation_text_clean import clean_embedding_text, format_sql_cell_for_embedding
from backendapi.services.player_context_document import document_from_sql_row
from backendapi.services.text_chunking import chunk_text_by_tokens

PLAYER_CONTEXT_VECTOR_FORMAT = "player_context_v4"


def _scalar(row: dict[str, str], *keys: str) -> str:
    for k in keys:
        v = str(row.get(k) or "").strip()
        if v:
            return v
    return ""


def _clean_scalar(row: dict[str, str], *keys: str) -> str:
    return clean_embedding_text(_scalar(row, *keys), min_length=1)


def _parse_json_field(raw: str) -> Any:
    s = (raw or "").strip()
    if not s or s in ("null", "NULL"):
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return None


def _player_header(row: dict[str, str]) -> str:
    player_id = _scalar(row, "reviewee_id", "player_user_id", "player_id")
    player_name = clean_embedding_text(_scalar(row, "player_name", "reviewee_name"), min_length=1)
    club = clean_embedding_text(_scalar(row, "club_name"), min_length=1)
    header = f"Player: {player_name or player_id}"
    if club:
        header += f" | Club: {club}"
    return header


def structured_chunks_from_player_row(row: dict[str, str]) -> list[tuple[str, str, dict[str, Any]]]:
    """
    Turn one personal-context SQL row into embeddable chunks.

    Expected SQL columns:
      player_user_id, player_name, club_name, profile_text, videos (JSON), feedback (JSON)

    Only these fields are embedded — no other SQL columns are stored in the vector DB.
    """
    out: list[tuple[str, str, dict[str, Any]]] = []
    player_id = _scalar(row, "reviewee_id", "player_user_id", "player_id")
    player_name = clean_embedding_text(_scalar(row, "player_name", "reviewee_name"), min_length=1)
    club_name = clean_embedding_text(_scalar(row, "club_name"), min_length=1)
    header = _player_header(row)

    player_context = document_from_sql_row(row)
    # Slim metadata only — do not store full player_context on every chunk (bloats Firestore writes).
    base_meta: dict[str, Any] = {
        "vector_format": PLAYER_CONTEXT_VECTOR_FORMAT,
        "player_user_id": player_context["player_user_id"],
        "player_name": player_name,
        "club_name": club_name,
    }

    profile = clean_embedding_text(_scalar(row, "profile_text"), min_length=1)
    if profile:
        body = f"{header}\nProfile:\n{profile}"
        out.append((body, "profile", {**base_meta, "chunk_type": "profile"}))

    videos_raw = _parse_json_field(_scalar(row, "videos"))
    if isinstance(videos_raw, list):
        seen_video: set[str] = set()
        video_idx = 0
        for item in videos_raw:
            if not isinstance(item, dict):
                continue
            summary = clean_embedding_text(str(item.get("summary") or ""))
            desc = clean_embedding_text(str(item.get("description") or ""))
            parts = [p for p in (summary, desc) if p]
            if not parts:
                continue
            dedupe_key = "|".join(parts).lower()
            if dedupe_key in seen_video:
                continue
            seen_video.add(dedupe_key)
            video_idx += 1
            body = f"{header}\nVideo {video_idx}:\n" + "\n".join(parts)
            out.append(
                (
                    body,
                    f"video_{video_idx}",
                    {
                        **base_meta,
                        "chunk_type": "video",
                        "video_index": video_idx,
                        "video_summary": summary,
                        "video_description": desc or None,
                    },
                )
            )

    feedback_raw = _parse_json_field(_scalar(row, "feedback"))
    if isinstance(feedback_raw, dict):
        notes = feedback_raw.get("notes")
        if isinstance(notes, list):
            note_idx = 0
            for note in notes:
                text = clean_embedding_text(str(note or ""))
                if not text:
                    continue
                note_idx += 1
                body = f"{header}\nCoach evaluation note {note_idx}:\n{text}"
                out.append(
                    (
                        body,
                        f"note_{note_idx}",
                        {**base_meta, "chunk_type": "feedback_note", "note_index": note_idx},
                    )
                )
        annotations = feedback_raw.get("video_annotations")
        if isinstance(annotations, list):
            ann_idx = 0
            for ann in annotations:
                text = clean_embedding_text(str(ann or ""))
                if not text:
                    continue
                ann_idx += 1
                body = f"{header}\nVideo coaching annotation {ann_idx}:\n{text}"
                out.append(
                    (
                        body,
                        f"annotation_{ann_idx}",
                        {
                            **base_meta,
                            "chunk_type": "video_annotation",
                            "annotation_index": ann_idx,
                        },
                    )
                )

    if out:
        return out

    # Fallback: flatten all columns (legacy SQL shape)
    lines: list[str] = []
    for k, v in row.items():
        disp = format_sql_cell_for_embedding(k, v)
        if disp:
            lines.append(f"{k}: {disp}")
    merged = "\n".join(lines)
    if not merged.strip():
        return []
    parts = chunk_text_by_tokens(merged)
    return [(p, f"part_{i + 1}", {**base_meta, "chunk_type": "general"}) for i, p in enumerate(parts)]


def structured_chunks_from_shared_sheet(text: str) -> list[tuple[str, str, dict[str, Any]]]:
    """Legacy TSV path — delegates to shared_context_schema."""
    from backendapi.services.shared_context_schema import structured_chunks_from_shared_sheet_tsv

    return structured_chunks_from_shared_sheet_tsv(text)
