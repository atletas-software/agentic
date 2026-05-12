from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from sqlalchemy.orm import Session

from app.models.workspace import AgentJob, WorkspaceContextItem
from app.services.player_directory import resolve_player_key_by_name
from app.services.player_key import (
    normalize_player_key,
    player_key_column_names,
    row_dict_from_sheet,
)
from app.services.player_memory_service import delete_chunks_for_source_prefix, insert_chunks
from app.services.text_chunking import chunk_sheet_row_document
from app.services.workspace_enqueue import enqueue_feedback_delegate_job


def _row_hash(row: list[str]) -> str:
    raw = json.dumps(row, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def embed_destination_snapshot_changes(*, db: Session, workspace_id: int) -> dict[str, Any]:
    """Diff latest destination_snapshot vs previous; re-embed changed rows (sheet_row refs)."""
    items = (
        db.query(WorkspaceContextItem)
        .filter(
            WorkspaceContextItem.workspace_id == workspace_id,
            WorkspaceContextItem.item_type == "destination_snapshot",
        )
        .order_by(WorkspaceContextItem.created_at.desc())
        .limit(2)
        .all()
    )
    if not items:
        return {"ok": False, "reason": "no_snapshot"}

    new_payload = json.loads(items[0].payload_json)
    headers = new_payload.get("headers") or []
    new_rows = new_payload.get("rows") or []

    old_rows: list[list[str]] = []
    if len(items) > 1:
        old_payload = json.loads(items[1].payload_json)
        old_rows = old_payload.get("rows") or []

    max_len = max(len(new_rows), len(old_rows))
    changed_indices: list[int] = []
    for i in range(max_len):
        nr = new_rows[i] if i < len(new_rows) else []
        orow = old_rows[i] if i < len(old_rows) else []
        if _row_hash(nr) != _row_hash(orow):
            changed_indices.append(i)

    key_cols = player_key_column_names()
    written = 0
    auto_feedback_queued = 0
    for i in changed_indices:
        row = new_rows[i] if i < len(new_rows) else []
        rd = row_dict_from_sheet(headers, row)
        if _row_empty_for_keys(rd, key_cols):
            continue
        pk = normalize_player_key(rd)
        prefix = f"sheet_row:{i}:"
        delete_chunks_for_source_prefix(
            db=db,
            workspace_id=workspace_id,
            player_key=pk,
            source_type="sheet_row",
            source_ref_prefix=prefix,
        )
        parts = chunk_sheet_row_document(headers, row)
        batch: list[tuple[str, str, dict[str, Any]]] = []
        for p_idx, part in enumerate(parts):
            ref = f"sheet_row:{i}:{p_idx}"
            meta = {
                "sheet_name": new_payload.get("sheet_name"),
                "row_index": i,
                "part": p_idx,
            }
            batch.append((part, ref, meta))
        written += insert_chunks(
            db=db,
            workspace_id=workspace_id,
            player_key=pk,
            source_type="sheet_row",
            texts_with_refs=batch,
        )
        auto_feedback_queued += _enqueue_auto_feedback_if_ready(
            db=db,
            workspace_id=workspace_id,
            row_index=i,
            row_data=rd,
        )

    return {
        "ok": True,
        "snapshots_compared": len(items),
        "rows_changed": len(changed_indices),
        "chunks_written": written,
        "auto_feedback_queued": auto_feedback_queued,
    }


def _row_empty_for_keys(rd: dict[str, str], cols: list[str]) -> bool:
    return not any(str(rd.get(c, "")).strip() for c in cols)


def _enqueue_auto_feedback_if_ready(*, db: Session, workspace_id: int, row_index: int, row_data: dict[str, str]) -> int:
    first_name = _pick(row_data, ["first and last name", "first_name", "firstname", "name"])
    last_name = _pick(row_data, ["last_name", "lastname", "surname"])
    if first_name and not last_name:
        fn, ln = _split_name(first_name)
        first_name, last_name = fn, ln
    video_url = _pick(row_data, ["link to game", "video_url", "video link", "link"])
    if not (first_name and video_url):
        return 0
    player_key = resolve_player_key_by_name(
        db=db,
        workspace_id=workspace_id,
        first_name=first_name,
        last_name=last_name,
    )
    row_hash = hashlib.sha256(
        json.dumps({"row_index": row_index, "first_name": first_name, "last_name": last_name, "video_url": video_url}, ensure_ascii=True).encode("utf-8")
    ).hexdigest()
    exists = (
        db.query(WorkspaceContextItem)
        .filter(
            WorkspaceContextItem.workspace_id == workspace_id,
            WorkspaceContextItem.item_type == "auto_feedback_row",
            WorkspaceContextItem.content_hash == row_hash,
        )
        .first()
    )
    if exists is not None:
        return 0
    payload = {
        "video_url": video_url,
        "sport": "Soccer",
        "player_focus": f"{first_name} {last_name}".strip(),
        "coaching_focus": "auto",
        "analysis_scope": "auto_row_trigger",
        "first_name": first_name,
        "last_name": last_name,
        "player_key": player_key or "",
    }
    job = AgentJob(
        workspace_id=workspace_id,
        agent_type="FEEDBACK_DELEGATE",
        status="PENDING",
        payload_json=json.dumps(payload, ensure_ascii=True),
    )
    db.add(job)
    db.add(
        WorkspaceContextItem(
            workspace_id=workspace_id,
            item_type="auto_feedback_row",
            content_hash=row_hash,
            payload_json=json.dumps(payload, ensure_ascii=True),
        )
    )
    db.commit()
    db.refresh(job)
    ok = enqueue_feedback_delegate_job(job.id)
    if not ok:
        job.status = "FAILED"
        job.error_message = "Failed to enqueue auto feedback job."
        db.commit()
        return 0
    return 1


def _pick(row_data: dict[str, str], keys: list[str]) -> str:
    for k in keys:
        v = str(row_data.get(k, "")).strip()
        if v:
            return v
    return ""


def _split_name(full_name: str) -> tuple[str, str]:
    parts = [p for p in re.split(r"\s+", full_name.strip()) if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])
