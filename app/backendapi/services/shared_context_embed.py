from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from backendapi.services.context_scope import CONTEXT_SCOPE_SHARED, SHARED_PLAYER_KEY
from backendapi.core.logger import info as log_info
from backendapi.services.player_memory_settings import get_player_memory_settings
from backendapi.services.player_memory_service import (
    delete_chunks_for_workspace_source_type,
    insert_chunks,
)
from backendapi.services.shared_context_schema import (
    SHARED_CONTEXT_VECTOR_FORMAT,
    structured_chunks_from_shared_records,
)
from backendapi.services.shared_feedback_context_sheet import fetch_shared_feedback_context_records


def sync_shared_context_from_sheet(*, db: Session, workspace_id: int) -> dict[str, Any]:
    """Fetch org-wide shared coaching sheet, chunk rows, embed into shared vector collection."""
    records, debug = fetch_shared_feedback_context_records()
    if not records:
        return {
            "ok": False,
            "reason": debug.get("reason") or "empty_sheet",
            "debug": debug,
        }

    settings = get_player_memory_settings(db)
    max_tokens = int(settings.get("chunk_max_tokens") or 650)

    deleted = delete_chunks_for_workspace_source_type(
        db=db,
        workspace_id=workspace_id,
        source_type="shared_sheet",
        context_scope=CONTEXT_SCOPE_SHARED,
    )

    structured = structured_chunks_from_shared_records(records, max_tokens=max_tokens)
    if not structured:
        return {
            "ok": False,
            "reason": "no_chunks_after_processing",
            "debug": debug,
            "chunks_deleted": deleted,
        }

    batch: list[tuple[str, str, dict[str, Any]]] = []
    for embed_text, ref, meta in structured:
        meta.update(
            {
                "spreadsheet_id": debug.get("spreadsheet_id"),
                "sheet_gid": debug.get("sheet_gid"),
                "sheet_title": debug.get("sheet_title_resolved"),
                "chunk_text": embed_text,
            }
        )
        batch.append((embed_text, ref, meta))

    written = insert_chunks(
        db=db,
        workspace_id=workspace_id,
        player_key=SHARED_PLAYER_KEY,
        source_type="shared_sheet",
        texts_with_refs=batch,
        context_scope=CONTEXT_SCOPE_SHARED,
    )

    log_info(
        "shared_context_sheet_sync_done",
        workspace_id=workspace_id,
        chunks_written=written,
        chunks_deleted=deleted,
        rows_processed=len(records),
    )

    return {
        "ok": True,
        "chunks_written": written,
        "chunks_deleted": deleted,
        "rows_processed": len(records),
        "parts_total": len(structured),
        "vector_format": SHARED_CONTEXT_VECTOR_FORMAT,
        "debug": debug,
    }
