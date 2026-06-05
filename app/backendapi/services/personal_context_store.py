from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from backendapi.services.external_sql_engine import get_external_sql_engine, normalize_external_db_url
from backendapi.models.player_memory import PlayerPersonalContextOverlay
from backendapi.services.context_scope import CONTEXT_SCOPE_PERSONAL
from backendapi.services.player_context_chunker import structured_chunks_from_player_row
from backendapi.services.player_context_document import (
    document_from_sql_row,
    document_to_sql_row_dict,
    merge_personal_documents,
)
from backendapi.services.player_directory import upsert_player_directory_entry
from backendapi.services.player_memory_service import (
    delete_chunks_for_player_source_type,
    delete_player_sql_sync_chunks_for_resync,
    insert_chunks,
    list_chunks,
)
from backendapi.services.player_memory_settings import effective_player_context_sql, get_player_memory_settings
from backendapi.services.personal_context_sql_write import write_profile_text_to_sportal


def _sql_append_limit(sql_query: str, limit: int) -> str:
    s = (sql_query or "").strip().rstrip(";")
    if not s:
        return s
    import re

    if re.search(r"\blimit\s+\d+\s*$", s, flags=re.I):
        return s
    return f"{s}\nLIMIT {max(1, int(limit))}"


def _fetch_sql_row(*, database_url: str, sql_query: str, player_user_id: int) -> dict[str, str] | None:
    if not database_url.strip() or not sql_query.strip():
        return None
    if ":player_user_id" not in sql_query:
        return None
    eng = get_external_sql_engine(database_url)
    if eng is None:
        return None
    stmt = _sql_append_limit(sql_query, 1)
    params = {"player_user_id": int(player_user_id)}
    with eng.connect() as conn:
        result = conn.execute(text(stmt), params)
        cols = [str(c) for c in result.keys()]
        row = result.fetchone()
        if row is None:
            return None
        return {cols[i]: "" if row[i] is None else str(row[i]) for i in range(len(cols))}


def get_overlay(db: Session, *, workspace_id: int, player_user_id: int) -> dict[str, Any] | None:
    row = (
        db.query(PlayerPersonalContextOverlay)
        .filter(
            PlayerPersonalContextOverlay.workspace_id == workspace_id,
            PlayerPersonalContextOverlay.player_user_id == int(player_user_id),
        )
        .one_or_none()
    )
    if row is None or not row.document_json:
        return None
    try:
        data = json.loads(row.document_json)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None


def save_overlay(db: Session, *, workspace_id: int, document: dict[str, Any]) -> None:
    pid = int(document["player_user_id"])
    payload = json.dumps(document, ensure_ascii=False)
    row = (
        db.query(PlayerPersonalContextOverlay)
        .filter(
            PlayerPersonalContextOverlay.workspace_id == workspace_id,
            PlayerPersonalContextOverlay.player_user_id == pid,
        )
        .one_or_none()
    )
    if row is None:
        row = PlayerPersonalContextOverlay(
            workspace_id=workspace_id,
            player_user_id=pid,
            document_json=payload,
        )
        db.add(row)
    else:
        row.document_json = payload
    db.commit()


def delete_overlay(db: Session, *, workspace_id: int, player_user_id: int) -> bool:
    row = (
        db.query(PlayerPersonalContextOverlay)
        .filter(
            PlayerPersonalContextOverlay.workspace_id == workspace_id,
            PlayerPersonalContextOverlay.player_user_id == int(player_user_id),
        )
        .one_or_none()
    )
    if row is None:
        return False
    db.delete(row)
    db.commit()
    return True


def merged_document_for_player(
    db: Session,
    *,
    workspace_id: int,
    player_user_id: int,
) -> dict[str, Any]:
    settings = get_player_memory_settings(db)
    sql_row = _fetch_sql_row(
        database_url=str(settings.get("sql_database_url") or ""),
        sql_query=effective_player_context_sql(settings),
        player_user_id=player_user_id,
    )
    sql_doc = document_from_sql_row(sql_row) if sql_row else {
        "player_user_id": player_user_id,
        "player_name": "",
        "club_name": "",
        "profile_text": "",
        "videos": [],
        "feedback": {"notes": [], "video_annotations": []},
    }
    overlay = get_overlay(db, workspace_id=workspace_id, player_user_id=player_user_id)
    return merge_personal_documents(sql_doc, overlay)


def reindex_personal_vectors(
    db: Session,
    *,
    workspace_id: int,
    document: dict[str, Any],
) -> int:
    """Replace sql_sync vectors for a player from the merged personal-context document."""
    pid = int(document["player_user_id"])
    player_key = str(pid)
    row_dict = document_to_sql_row_dict(document)
    structured = structured_chunks_from_player_row(row_dict)
    delete_player_sql_sync_chunks_for_resync(
        db=db,
        workspace_id=workspace_id,
        player_key=player_key,
    )
    if not structured:
        return 0

    player_name = str(document.get("player_name") or "").strip()
    batch: list[tuple[str, str, dict[str, Any]]] = []
    for idx, (embed_text, kind, extra) in enumerate(structured, start=1):
        ref = f"sql_sync:player_{player_key}:{kind}"
        meta: dict[str, Any] = {
            "context_scope": CONTEXT_SCOPE_PERSONAL,
            "player_key": player_key,
            "player_id": pid,
            "chunk_index": idx,
            "chunk_text": embed_text,
            "sync_mode": "admin_upsert",
            **extra,
        }
        batch.append((embed_text, ref, meta))

    written = insert_chunks(
        db=db,
        workspace_id=workspace_id,
        player_key=player_key,
        source_type="sql_sync",
        texts_with_refs=batch,
        context_scope=CONTEXT_SCOPE_PERSONAL,
    )
    if player_name:
        parts = player_name.split(" ", 1)
        upsert_player_directory_entry(
            db=db,
            workspace_id=workspace_id,
            player_key=player_key,
            first_name=parts[0],
            last_name=parts[1] if len(parts) > 1 else "",
        )
    return written


def upsert_personal_record(
    db: Session,
    *,
    workspace_id: int,
    document: dict[str, Any],
    write_sql_profile: bool = True,
) -> dict[str, Any]:
    settings = get_player_memory_settings(db)
    pid = int(document["player_user_id"])
    save_overlay(db, workspace_id=workspace_id, document=document)

    sql_result: dict[str, object] = {"ok": False, "skipped": True}
    if write_sql_profile:
        sql_url = str(settings.get("sql_database_url") or "").strip()
        if sql_url:
            sql_result = write_profile_text_to_sportal(
                database_url=sql_url,
                player_user_id=pid,
                profile_text=str(document.get("profile_text") or ""),
            )

    # Rebuild vectors from fresh SQL + overlay — not the raw form body (sparse creates
    # used to delete all sql_sync chunks and replace with empty lists).
    merged = merged_document_for_player(db, workspace_id=workspace_id, player_user_id=pid)
    chunks_written = reindex_personal_vectors(db, workspace_id=workspace_id, document=merged)
    return {
        "ok": True,
        "player_user_id": pid,
        "chunks_written": chunks_written,
        "sql_profile_write": sql_result,
        "merged_from_sql": bool(str(settings.get("sql_database_url") or "").strip()),
    }


def delete_personal_record(
    db: Session,
    *,
    workspace_id: int,
    player_user_id: int,
    delete_vectors: bool = True,
) -> dict[str, Any]:
    player_key = str(int(player_user_id))
    delete_overlay(db, workspace_id=workspace_id, player_user_id=player_user_id)
    deleted = 0
    if delete_vectors:
        for source_type in ("sql_sync", "manual"):
            deleted += delete_chunks_for_player_source_type(
                db=db,
                workspace_id=workspace_id,
                player_key=player_key,
                source_type=source_type,
                context_scope=CONTEXT_SCOPE_PERSONAL,
            )
    return {"ok": True, "player_user_id": player_user_id, "chunks_deleted": deleted}


def _record_matches_search(doc: dict[str, Any], needle: str) -> bool:
    if not needle:
        return True
    hay = " ".join(
        [
            str(doc.get("player_user_id") or ""),
            str(doc.get("player_name") or ""),
            str(doc.get("club_name") or ""),
        ]
    ).lower()
    return all(tok in hay for tok in needle.split() if tok)


def list_personal_records(
    db: Session,
    *,
    workspace_id: int,
    search: str = "",
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """List merged personal-context documents (SQL + admin overlay) per player."""
    from backendapi.services.player_memory_service import list_personal_player_keys

    needle = search.strip().lower()
    players = list_personal_player_keys(db=db, workspace_id=workspace_id, search="")
    records: list[dict[str, Any]] = []
    for p in players:
        pk = str(p["player_key"])
        if not pk.isdigit():
            continue
        doc = merged_document_for_player(db, workspace_id=workspace_id, player_user_id=int(pk))
        doc["chunk_count"] = int(p.get("chunk_count") or 0)
        if _record_matches_search(doc, needle):
            records.append(doc)
    records.sort(key=lambda r: (-int(r.get("chunk_count") or 0), str(r.get("player_user_id"))))
    total = len(records)
    page = records[offset : offset + limit]
    return page, total


def personal_context_diagnostics(
    db: Session,
    *,
    workspace_id: int,
    player_user_id: int,
) -> dict[str, Any]:
    """Optional debug: Sportal SQL row vs Firestore chunks (not required for normal admin use)."""
    from backendapi.services.player_context_document import document_from_sql_row, embedding_eligibility_report

    player_key = str(player_user_id)
    sql_row: dict[str, str] | None = None
    sql_error: str | None = None
    settings = get_player_memory_settings(db)
    sql_configured = bool(str(settings.get("sql_database_url") or "").strip())
    if sql_configured:
        try:
            sql_row = _fetch_sql_row(
                database_url=str(settings.get("sql_database_url") or ""),
                sql_query=effective_player_context_sql(settings),
                player_user_id=player_user_id,
            )
        except Exception as exc:  # noqa: BLE001
            sql_error = str(exc)

    sql_doc = document_from_sql_row(sql_row) if sql_row else None
    merged = merged_document_for_player(db, workspace_id=workspace_id, player_user_id=player_user_id)
    eligibility = embedding_eligibility_report(sql_row) if sql_row else None

    chunks, total = list_chunks(
        db=db,
        workspace_id=workspace_id,
        context_scope=CONTEXT_SCOPE_PERSONAL,
        player_key=player_key,
        limit=500,
        offset=0,
    )
    by_type: dict[str, int] = {}
    by_source: dict[str, int] = {}
    for ch in chunks:
        st = str(ch.get("source_type") or "unknown")
        by_source[st] = by_source.get(st, 0) + 1
        meta = ch.get("metadata") if isinstance(ch.get("metadata"), dict) else {}
        ct = str(meta.get("chunk_type") or "unknown")
        by_type[ct] = by_type.get(ct, 0) + 1

    return {
        "ok": sql_error is None,
        "player_user_id": player_user_id,
        "player_key": player_key,
        "sql_configured": sql_configured,
        "sql_error": sql_error,
        "sql_row_found": sql_row is not None,
        "sql_document": sql_doc,
        "merged_document": merged,
        "embedding_eligibility": eligibility,
        "firestore_chunk_total": total,
        "firestore_chunks_by_type": by_type,
        "firestore_chunks_by_source": by_source,
        "hint": (
            "If SQL shows profile/videos/notes but embeddable counts are lower, text was filtered "
            "(HTML, URLs, or junk). Re-run SQL sync to refresh vectors. Full structured data remains "
            "in metadata.player_context on each chunk. Agent feedback returns top-K similar chunks only."
        ),
    }


def list_chunks_with_search(
    db: Session,
    *,
    workspace_id: int,
    player_key: str | None,
    source_type: str | None,
    search: str,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    chunks, total = list_chunks(
        db=db,
        workspace_id=workspace_id,
        context_scope=CONTEXT_SCOPE_PERSONAL,
        player_key=player_key,
        source_type=source_type,
        limit=500,
        offset=0,
    )
    needle = search.strip().lower()
    if needle:
        filtered: list[dict[str, Any]] = []
        for ch in chunks:
            meta = ch.get("metadata") if isinstance(ch.get("metadata"), dict) else {}
            ctx = meta.get("player_context") if isinstance(meta.get("player_context"), dict) else {}
            hay = " ".join(
                [
                    str(ch.get("player_key") or ""),
                    str(ch.get("content") or ""),
                    str(meta.get("player_name") or ""),
                    str(meta.get("club_name") or ""),
                    str(ctx.get("player_name") or ""),
                    str(ctx.get("club_name") or ""),
                    str(ctx.get("player_user_id") or ""),
                ]
            ).lower()
            if all(tok in hay for tok in needle.split() if tok):
                filtered.append(ch)
        chunks = filtered
        total = len(chunks)
    page = chunks[offset : offset + limit]
    return page, total
