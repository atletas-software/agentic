from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db import get_db
from app.dependencies.player_memory_auth import get_player_memory_user_id
from app.models.player_memory import PlayerChunk
from app.services.embedding_service import embed_single_query
from app.services.player_memory_settings import get_player_memory_settings
from app.services.player_memory_service import (
    insert_chunks,
    memory_supported,
    search_similar_chunks,
)
from app.services.text_chunking import chunk_text_by_tokens
from app.services.vector_store import get_vector_store
from app.services.snapshot_embed import embed_destination_snapshot_changes
from app.services.sql_player_sync import sync_sql_context_for_workspace
from app.services.workspace_enqueue import (
    enqueue_destination_snapshot_embed_job,
    enqueue_sql_player_sync_job,
    enqueue_sql_player_sync_single_job,
)
from app.services.workspace_service import ensure_workspace

router = APIRouter(prefix="/agents/player-memory", tags=["player-memory"])


class MemorySearchBody(BaseModel):
    player_key: str = Field(..., min_length=1)
    query: str = Field(..., min_length=1)
    top_k: int | None = Field(default=None, ge=1, le=50)


class ManualNoteBody(BaseModel):
    player_key: str = Field(..., min_length=1)
    text: str = Field(..., min_length=1)


class EmbedDocumentBody(BaseModel):
    """Chunk long text and embed as multiple manual chunks for one player."""

    player_key: str = Field(..., min_length=1)
    text: str = Field(..., min_length=1, max_length=500_000)
    label: str = Field(default="", max_length=256)


class SinglePlayerSqlSyncBody(BaseModel):
    player_id: int = Field(..., ge=1)
    first_name: str = Field(..., min_length=1, max_length=200)
    last_name: str = Field(..., min_length=1, max_length=200)


@router.get("/status")
async def player_memory_status(
    user_id: str = Depends(get_player_memory_user_id),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Whether vector memory is active for this deployment."""
    try:
        get_vector_store(db)
        ok = True
    except Exception:
        ok = False
    settings = get_player_memory_settings(db)
    return {
        "memory_enabled": ok,
        "backend": settings.get("vector_backend"),
        "hint": None if ok else "Configure vector backend settings from admin panel.",
    }


@router.get("/stats")
async def player_memory_stats(
    user_id: str = Depends(get_player_memory_user_id),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    ws = ensure_workspace(user_id=user_id, db=db)
    bind = db.get_bind()
    settings = get_player_memory_settings(db)
    backend = str(settings.get("vector_backend") or "pinecone")
    if backend != "pgvector" or bind is None or not memory_supported(bind):
        return {"workspace_id": ws.id, "chunks_by_source": {}, "total_chunks": 0, "backend": backend}
    rows = (
        db.query(PlayerChunk.source_type, func.count(PlayerChunk.id))
        .filter(PlayerChunk.workspace_id == ws.id)
        .group_by(PlayerChunk.source_type)
        .all()
    )
    by_src = {str(r[0]): int(r[1]) for r in rows}
    total = sum(by_src.values())
    return {"workspace_id": ws.id, "chunks_by_source": by_src, "total_chunks": total, "backend": backend}


@router.post("/search")
async def player_memory_search(
    body: MemorySearchBody,
    user_id: str = Depends(get_player_memory_user_id),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    ws = ensure_workspace(user_id=user_id, db=db)
    try:
        get_vector_store(db)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Vector memory is not available: {exc}") from exc
    try:
        qemb = embed_single_query(body.query)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Embedding failed: {exc}") from exc
    chunks = search_similar_chunks(
        db=db,
        workspace_id=ws.id,
        player_key=body.player_key.strip(),
        query_embedding=qemb,
        top_k=body.top_k,
    )
    return {"chunks": chunks}


@router.post("/manual")
async def player_memory_manual_note(
    body: ManualNoteBody,
    user_id: str = Depends(get_player_memory_user_id),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    ws = ensure_workspace(user_id=user_id, db=db)
    try:
        get_vector_store(db)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Vector memory is not available: {exc}") from exc

    ref = f"manual:{uuid.uuid4().hex[:12]}"
    meta = {"origin": "dashboard_manual"}
    n = insert_chunks(
        db=db,
        workspace_id=ws.id,
        player_key=body.player_key.strip(),
        source_type="manual",
        texts_with_refs=[(body.text.strip(), ref, meta)],
    )
    return {"chunks_written": n, "source_ref": ref}


@router.post("/embed-document")
async def player_memory_embed_document(
    body: EmbedDocumentBody,
    user_id: str = Depends(get_player_memory_user_id),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Split large pasted context into token chunks and upsert embeddings (source_type=manual)."""
    ws = ensure_workspace(user_id=user_id, db=db)
    try:
        get_vector_store(db)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Vector memory is not available: {exc}") from exc

    raw = body.text.strip()
    parts = chunk_text_by_tokens(raw)
    if not parts:
        raise HTTPException(status_code=400, detail="No embeddable text after chunking.")

    batch_id = uuid.uuid4().hex[:12]
    label = (body.label or "").strip()
    batch: list[tuple[str, str, dict[str, Any]]] = []
    for i, part in enumerate(parts):
        ref = f"manual_doc:{batch_id}:{i}"
        meta: dict[str, Any] = {
            "origin": "dashboard_embed_document",
            "batch_id": batch_id,
            "part": i,
            "parts_total": len(parts),
        }
        if label:
            meta["label"] = label
        batch.append((part, ref, meta))

    n = insert_chunks(
        db=db,
        workspace_id=ws.id,
        player_key=body.player_key.strip(),
        source_type="manual",
        texts_with_refs=batch,
    )
    return {
        "chunks_written": n,
        "parts_total": len(parts),
        "batch_id": batch_id,
        "player_key": body.player_key.strip(),
    }


@router.post("/sync/sql")
async def trigger_sql_sync(
    user_id: str = Depends(get_player_memory_user_id),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    ws = ensure_workspace(user_id=user_id, db=db)
    ok = enqueue_sql_player_sync_job(ws.id)
    if not ok:
        raise HTTPException(status_code=503, detail="Could not enqueue SQL sync job.")
    return {"queued": True, "workspace_id": ws.id}


@router.post("/sync/sql-player")
async def trigger_sql_sync_single_player(
    body: SinglePlayerSqlSyncBody,
    user_id: str = Depends(get_player_memory_user_id),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    ws = ensure_workspace(user_id=user_id, db=db)
    ok = enqueue_sql_player_sync_single_job(
        ws.id,
        body.player_id,
        body.first_name.strip(),
        body.last_name.strip(),
    )
    if not ok:
        raise HTTPException(status_code=503, detail="Could not enqueue SQL sync job.")
    return {"queued": True, "workspace_id": ws.id, "player_id": body.player_id}


@router.post("/sync/snapshot-embed")
async def trigger_snapshot_embed(
    user_id: str = Depends(get_player_memory_user_id),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    ws = ensure_workspace(user_id=user_id, db=db)
    ok = enqueue_destination_snapshot_embed_job(ws.id)
    if not ok:
        raise HTTPException(status_code=503, detail="Could not enqueue snapshot embed job.")
    return {"queued": True, "workspace_id": ws.id}


@router.post("/run/snapshot-embed")
async def snapshot_embed_inline(
    user_id: str = Depends(get_player_memory_user_id),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Run snapshot diff embedding immediately (dev / small sheets)."""
    ws = ensure_workspace(user_id=user_id, db=db)
    result = embed_destination_snapshot_changes(db=db, workspace_id=ws.id)
    return dict(result)


@router.post("/run/sql-sync")
async def sql_sync_inline(
    user_id: str = Depends(get_player_memory_user_id),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Run SQL sync immediately."""
    ws = ensure_workspace(user_id=user_id, db=db)
    result = sync_sql_context_for_workspace(db=db, workspace_id=ws.id)
    return dict(result)


@router.post("/run/sql-sync-player")
async def sql_sync_single_player_inline(
    body: SinglePlayerSqlSyncBody,
    user_id: str = Depends(get_player_memory_user_id),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    ws = ensure_workspace(user_id=user_id, db=db)
    result = sync_sql_context_for_workspace(
        db=db,
        workspace_id=ws.id,
        single_player={
            "player_id": body.player_id,
            "first_name": body.first_name.strip(),
            "last_name": body.last_name.strip(),
        },
    )
    return dict(result)
