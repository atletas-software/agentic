from __future__ import annotations

import json
import os
from typing import Any, Sequence

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.services.embedding_service import content_hash, embed_texts
from app.services.player_memory_settings import get_player_memory_settings
from app.services.vector_store import get_vector_store


def memory_supported(engine: Engine) -> bool:
    return engine.dialect.name == "postgresql"


def delete_chunks_for_source_prefix(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    source_type: str,
    source_ref_prefix: str,
) -> int:
    """Remove chunks whose source_ref starts with prefix (before re-ingesting)."""
    # Prefix deletion is currently only supported on pgvector backend.
    settings = get_player_memory_settings(db)
    if str(settings.get("vector_backend") or "pinecone").strip().lower() != "pgvector":
        return 0
    from sqlalchemy import delete
    from app.models.player_memory import PlayerChunk

    q = delete(PlayerChunk).where(
        PlayerChunk.workspace_id == workspace_id,
        PlayerChunk.player_key == player_key,
        PlayerChunk.source_type == source_type,
        PlayerChunk.source_ref.startswith(source_ref_prefix),
    )
    res = db.execute(q)
    db.commit()
    return res.rowcount or 0


def delete_chunks_for_exact_source(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    source_type: str,
    source_ref: str,
) -> int:
    settings = get_player_memory_settings(db)
    if str(settings.get("vector_backend") or "pinecone").strip().lower() != "pgvector":
        return 0
    from sqlalchemy import delete
    from app.models.player_memory import PlayerChunk

    q = delete(PlayerChunk).where(
        PlayerChunk.workspace_id == workspace_id,
        PlayerChunk.player_key == player_key,
        PlayerChunk.source_type == source_type,
        PlayerChunk.source_ref == source_ref,
    )
    res = db.execute(q)
    db.commit()
    return res.rowcount or 0


def delete_chunks_for_workspace_source_type(
    *,
    db: Session,
    workspace_id: int,
    source_type: str,
) -> int:
    """Used for full SQL refresh before re-ingesting all rows."""
    store = get_vector_store(db)
    return store.delete_workspace_source_type(workspace_id=workspace_id, source_type=source_type)


def delete_chunks_for_player_source_type(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    source_type: str,
) -> int:
    """Remove one player's vectors for a source (re-sync before re-ingest)."""
    store = get_vector_store(db)
    return store.delete_player_source_type(
        workspace_id=workspace_id,
        player_key=player_key,
        source_type=source_type,
    )


def insert_chunks(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    source_type: str,
    texts_with_refs: list[tuple[str, str, dict[str, Any]]],
) -> int:
    """
    texts_with_refs: list of (text, source_ref, metadata dict).
    Skips duplicates when content_hash already exists for same source_ref.
    For player_memory_v2 (sql_sync), pgvector also skips rows already stored for the same
    reviewee_id + evaluation_id + content_hash (stable across vector id format changes).
    """
    if not texts_with_refs:
        return 0
    texts = [t[0] for t in texts_with_refs]
    hashes = [content_hash(t) for t in texts]
    refs = [t[1] for t in texts_with_refs]
    metas = [t[2] for t in texts_with_refs]

    vectors = embed_texts(texts)
    store = get_vector_store(db)
    return store.insert_chunks(
        workspace_id=workspace_id,
        player_key=player_key,
        source_type=source_type,
        texts_with_refs=texts_with_refs,
        embeddings=vectors,
        hashes=hashes,
    )


def search_similar_chunks(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    query_embedding: Sequence[float],
    top_k: int | None = None,
) -> list[dict[str, Any]]:
    """Cosine distance ordering; mandatory tenant + player filters."""
    settings = get_player_memory_settings(db)
    k = top_k or int(settings.get("top_k") or 12)
    store = get_vector_store(db)
    return store.search(
        workspace_id=workspace_id,
        player_key=player_key,
        query_embedding=query_embedding,
        top_k=k,
    )


def format_retrieval_context(chunks: list[dict[str, Any]], max_chars: int | None = None) -> str:
    limit = max_chars or int(os.getenv("PLAYER_MEMORY_CONTEXT_MAX_CHARS", "12000"))
    lines: list[str] = []
    lines.append("Retrieved player memory (use as grounding; do not invent facts beyond this text):")
    used = len(lines[0])
    for i, ch in enumerate(chunks, start=1):
        block = f"\n---\n[{i}] ({ch.get('source_type')})\n{ch.get('content', '')}"
        if used + len(block) > limit:
            break
        lines.append(block)
        used += len(block)
    return "\n".join(lines).strip()
