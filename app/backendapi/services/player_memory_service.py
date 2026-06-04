from __future__ import annotations

import os
from typing import Any, Sequence

from sqlalchemy.orm import Session

from backendapi.services.context_scope import CONTEXT_SCOPE_PERSONAL, normalize_context_scope
from backendapi.services.embedding_service import content_hash, embed_texts
from backendapi.services.player_memory_settings import get_player_memory_settings
from backendapi.services.vector_store import get_vector_store


def delete_chunks_for_source_prefix(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    source_type: str,
    source_ref_prefix: str,
    context_scope: str = CONTEXT_SCOPE_PERSONAL,
) -> int:
    store = get_vector_store(db)
    return store.delete_player_source_prefix(
        workspace_id=workspace_id,
        player_key=player_key,
        source_type=source_type,
        source_ref_prefix=source_ref_prefix,
        context_scope=context_scope,
    )


def delete_chunks_for_exact_source(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    source_type: str,
    source_ref: str,
    context_scope: str = CONTEXT_SCOPE_PERSONAL,
) -> int:
    store = get_vector_store(db)
    deleted = 0
    chunks, _ = store.list_chunks(
        workspace_id=workspace_id,
        context_scope=context_scope,
        player_key=player_key,
        source_type=source_type,
        limit=500,
    )
    for ch in chunks:
        if str(ch.get("source_ref") or "") == source_ref:
            if store.delete_chunk_by_id(
                workspace_id=workspace_id,
                chunk_id=str(ch["id"]),
                context_scope=context_scope,
            ):
                deleted += 1
    return deleted


def delete_chunks_for_workspace_source_type(
    *,
    db: Session,
    workspace_id: int,
    source_type: str,
    context_scope: str = CONTEXT_SCOPE_PERSONAL,
) -> int:
    store = get_vector_store(db)
    return store.delete_workspace_source_type(
        workspace_id=workspace_id, source_type=source_type, context_scope=context_scope
    )


def delete_chunks_for_player_source_type(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    source_type: str,
    context_scope: str = CONTEXT_SCOPE_PERSONAL,
) -> int:
    store = get_vector_store(db)
    return store.delete_player_source_type(
        workspace_id=workspace_id,
        player_key=player_key,
        source_type=source_type,
        context_scope=context_scope,
    )


def insert_chunks(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    source_type: str,
    texts_with_refs: list[tuple[str, str, dict[str, Any]]],
    context_scope: str = CONTEXT_SCOPE_PERSONAL,
) -> int:
    if not texts_with_refs:
        return 0
    scope = normalize_context_scope(context_scope)
    texts = [t[0] for t in texts_with_refs]
    hashes = [content_hash(t) for t in texts]
    vectors = embed_texts(texts)
    store = get_vector_store(db)
    return store.insert_chunks(
        workspace_id=workspace_id,
        player_key=player_key,
        source_type=source_type,
        texts_with_refs=texts_with_refs,
        embeddings=vectors,
        hashes=hashes,
        context_scope=scope,
    )


def search_similar_chunks(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    query_embedding: Sequence[float],
    top_k: int | None = None,
    context_scope: str = CONTEXT_SCOPE_PERSONAL,
) -> list[dict[str, Any]]:
    settings = get_player_memory_settings(db)
    k = top_k or int(settings.get("top_k") or 12)
    scope = normalize_context_scope(context_scope)
    store = get_vector_store(db)
    return store.search(
        workspace_id=workspace_id,
        player_key=player_key,
        query_embedding=query_embedding,
        top_k=k,
        context_scope=scope,
    )


def list_chunks(
    *,
    db: Session,
    workspace_id: int,
    context_scope: str,
    player_key: str | None = None,
    source_type: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    store = get_vector_store(db)
    return store.list_chunks(
        workspace_id=workspace_id,
        context_scope=context_scope,
        player_key=player_key,
        source_type=source_type,
        limit=limit,
        offset=offset,
    )


def _player_row_matches_search(row: dict[str, Any], needle: str) -> bool:
    if not needle:
        return True
    hay = " ".join(
        [
            str(row.get("player_key") or ""),
            str(row.get("display_name") or ""),
            str(row.get("vector_name") or ""),
            str(row.get("club_name") or ""),
        ]
    ).lower()
    return all(token in hay for token in needle.split() if token)


def list_personal_player_keys(
    *,
    db: Session,
    workspace_id: int,
    search: str = "",
) -> list[dict[str, Any]]:
    from backendapi.services.player_directory import display_names_by_player_keys

    store = get_vector_store(db)
    rows = store.list_personal_player_keys(workspace_id=workspace_id, search="")
    keys = [str(r["player_key"]) for r in rows]
    directory_names = display_names_by_player_keys(db=db, workspace_id=workspace_id, player_keys=keys)
    needle = search.strip().lower()
    out: list[dict[str, Any]] = []
    for row in rows:
        pk = str(row["player_key"])
        display = directory_names.get(pk) or str(row.get("vector_name") or "").strip()
        enriched = {
            "player_key": pk,
            "chunk_count": int(row.get("chunk_count") or 0),
            "display_name": display,
            "club_name": str(row.get("club_name") or "").strip(),
        }
        if _player_row_matches_search(enriched, needle):
            out.append(enriched)
    return out


def delete_chunk_by_id(*, db: Session, workspace_id: int, chunk_id: str, context_scope: str) -> bool:
    store = get_vector_store(db)
    return store.delete_chunk_by_id(
        workspace_id=workspace_id,
        chunk_id=chunk_id,
        context_scope=context_scope,
    )


def update_chunk_by_id(
    *,
    db: Session,
    workspace_id: int,
    chunk_id: str,
    context_scope: str,
    content: str,
    metadata: dict[str, Any] | None = None,
) -> bool:
    store = get_vector_store(db)
    return store.update_chunk_by_id(
        workspace_id=workspace_id,
        chunk_id=chunk_id,
        context_scope=context_scope,
        content=content,
        metadata=metadata,
    )


def chunk_stats_by_scope(*, db: Session, workspace_id: int) -> dict[str, Any]:
    store = get_vector_store(db)
    return store.chunk_stats_by_scope(workspace_id=workspace_id)


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


def format_shared_retrieval_context(chunks: list[dict[str, Any]], max_chars: int | None = None) -> str:
    limit = max_chars or int(os.getenv("PLAYER_MEMORY_SHARED_CONTEXT_MAX_CHARS", "8000"))
    lines: list[str] = ["Shared organization coaching context (applies to all players):"]
    used = len(lines[0])
    for i, ch in enumerate(chunks, start=1):
        meta = ch.get("metadata") if isinstance(ch.get("metadata"), dict) else {}
        title = str(meta.get("title") or "").strip()
        position = str(meta.get("position") or "").strip()
        category = str(meta.get("category") or "").strip()
        header_bits = [b for b in (position, category, title) if b]
        header = " · ".join(header_bits) if header_bits else f"Entry {i}"
        block = f"\n---\n[{i}] {header}\n{ch.get('content', '')}"
        if used + len(block) > limit:
            break
        lines.append(block)
        used += len(block)
    return "\n".join(lines).strip()
