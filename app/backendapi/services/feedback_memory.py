from __future__ import annotations

import os
from typing import Any

from sqlalchemy.orm import Session

from backendapi.services.context_scope import CONTEXT_SCOPE_PERSONAL, CONTEXT_SCOPE_SHARED, SHARED_PLAYER_KEY
from backendapi.services.embedding_service import (
    embed_single_query,
    embedding_dimensions,
    embedding_model_name,
)
from backendapi.core.logger import error as log_error
from backendapi.services.player_memory_settings import get_player_memory_settings
from backendapi.services.player_memory_service import (
    format_retrieval_context,
    format_shared_retrieval_context,
    search_similar_chunks,
)
from backendapi.services.vector_store import get_vector_store


def build_feedback_query_text(payload: dict[str, Any]) -> str:
    parts = [
        str(payload.get("player_focus") or "").strip(),
        str(payload.get("coaching_focus") or "").strip(),
        str(payload.get("analysis_scope") or "").strip(),
        str(payload.get("sport") or "").strip(),
    ]
    return "\n".join(p for p in parts if p)


def retrieve_player_memory_context(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    payload: dict[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    """
    Returns (formatted_context_or_none, debug_dict_for_review_ui).
    Merges personal player vectors + global shared context vectors.
    """
    settings = get_player_memory_settings(db)
    backend = str(settings.get("vector_backend") or "firestore").strip().lower()
    top_k = int(settings.get("top_k") or os.getenv("PLAYER_MEMORY_TOP_K", "12"))
    shared_top_k = int(settings.get("shared_top_k") or os.getenv("PLAYER_MEMORY_SHARED_TOP_K", "6"))
    debug: dict[str, Any] = {
        "workspace_id": workspace_id,
        "player_key": (player_key or "").strip(),
        "vector_backend": backend,
        "vector_collections": {
            "personal": settings.get("vector_collection_personal") or "player_personal_context",
            "shared": settings.get("vector_collection_shared") or "shared_context",
        },
        "embedding_model": embedding_model_name(),
        "embedding_dimensions": embedding_dimensions(),
        "top_k_personal": top_k,
        "top_k_shared": shared_top_k,
        "query_text_built_from": "player_focus, coaching_focus, analysis_scope, sport (non-empty lines concatenated)",
        "retrieval_steps": [
            "Build query text from job fields.",
            "Embed query with PLAYER_MEMORY_EMBEDDING_MODEL.",
            "Search personal context collection (scoped to player).",
            "Search shared context collection (global).",
            "Format both blocks for the LLM user message.",
        ],
    }
    try:
        get_vector_store(db)
    except Exception as exc:  # noqa: BLE001
        debug["outcome"] = "vector_store_unavailable"
        debug["error"] = str(exc)
        return None, debug

    qtext = build_feedback_query_text(payload)
    if not qtext.strip():
        qtext = "soccer coaching feedback context"
    debug["query_text_for_embedding"] = qtext
    try:
        qemb = embed_single_query(qtext)
    except Exception as exc:  # noqa: BLE001
        debug["outcome"] = "embedding_failed"
        debug["error"] = str(exc)
        return None, debug
    debug["embedding_vector_length"] = len(qemb) if qemb else 0

    personal_chunks: list[dict[str, Any]] = []
    shared_chunks: list[dict[str, Any]] = []

    if (player_key or "").strip():
        try:
            personal_chunks = search_similar_chunks(
                db=db,
                workspace_id=workspace_id,
                player_key=player_key.strip(),
                query_embedding=qemb,
                top_k=top_k,
                context_scope=CONTEXT_SCOPE_PERSONAL,
            )
        except Exception as exc:  # noqa: BLE001
            debug["personal_search_error"] = str(exc)
            log_error(
                "player_memory_personal_search_failed",
                player_key=player_key.strip(),
                workspace_id=workspace_id,
                error=str(exc),
                hint=(
                    "Firestore may need a composite vector index on workspace_id + player_key. "
                    "See Admin → Player memory → test retrieval or GCP Firestore indexes."
                ),
            )

    try:
        shared_chunks = search_similar_chunks(
            db=db,
            workspace_id=workspace_id,
            player_key=SHARED_PLAYER_KEY,
            query_embedding=qemb,
            top_k=shared_top_k,
            context_scope=CONTEXT_SCOPE_SHARED,
        )
    except Exception as exc:  # noqa: BLE001
        debug["shared_search_error"] = str(exc)

    debug["personal_chunks_returned"] = len(personal_chunks)
    debug["shared_chunks_returned"] = len(shared_chunks)

    if not personal_chunks and not shared_chunks:
        debug["outcome"] = "no_matches"
        return None, debug

    blocks: list[str] = []
    if shared_chunks:
        blocks.append(format_shared_retrieval_context(shared_chunks))
    if personal_chunks:
        blocks.append(format_retrieval_context(personal_chunks))

    debug["outcome"] = "success"
    return "\n\n".join(blocks).strip(), debug
