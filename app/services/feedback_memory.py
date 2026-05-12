from __future__ import annotations

import os
from typing import Any

from sqlalchemy.orm import Session

from app.services.embedding_service import (
    embed_single_query,
    embedding_dimensions,
    embedding_model_name,
)
from app.services.player_memory_settings import get_player_memory_settings
from app.services.player_memory_service import format_retrieval_context, search_similar_chunks
from app.services.vector_store import get_vector_store


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
    debug_dict is always returned so the feedback agent can show how vector memory was fetched.
    """
    settings = get_player_memory_settings(db)
    backend = str(settings.get("vector_backend") or "pinecone").strip().lower()
    top_k = int(settings.get("top_k") or os.getenv("PLAYER_MEMORY_TOP_K", "12"))
    debug: dict[str, Any] = {
        "workspace_id": workspace_id,
        "player_key": (player_key or "").strip(),
        "vector_backend": backend,
        "embedding_model": embedding_model_name(),
        "embedding_dimensions": embedding_dimensions(),
        "top_k_requested": top_k,
        "query_text_built_from": "player_focus, coaching_focus, analysis_scope, sport (non-empty lines concatenated)",
        "metadata_filter_summary": (
            f"Vectors filtered to workspace_id={workspace_id} and this player "
            "(metadata: player_key match; if key is numeric, also player_id / reviewee_id for v2 chunks)."
        ),
        "retrieval_steps": [
            "Build query text from job fields (see query_text_for_embedding).",
            "Embed query with PLAYER_MEMORY_EMBEDDING_MODEL.",
            "Similarity search (cosine) in vector store with tenant + player filters.",
            "If chunks exist, format text block → sent as player_memory_context in the LLM user message.",
        ],
    }
    try:
        get_vector_store(db)
    except Exception as exc:  # noqa: BLE001
        debug["outcome"] = "vector_store_unavailable"
        debug["error"] = str(exc)
        return None, debug
    if not (player_key or "").strip():
        debug["outcome"] = "skipped_no_player_key"
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
    try:
        chunks = search_similar_chunks(
            db=db,
            workspace_id=workspace_id,
            player_key=player_key.strip(),
            query_embedding=qemb,
            top_k=top_k,
        )
    except Exception as exc:  # noqa: BLE001
        debug["outcome"] = "search_failed"
        debug["error"] = str(exc)
        return None, debug
    debug["chunks_returned"] = len(chunks)
    if not chunks:
        debug["outcome"] = "no_matches"
        return None, debug
    debug["outcome"] = "success"
    return format_retrieval_context(chunks), debug
