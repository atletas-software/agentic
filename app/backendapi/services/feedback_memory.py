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


def coaching_prompt_from_payload(payload: dict[str, Any]) -> str:
    """Admin session brief from Agent Lab (role, position, session focus)."""
    return str(payload.get("coaching_prompt") or payload.get("coaching_focus") or "").strip()


def build_feedback_query_text(payload: dict[str, Any]) -> str:
    coaching = coaching_prompt_from_payload(payload)
    parts: list[str] = []
    if coaching:
        parts.append(f"Admin coaching directive (role, position, session focus): {coaching}")
    player = str(payload.get("player_focus") or "").strip()
    if player:
        parts.append(player)
    scope = str(payload.get("analysis_scope") or "").strip()
    if scope:
        parts.append(scope)
    sport = str(payload.get("sport") or "").strip()
    if sport:
        parts.append(sport)
    return "\n".join(parts)


def build_shared_context_query_text(payload: dict[str, Any]) -> str:
    """Bias shared-rubric retrieval toward the admin prompt (e.g. Center Defender expectations)."""
    coaching = coaching_prompt_from_payload(payload)
    sport = str(payload.get("sport") or "Soccer").strip()
    if coaching:
        return "\n".join(
            p
            for p in (
                f"Club coaching standards and role expectations: {coaching}",
                sport,
            )
            if p
        )
    return build_feedback_query_text(payload)


def retrieve_feedback_context(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    payload: dict[str, Any],
) -> tuple[str | None, str | None, dict[str, Any]]:
    """
    Returns (personal_context, shared_context, debug_dict).
    Personal and shared Firestore vectors are retrieved separately for distinct prompt sections.
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
        "query_text_built_from": (
            "Admin coaching_prompt first (role/position), then player_focus, analysis_scope, sport"
        ),
        "shared_query_text_built_from": (
            "coaching_prompt-led rubric query when admin prompt is set; else same as personal query"
        ),
        "retrieval_source": "firestore_vectors",
        "retrieval_steps": [
            "Build query text (admin coaching_prompt first for role/position).",
            "Embed personal query; embed coaching-led query for shared rubric when prompt is set.",
            "Search personal context collection (scoped to player).",
            "Search shared context collection (global, synced sheet data).",
            "Format personal and shared blocks separately for the LLM.",
        ],
    }
    try:
        get_vector_store(db)
    except Exception as exc:  # noqa: BLE001
        debug["outcome"] = "vector_store_unavailable"
        debug["error"] = str(exc)
        return None, None, debug

    coaching = coaching_prompt_from_payload(payload)
    qtext = build_feedback_query_text(payload)
    if not qtext.strip():
        qtext = "soccer coaching feedback context"
    shared_qtext = build_shared_context_query_text(payload)
    if not shared_qtext.strip():
        shared_qtext = qtext
    debug["query_text_for_embedding"] = qtext
    debug["shared_query_text_for_embedding"] = shared_qtext
    debug["admin_coaching_prompt"] = coaching or None
    try:
        qemb = embed_single_query(qtext)
        shared_qemb = embed_single_query(shared_qtext) if shared_qtext != qtext else qemb
    except Exception as exc:  # noqa: BLE001
        debug["outcome"] = "embedding_failed"
        debug["error"] = str(exc)
        return None, None, debug
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
            query_embedding=shared_qemb,
            top_k=shared_top_k,
            context_scope=CONTEXT_SCOPE_SHARED,
        )
    except Exception as exc:  # noqa: BLE001
        debug["shared_search_error"] = str(exc)

    debug["personal_chunks_returned"] = len(personal_chunks)
    debug["shared_chunks_returned"] = len(shared_chunks)

    personal_text = format_retrieval_context(personal_chunks) if personal_chunks else None
    shared_text = format_shared_retrieval_context(shared_chunks) if shared_chunks else None

    if not personal_text and not shared_text:
        debug["outcome"] = "no_matches"
        return None, None, debug

    debug["outcome"] = "success"
    return personal_text, shared_text, debug


def retrieve_player_memory_context(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    payload: dict[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    """
    Returns (formatted_context_or_none, debug_dict_for_review_ui).
    Merges personal player vectors + global shared context vectors (legacy combined format).
    """
    personal, shared, debug = retrieve_feedback_context(
        db=db,
        workspace_id=workspace_id,
        player_key=player_key,
        payload=payload,
    )
    if not personal and not shared:
        return None, debug
    blocks = [b for b in (shared, personal) if b]
    return "\n\n".join(blocks).strip(), debug
