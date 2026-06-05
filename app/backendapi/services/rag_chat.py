from __future__ import annotations

import os
import re
from typing import Any, Literal

from openai import OpenAI
from sqlalchemy.orm import Session

from backendapi.services.context_scope import CONTEXT_SCOPE_PERSONAL, CONTEXT_SCOPE_SHARED, SHARED_PLAYER_KEY
from backendapi.services.embedding_service import embed_single_query, embedding_dimensions, embedding_model_name
from backendapi.services.player_directory import (
    display_names_by_player_keys,
    search_players_by_name_fragment,
)
from backendapi.services.player_memory_service import (
    format_retrieval_context,
    format_shared_retrieval_context,
    list_chunks,
    list_personal_player_keys,
    search_similar_chunks,
)
from backendapi.services.player_memory_settings import get_player_memory_settings
from backendapi.services.vector_store import get_vector_store

ChatRole = Literal["user", "assistant"]

_DEFAULT_SYSTEM = """You are Athlete Agent, an expert soccer coaching analyst for club staff.

## Your knowledge sources (in order of use)
1. **Shared coaching context** — organization-wide coaching guidelines, position standards, and evaluation criteria. Use these as the authoritative rubric when validating whether a player's actions meet expectations and when structuring new feedback.
2. **Personal player memory** — profile text, video summaries, coach evaluation notes, video coaching annotations, and other player-specific chunks from the vector database.

## How to answer
- Ground every player-specific claim in the retrieved personal memory. Quote or paraphrase concrete evidence (profile, videos, prior notes).
- When generating **new feedback**, produce a structured coaching report:
  - **Player identity** (name, club if known)
  - **Stated vs observed role** — if the user claims a position (e.g. "center defender"), compare against profile/data; note agreement or conflict explicitly.
  - **Evidence from prior feedback & videos** — summarize recurring themes from stored notes and annotations.
  - **Validation against shared guidelines** — assess whether behavior aligns with shared coaching standards for that role.
  - **New coaching feedback** — actionable, evidence-based recommendations tied to the above (do not invent match events or stats not in context).
- When asked for **player details**, synthesize profile, videos, and feedback notes into a clear overview.
- Be concise, practical, and coaching-oriented.

## Critical rules
- If personal memory chunks are present in context, the player **exists** in the knowledge base — never claim they are missing or unknown.
- Only say player-specific data was not found after you have checked the provided context and it truly contains no personal chunks for that player.
- Shared context may be present even when personal data is thin; still apply shared guidelines and state what personal evidence is lacking.
- Do not invent video timestamps, match results, or evaluations not supported by retrieved text.
- Future phases will add automated video (YOLO) analysis; until then, rely on stored summaries and annotations only."""


_FULL_PLAYER_QUERY_HINTS = (
    "feedback",
    "profile",
    "detail",
    "video",
    "summary",
    "position",
    "plays as",
    "plays ",
    "generate",
    "report",
    "who is",
    "tell me about",
    "validate",
    "coaching",
    "midfielder",
    "defender",
    "forward",
    "goalkeeper",
    "winger",
    "striker",
)


def _chunk_sources(chunks: list[dict[str, Any]], *, limit: int = 16) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for ch in chunks[:limit]:
        meta = ch.get("metadata") if isinstance(ch.get("metadata"), dict) else {}
        chunk_type = str(meta.get("chunk_type") or ch.get("source_ref") or "")
        out.append(
            {
                "source_type": str(ch.get("source_type") or ""),
                "source_ref": str(ch.get("source_ref") or ""),
                "chunk_type": chunk_type,
                "preview": str(ch.get("content") or "")[:280],
            }
        )
    return out


def _chunk_dedupe_key(ch: dict[str, Any]) -> str:
    return "|".join(
        [
            str(ch.get("source_type") or ""),
            str(ch.get("source_ref") or ""),
            str(ch.get("content") or "")[:200],
        ]
    )


def _normalize_chunk_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "content": str(row.get("content") or ""),
        "metadata": row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
        "source_type": str(row.get("source_type") or ""),
        "source_ref": str(row.get("source_ref") or ""),
    }


def _merge_chunks(
    primary: list[dict[str, Any]],
    supplemental: list[dict[str, Any]],
    *,
    max_total: int,
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    merged: list[dict[str, Any]] = []
    for ch in primary + supplemental:
        key = _chunk_dedupe_key(ch)
        if key in seen:
            continue
        seen.add(key)
        merged.append(ch)
        if len(merged) >= max_total:
            break
    return merged


def _name_candidates_from_message(message: str) -> list[str]:
    text = (message or "").strip()
    if not text:
        return []
    candidates: list[str] = []
    for match in re.finditer(r"\b([A-Z][a-z]{2,})\b", text):
        candidates.append(match.group(1).lower())
    first_word = re.match(r"^([A-Za-z]{3,})\b", text)
    if first_word:
        candidates.append(first_word.group(1).lower())
    seen: set[str] = set()
    out: list[str] = []
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


def _query_needs_full_player_context(query_text: str) -> bool:
    ql = (query_text or "").lower()
    return any(hint in ql for hint in _FULL_PLAYER_QUERY_HINTS)


def resolve_player_for_rag(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    message: str,
) -> dict[str, Any]:
    """Resolve Sportal player_key + display name from UI selection or name in the message."""
    pk = (player_key or "").strip()
    if pk:
        names = display_names_by_player_keys(db=db, workspace_id=workspace_id, player_keys=[pk])
        return {
            "player_key": pk,
            "display_name": names.get(pk) or "",
            "resolution": "ui_selection",
        }

    for fragment in _name_candidates_from_message(message):
        directory_matches = search_players_by_name_fragment(
            db=db, workspace_id=workspace_id, fragment=fragment, limit=3
        )
        if len(directory_matches) == 1:
            m = directory_matches[0]
            return {
                "player_key": m["player_key"],
                "display_name": m.get("display_name") or "",
                "resolution": "directory_name_match",
                "matched_fragment": fragment,
            }

        firestore_players = list_personal_player_keys(db=db, workspace_id=workspace_id, search=fragment)
        if len(firestore_players) == 1:
            p = firestore_players[0]
            return {
                "player_key": str(p.get("player_key") or ""),
                "display_name": str(p.get("display_name") or p.get("vector_name") or ""),
                "resolution": "firestore_name_match",
                "matched_fragment": fragment,
            }

    return {"player_key": "", "display_name": "", "resolution": "unresolved"}


def _build_embedding_query(*, message: str, display_name: str, player_key: str) -> str:
    parts: list[str] = []
    if display_name.strip():
        parts.append(f"Player: {display_name.strip()}")
    if player_key.strip():
        parts.append(f"player_key: {player_key.strip()}")
    parts.append(message.strip())
    return "\n".join(p for p in parts if p)


def _list_chunks_as_search_rows(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    context_scope: str,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    rows, total = list_chunks(
        db=db,
        workspace_id=workspace_id,
        context_scope=context_scope,
        player_key=player_key,
        limit=limit,
        offset=0,
    )
    return [_normalize_chunk_row(r) for r in rows], total


def retrieve_knowledge_for_query(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    query_text: str,
    display_name: str = "",
    include_shared: bool = True,
) -> tuple[str | None, list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Hybrid retrieval: vector search + full chunk listing for comprehensive player coverage."""
    settings = get_player_memory_settings(db)
    top_k = int(
        os.getenv("AGENT_LAB_RAG_TOP_K_PERSONAL")
        or settings.get("top_k")
        or os.getenv("PLAYER_MEMORY_TOP_K", "16")
    )
    shared_top_k = int(
        os.getenv("AGENT_LAB_RAG_TOP_K_SHARED")
        or settings.get("shared_top_k")
        or os.getenv("PLAYER_MEMORY_SHARED_TOP_K", "10")
    )
    max_personal = int(os.getenv("AGENT_LAB_RAG_MAX_PERSONAL_CHUNKS", "40"))
    max_shared = int(os.getenv("AGENT_LAB_RAG_MAX_SHARED_CHUNKS", "20"))

    pk = (player_key or "").strip()
    qtext = _build_embedding_query(message=query_text, display_name=display_name, player_key=pk)
    if not qtext.strip():
        qtext = "soccer player coaching context profile feedback videos"

    debug: dict[str, Any] = {
        "workspace_id": workspace_id,
        "player_key": pk,
        "display_name": display_name,
        "query_text_for_embedding": qtext,
        "embedding_model": embedding_model_name(),
        "embedding_dimensions": embedding_dimensions(),
        "top_k_personal": top_k,
        "top_k_shared": shared_top_k,
        "retrieval_strategy": [],
    }

    try:
        get_vector_store(db)
    except Exception as exc:  # noqa: BLE001
        debug["outcome"] = "vector_store_unavailable"
        debug["error"] = str(exc)
        return None, [], [], debug

    try:
        qemb = embed_single_query(qtext)
    except Exception as exc:  # noqa: BLE001
        debug["outcome"] = "embedding_failed"
        debug["error"] = str(exc)
        return None, [], [], debug

    personal: list[dict[str, Any]] = []
    shared: list[dict[str, Any]] = []
    stored_personal_total = 0
    stored_shared_total = 0

    if pk:
        try:
            personal = search_similar_chunks(
                db=db,
                workspace_id=workspace_id,
                player_key=pk,
                query_embedding=qemb,
                top_k=top_k,
                context_scope=CONTEXT_SCOPE_PERSONAL,
            )
            debug["retrieval_strategy"].append("vector_search_personal")
        except Exception as exc:  # noqa: BLE001
            debug["personal_search_error"] = str(exc)

        need_broad = _query_needs_full_player_context(query_text) or len(personal) < min(3, top_k)
        try:
            stored_rows, stored_personal_total = _list_chunks_as_search_rows(
                db=db,
                workspace_id=workspace_id,
                player_key=pk,
                context_scope=CONTEXT_SCOPE_PERSONAL,
                limit=max_personal,
            )
            debug["stored_personal_chunk_total"] = stored_personal_total
            if stored_personal_total > 0 and need_broad:
                personal = _merge_chunks(personal, stored_rows, max_total=max_personal)
                debug["retrieval_strategy"].append("list_all_personal_chunks")
        except Exception as exc:  # noqa: BLE001
            debug["list_personal_error"] = str(exc)

    if include_shared:
        shared_guidelines_query = (
            f"{qtext}\n\nOrganization coaching guidelines position standards tactical responsibilities evaluation"
        )
        try:
            shared_emb = embed_single_query(shared_guidelines_query)
        except Exception:  # noqa: BLE001
            shared_emb = qemb
        try:
            shared = search_similar_chunks(
                db=db,
                workspace_id=workspace_id,
                player_key=SHARED_PLAYER_KEY,
                query_embedding=shared_emb,
                top_k=shared_top_k,
                context_scope=CONTEXT_SCOPE_SHARED,
            )
            debug["retrieval_strategy"].append("vector_search_shared")
        except Exception as exc:  # noqa: BLE001
            debug["shared_search_error"] = str(exc)

        try:
            stored_shared, stored_shared_total = _list_chunks_as_search_rows(
                db=db,
                workspace_id=workspace_id,
                player_key=SHARED_PLAYER_KEY,
                context_scope=CONTEXT_SCOPE_SHARED,
                limit=max_shared,
            )
            debug["stored_shared_chunk_total"] = stored_shared_total
            if stored_shared_total > 0 and (len(shared) < min(2, shared_top_k) or _query_needs_full_player_context(query_text)):
                shared = _merge_chunks(shared, stored_shared, max_total=max_shared)
                debug["retrieval_strategy"].append("list_all_shared_chunks")
        except Exception as exc:  # noqa: BLE001
            debug["list_shared_error"] = str(exc)

    debug["personal_chunks_returned"] = len(personal)
    debug["shared_chunks_returned"] = len(shared)

    if not personal and not shared:
        if pk and stored_personal_total == 0:
            debug["outcome"] = "player_not_in_database"
            debug["hint"] = f"No chunks stored for player_key={pk}. Sync player memory first."
        elif not pk:
            debug["outcome"] = "player_unresolved"
            debug["hint"] = "Select a synced player or mention a recognizable player name in your message."
        else:
            debug["outcome"] = "no_matches"
        return None, personal, shared, debug

    blocks: list[str] = []
    if shared:
        blocks.append(format_shared_retrieval_context(shared))
    if personal:
        identity = ""
        if display_name.strip() or pk:
            identity = f"Resolved player: {display_name.strip() or 'unknown name'} (player_key={pk})"
        player_block = format_retrieval_context(personal)
        if identity:
            player_block = f"{identity}\n\n{player_block}"
        blocks.append(player_block)

    debug["outcome"] = "success"
    return "\n\n".join(blocks).strip(), personal, shared, debug


def _context_instruction(
    *,
    knowledge_context: str | None,
    retrieval_debug: dict[str, Any],
    player_key: str,
    display_name: str,
) -> str:
    if knowledge_context and knowledge_context.strip():
        return f"Retrieved knowledge base (ground truth for this turn):\n\n{knowledge_context.strip()}"

    outcome = str(retrieval_debug.get("outcome") or "")
    if outcome == "player_not_in_database":
        return (
            f"No personal memory chunks exist in Firestore for player_key={player_key}. "
            "Tell the user to sync this player in Admin → Player memory. "
            "You may still answer using shared coaching guidelines if those were retrieved."
        )
    if outcome == "player_unresolved":
        return (
            "Could not resolve which player the user means. Ask them to select a synced player from the dropdown "
            "or include the player's name as stored in Sportal."
        )
    if display_name.strip() or player_key.strip():
        return (
            f"Retrieval found no chunks for {display_name.strip() or player_key.strip()}. "
            "State that clearly; do not invent player-specific facts."
        )
    return (
        "No knowledge base chunks were retrieved. Answer only from general coaching principles and "
        "state that player-specific data was not found."
    )


def generate_rag_chat_reply(
    *,
    message: str,
    knowledge_context: str | None,
    history: list[dict[str, str]],
    player_key: str = "",
    display_name: str = "",
    retrieval_debug: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Call OpenAI chat completions with RAG context and optional conversation history."""
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for Agent Lab chat.")

    model = (os.getenv("AGENT_LAB_CHAT_MODEL") or os.getenv("PLAYER_MEMORY_SUMMARY_MODEL") or "gpt-4.1-mini").strip()
    system = (os.getenv("AGENT_LAB_CHAT_SYSTEM_PROMPT") or _DEFAULT_SYSTEM).strip()

    identity_bits: list[str] = []
    if display_name.strip():
        identity_bits.append(f"display_name: {display_name.strip()}")
    if player_key.strip():
        identity_bits.append(f"player_key (Sportal id): {player_key.strip()}")
    if identity_bits:
        system += "\n\n## Active player for this conversation\n" + "\n".join(identity_bits)

    messages: list[dict[str, str]] = [{"role": "system", "content": system}]
    messages.append(
        {
            "role": "system",
            "content": _context_instruction(
                knowledge_context=knowledge_context,
                retrieval_debug=retrieval_debug or {},
                player_key=player_key,
                display_name=display_name,
            ),
        }
    )

    for turn in history[-10:]:
        role = str(turn.get("role") or "").strip()
        content = str(turn.get("content") or "").strip()
        if role in ("user", "assistant") and content:
            messages.append({"role": role, "content": content[:8000]})

    messages.append({"role": "user", "content": message.strip()[:4000]})

    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=float(os.getenv("AGENT_LAB_CHAT_TEMPERATURE", "0.35")),
        max_tokens=int(os.getenv("AGENT_LAB_CHAT_MAX_TOKENS", "2000")),
    )
    choice = resp.choices[0] if resp.choices else None
    reply = (choice.message.content if choice and choice.message else "") or ""
    usage = resp.usage
    meta: dict[str, Any] = {
        "model": model,
        "finish_reason": choice.finish_reason if choice else None,
        "prompt_tokens": usage.prompt_tokens if usage else None,
        "completion_tokens": usage.completion_tokens if usage else None,
    }
    return reply.strip(), meta


def run_rag_chat(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    message: str,
    history: list[dict[str, str]] | None = None,
    include_shared: bool = True,
) -> dict[str, Any]:
    """Full RAG chat turn: resolve player, retrieve from Firestore, then generate OpenAI reply."""
    hist = history or []
    resolved = resolve_player_for_rag(
        db=db,
        workspace_id=workspace_id,
        player_key=player_key,
        message=message,
    )
    effective_pk = str(resolved.get("player_key") or "").strip()
    display_name = str(resolved.get("display_name") or "").strip()

    context, personal, shared, retrieval_debug = retrieve_knowledge_for_query(
        db=db,
        workspace_id=workspace_id,
        player_key=effective_pk,
        query_text=message,
        display_name=display_name,
        include_shared=include_shared,
    )
    retrieval_debug["player_resolution"] = resolved

    reply, llm_meta = generate_rag_chat_reply(
        message=message,
        knowledge_context=context,
        history=hist,
        player_key=effective_pk,
        display_name=display_name,
        retrieval_debug=retrieval_debug,
    )
    sources = _chunk_sources(personal + shared)
    return {
        "reply": reply,
        "resolved_player": {
            "player_key": effective_pk,
            "display_name": display_name,
            "resolution": resolved.get("resolution"),
        },
        "retrieval": {
            "outcome": retrieval_debug.get("outcome"),
            "personal_chunks": len(personal),
            "shared_chunks": len(shared),
            "stored_personal_total": retrieval_debug.get("stored_personal_chunk_total"),
            "sources": sources,
            "debug": retrieval_debug,
        },
        "llm": llm_meta,
        "had_knowledge_context": bool(context),
    }
