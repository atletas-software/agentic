from __future__ import annotations

from typing import Any, Protocol, Sequence

from sqlalchemy.orm import Session

from backendapi.services.context_scope import CONTEXT_SCOPE_PERSONAL
from backendapi.services.gcp_firestore_vector_store import GcpFirestoreVectorStore
from backendapi.services.player_memory_settings import get_player_memory_settings


class VectorStore(Protocol):
    def delete_workspace_source_type(
        self, *, workspace_id: int, source_type: str, context_scope: str = CONTEXT_SCOPE_PERSONAL
    ) -> int: ...
    def delete_player_source_type(
        self,
        *,
        workspace_id: int,
        player_key: str,
        source_type: str,
        context_scope: str = CONTEXT_SCOPE_PERSONAL,
    ) -> int: ...
    def insert_chunks(
        self,
        *,
        workspace_id: int,
        player_key: str,
        source_type: str,
        texts_with_refs: list[tuple[str, str, dict[str, Any]]],
        embeddings: list[list[float]],
        hashes: list[str],
        context_scope: str = CONTEXT_SCOPE_PERSONAL,
    ) -> int: ...
    def search(
        self,
        *,
        workspace_id: int,
        player_key: str,
        query_embedding: Sequence[float],
        top_k: int,
        context_scope: str = CONTEXT_SCOPE_PERSONAL,
    ) -> list[dict[str, Any]]: ...


def get_vector_store(db: Session) -> GcpFirestoreVectorStore:
    settings = get_player_memory_settings(db)
    backend = str(settings.get("vector_backend") or "firestore").strip().lower()
    if backend not in {"firestore", "gcp"}:
        raise RuntimeError(f"Unsupported vector_backend={backend!r}. Use vector_backend=firestore.")
    return GcpFirestoreVectorStore(settings=settings)
