from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from typing import Any, Sequence

from google.api_core import retry as api_retry
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1.base_vector_query import DistanceMeasure
from google.cloud.firestore_v1.vector import Vector

from backendapi.services.context_scope import CONTEXT_SCOPE_PERSONAL, CONTEXT_SCOPE_SHARED, normalize_context_scope
from backendapi.services.embedding_service import FIRESTORE_MAX_EMBEDDING_DIM, content_hash, embed_texts
from backendapi.core.logger import error as log_error, info as log_info

# Vector documents are large; small batches + retries avoid Firestore commit deadline exceeded.
_FIRESTORE_WRITE_BATCH_SIZE = int(os.getenv("FIRESTORE_WRITE_BATCH_SIZE", "20"))
_FIRESTORE_COMMIT_RETRY = api_retry.Retry(
    predicate=api_retry.if_exception_type(DeadlineExceeded),
    initial=2.0,
    maximum=30.0,
    multiplier=2.0,
    deadline=120.0,
)


def _prepare_gcp_credentials() -> None:
    """Use VM/workload ADC for Firestore — not the Sheets-only service account JSON."""
    cred_path = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    sheets_path = (os.getenv("DESTINATION_GOOGLE_CREDENTIALS_FILE") or "").strip()
    if not cred_path:
        return
    use_adc = not os.path.isfile(cred_path)
    if not use_adc and sheets_path:
        try:
            use_adc = os.path.samefile(cred_path, sheets_path)
        except OSError:
            use_adc = os.path.basename(cred_path) == os.path.basename(sheets_path)
    if use_adc:
        log_info(
            "gcp_firestore_using_adc",
            hint="Firestore uses the VM/workload service account; Sheets use DESTINATION_GOOGLE_CREDENTIALS_FILE.",
        )
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)


def _gcp_project_id(settings: dict[str, Any] | None = None) -> str:
    if settings:
        configured = str(settings.get("gcp_project_id") or "").strip()
        if configured:
            return configured
    project = (os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or "").strip()
    if not project:
        raise RuntimeError("GCP_PROJECT_ID (or GOOGLE_CLOUD_PROJECT) is required for Firestore vector memory.")
    return project


def _firestore_database(settings: dict[str, Any] | None = None) -> str:
    if settings:
        configured = str(settings.get("gcp_firestore_database") or "").strip()
        if configured:
            return configured
    return (os.getenv("GCP_FIRESTORE_DATABASE") or "(default)").strip() or "(default)"


def _collection_name(settings: dict[str, Any], context_scope: str) -> str:
    scope = normalize_context_scope(context_scope)
    if scope == CONTEXT_SCOPE_SHARED:
        return str(settings.get("vector_collection_shared") or "shared_context")
    return str(settings.get("vector_collection_personal") or "player_personal_context")


def _slim_chunk_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    """Drop duplicated bulky fields before Firestore write."""
    slim = {k: v for k, v in meta.items() if k != "player_context"}
    chunk_text = str(slim.get("chunk_text") or "")
    if len(chunk_text) > 12_000:
        slim["chunk_text"] = chunk_text[:12_000]
    return slim


def _commit_write_batch(batch: firestore.WriteBatch, *, label: str) -> None:
    try:
        _FIRESTORE_COMMIT_RETRY(batch.commit)()
    except DeadlineExceeded as exc:
        log_error("firestore_batch_commit_deadline", label=label, error=str(exc))
        raise


def _doc_id(
    *,
    workspace_id: int,
    player_key: str,
    source_type: str,
    source_ref: str,
    content_hash: str,
) -> str:
    raw = f"{workspace_id}|{player_key}|{source_type}|{source_ref}|{content_hash}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class GcpFirestoreVectorStore:
    """Native GCP vector storage: Firestore collections + vector search (find_nearest)."""

    def __init__(self, *, settings: dict[str, Any]) -> None:
        self.settings = settings
        self._client: firestore.Client | None = None

    @property
    def client(self) -> firestore.Client:
        if self._client is None:
            _prepare_gcp_credentials()
            self._client = firestore.Client(
                project=_gcp_project_id(self.settings),
                database=_firestore_database(self.settings),
            )
        return self._client

    def _collection(self, context_scope: str) -> firestore.CollectionReference:
        return self.client.collection(_collection_name(self.settings, context_scope))

    def _delete_matching(self, query: firestore.Query) -> int:
        deleted = 0
        batch = self.client.batch()
        pending = 0
        for snap in query.stream():
            batch.delete(snap.reference)
            pending += 1
            deleted += 1
            if pending >= 400:
                batch.commit()
                batch = self.client.batch()
                pending = 0
        if pending:
            batch.commit()
        return deleted

    def delete_workspace_source_type(
        self, *, workspace_id: int, source_type: str, context_scope: str = CONTEXT_SCOPE_PERSONAL
    ) -> int:
        scope = normalize_context_scope(context_scope)
        q = (
            self._collection(scope)
            .where(filter=FieldFilter("workspace_id", "==", workspace_id))
            .where(filter=FieldFilter("source_type", "==", source_type))
        )
        return self._delete_matching(q)

    def delete_player_source_type(
        self,
        *,
        workspace_id: int,
        player_key: str,
        source_type: str,
        context_scope: str = CONTEXT_SCOPE_PERSONAL,
    ) -> int:
        scope = normalize_context_scope(context_scope)
        q = (
            self._collection(scope)
            .where(filter=FieldFilter("workspace_id", "==", workspace_id))
            .where(filter=FieldFilter("player_key", "==", player_key))
            .where(filter=FieldFilter("source_type", "==", source_type))
        )
        return self._delete_matching(q)

    def delete_player_source_prefix(
        self,
        *,
        workspace_id: int,
        player_key: str,
        source_type: str,
        source_ref_prefix: str,
        context_scope: str = CONTEXT_SCOPE_PERSONAL,
    ) -> int:
        scope = normalize_context_scope(context_scope)
        q = (
            self._collection(scope)
            .where(filter=FieldFilter("workspace_id", "==", workspace_id))
            .where(filter=FieldFilter("player_key", "==", player_key))
            .where(filter=FieldFilter("source_type", "==", source_type))
        )
        prefix = source_ref_prefix or ""
        deleted = 0
        batch = self.client.batch()
        pending = 0
        for snap in q.stream():
            ref = str((snap.to_dict() or {}).get("source_ref") or "")
            if not ref.startswith(prefix):
                continue
            batch.delete(snap.reference)
            pending += 1
            deleted += 1
            if pending >= 400:
                batch.commit()
                batch = self.client.batch()
                pending = 0
        if pending:
            batch.commit()
        return deleted

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
    ) -> int:
        scope = normalize_context_scope(context_scope)
        coll = self._collection(scope)
        written = 0
        batch = self.client.batch()
        pending = 0
        for emb in embeddings:
            if len(emb) > FIRESTORE_MAX_EMBEDDING_DIM:
                raise RuntimeError(
                    f"Embedding has {len(emb)} dimensions; Firestore allows at most "
                    f"{FIRESTORE_MAX_EMBEDDING_DIM}. Set PLAYER_MEMORY_EMBEDDING_DIM=2048 and re-sync."
                )
        batch_size = max(1, _FIRESTORE_WRITE_BATCH_SIZE)
        for i, (text, ref, meta) in enumerate(texts_with_refs):
            doc_id = _doc_id(
                workspace_id=workspace_id,
                player_key=player_key,
                source_type=source_type,
                source_ref=ref,
                content_hash=hashes[i],
            )
            doc_ref = coll.document(doc_id)
            body = str(meta.get("chunk_text") or text)
            slim_meta = _slim_chunk_metadata(meta)
            payload: dict[str, Any] = {
                "workspace_id": workspace_id,
                "context_scope": scope,
                "player_key": player_key,
                "source_type": source_type,
                "source_ref": ref,
                "content": body,
                "content_hash": hashes[i],
                "metadata": slim_meta,
                "embedding": Vector(list(embeddings[i])),
                "created_at": datetime.now(UTC),
            }
            batch.set(doc_ref, payload)
            pending += 1
            written += 1
            if pending >= batch_size:
                _commit_write_batch(batch, label=f"insert_chunks:{player_key}")
                batch = self.client.batch()
                pending = 0
        if pending:
            _commit_write_batch(batch, label=f"insert_chunks:{player_key}:final")
        return written

    def search(
        self,
        *,
        workspace_id: int,
        player_key: str,
        query_embedding: Sequence[float],
        top_k: int,
        context_scope: str = CONTEXT_SCOPE_PERSONAL,
    ) -> list[dict[str, Any]]:
        scope = normalize_context_scope(context_scope)
        base = (
            self._collection(scope)
            .where(filter=FieldFilter("workspace_id", "==", workspace_id))
            .where(filter=FieldFilter("player_key", "==", player_key))
        )
        vector_query = base.find_nearest(
            vector_field="embedding",
            query_vector=Vector(list(query_embedding)),
            distance_measure=DistanceMeasure.COSINE,
            limit=max(1, int(top_k)),
        )
        out: list[dict[str, Any]] = []
        for snap in vector_query.stream():
            data = snap.to_dict() or {}
            meta = data.get("metadata")
            if not isinstance(meta, dict):
                meta = {}
            out.append(
                {
                    "content": str(data.get("content") or ""),
                    "metadata": meta,
                    "source_type": str(data.get("source_type") or ""),
                    "source_ref": str(data.get("source_ref") or ""),
                }
            )
        return out

    def list_chunks(
        self,
        *,
        workspace_id: int,
        context_scope: str,
        player_key: str | None = None,
        source_type: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        scope = normalize_context_scope(context_scope)
        q: firestore.Query = self._collection(scope).where(
            filter=FieldFilter("workspace_id", "==", workspace_id)
        )
        if player_key:
            q = q.where(filter=FieldFilter("player_key", "==", player_key))
        if source_type:
            q = q.where(filter=FieldFilter("source_type", "==", source_type))
        # Exclude embedding vectors from admin list reads (large, slow over the wire).
        q = q.select(
            [
                "workspace_id",
                "context_scope",
                "player_key",
                "source_type",
                "source_ref",
                "content",
                "content_hash",
                "metadata",
                "created_at",
            ]
        )
        # Sort in memory — Firestore composite indexes are not required for admin listing.
        rows: list[tuple[Any, str, dict[str, Any]]] = []
        for snap in q.stream():
            data = snap.to_dict() or {}
            rows.append((data.get("created_at"), snap.id, data))

        def _sort_key(item: tuple[Any, str, dict[str, Any]]) -> tuple[int, str]:
            created_at, _, data = item
            if hasattr(created_at, "timestamp"):
                ts = float(created_at.timestamp())
            elif isinstance(created_at, str) and created_at:
                ts = 0.0
            else:
                ts = 0.0
            return (-ts, str(data.get("source_ref") or ""))

        rows.sort(key=_sort_key)
        total = len(rows)
        page = rows[offset : offset + limit]
        chunks: list[dict[str, Any]] = []
        for _, doc_id, data in page:
            meta = data.get("metadata")
            if not isinstance(meta, dict):
                meta = {}
            created = data.get("created_at")
            chunks.append(
                {
                    "id": doc_id,
                    "player_key": str(data.get("player_key") or ""),
                    "context_scope": str(data.get("context_scope") or scope),
                    "source_type": str(data.get("source_type") or ""),
                    "source_ref": str(data.get("source_ref") or ""),
                    "content": str(data.get("content") or ""),
                    "content_hash": str(data.get("content_hash") or ""),
                    "metadata": meta,
                    "created_at": created.isoformat() if hasattr(created, "isoformat") else None,
                }
            )
        return chunks, total

    def list_personal_player_keys(
        self,
        *,
        workspace_id: int,
        search: str = "",
    ) -> list[dict[str, Any]]:
        """Distinct player_key values in personal collection (for admin filter UI)."""
        q = self._collection(CONTEXT_SCOPE_PERSONAL).where(
            filter=FieldFilter("workspace_id", "==", workspace_id)
        )
        counts: dict[str, int] = {}
        names: dict[str, str] = {}
        clubs: dict[str, str] = {}
        for snap in q.select(["player_key", "metadata"]).stream():
            data = snap.to_dict() or {}
            pk = str(data.get("player_key") or "").strip()
            if not pk:
                continue
            counts[pk] = counts.get(pk, 0) + 1
            meta = data.get("metadata")
            if isinstance(meta, dict):
                if pk not in names:
                    for key in ("player_name", "playerName", "reviewee_name"):
                        raw = str(meta.get(key) or "").strip()
                        if raw:
                            names[pk] = raw
                            break
                if pk not in clubs:
                    ctx = meta.get("player_context")
                    if isinstance(ctx, dict):
                        club = str(ctx.get("club_name") or "").strip()
                    else:
                        club = str(meta.get("club_name") or "").strip()
                    if club:
                        clubs[pk] = club
        rows = [
            {
                "player_key": pk,
                "chunk_count": n,
                "vector_name": names.get(pk, ""),
                "club_name": clubs.get(pk, ""),
            }
            for pk, n in counts.items()
        ]
        rows.sort(key=lambda r: (-int(r["chunk_count"]), r["player_key"]))
        return rows

    def update_chunk_by_id(
        self,
        *,
        workspace_id: int,
        chunk_id: str,
        context_scope: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        scope = normalize_context_scope(context_scope)
        body = (content or "").strip()
        if not body:
            return False
        for coll_scope in {scope, CONTEXT_SCOPE_PERSONAL, CONTEXT_SCOPE_SHARED}:
            ref = self._collection(coll_scope).document(chunk_id)
            snap = ref.get()
            if not snap.exists:
                continue
            data = snap.to_dict() or {}
            if int(data.get("workspace_id") or 0) != workspace_id:
                return False
            merged_meta = dict(data.get("metadata") or {})
            if isinstance(merged_meta, dict) and metadata:
                merged_meta.update(metadata)
            elif metadata:
                merged_meta = dict(metadata)
            if not isinstance(merged_meta, dict):
                merged_meta = {}
            merged_meta["chunk_text"] = body
            new_hash = content_hash(body)
            new_emb = embed_texts([body])[0]
            ref.update(
                {
                    "content": body,
                    "content_hash": new_hash,
                    "metadata": merged_meta,
                    "embedding": Vector(list(new_emb)),
                }
            )
            return True
        return False

    def delete_chunk_by_id(self, *, workspace_id: int, chunk_id: str, context_scope: str) -> bool:
        scope = normalize_context_scope(context_scope)
        for coll_scope in {scope, CONTEXT_SCOPE_PERSONAL, CONTEXT_SCOPE_SHARED}:
            ref = self._collection(coll_scope).document(chunk_id)
            snap = ref.get()
            if not snap.exists:
                continue
            data = snap.to_dict() or {}
            if int(data.get("workspace_id") or 0) != workspace_id:
                return False
            ref.delete()
            return True
        return False

    def chunk_stats_by_scope(self, *, workspace_id: int) -> dict[str, Any]:
        collections: dict[str, dict[str, int]] = {}
        total = 0
        for scope in (CONTEXT_SCOPE_PERSONAL, CONTEXT_SCOPE_SHARED):
            q = self._collection(scope).where(filter=FieldFilter("workspace_id", "==", workspace_id))
            by_src: dict[str, int] = {}
            for snap in q.stream():
                st = str((snap.to_dict() or {}).get("source_type") or "unknown")
                by_src[st] = by_src.get(st, 0) + 1
                total += 1
            if by_src:
                collections[scope] = by_src
        return {
            "backend": "firestore",
            "gcp_project": _gcp_project_id(self.settings),
            "firestore_database": _firestore_database(self.settings),
            "collections": collections,
            "total_chunks": total,
        }

    def health_check(self) -> dict[str, Any]:
        coll = self._collection(CONTEXT_SCOPE_PERSONAL)
        # Lightweight read — verifies credentials + database reachability.
        next(coll.limit(1).stream(), None)
        return {
            "ok": True,
            "backend": "firestore",
            "project": _gcp_project_id(self.settings),
            "database": _firestore_database(self.settings),
            "personal_collection": _collection_name(self.settings, CONTEXT_SCOPE_PERSONAL),
            "shared_collection": _collection_name(self.settings, CONTEXT_SCOPE_SHARED),
        }
