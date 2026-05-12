from __future__ import annotations

import hashlib
import json
from typing import Any, Protocol, Sequence

from pinecone import Pinecone
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models.player_memory import PlayerChunk
from app.services.player_memory_settings import get_player_memory_settings


class VectorStore(Protocol):
    def delete_workspace_source_type(self, *, workspace_id: int, source_type: str) -> int: ...
    def delete_player_source_type(self, *, workspace_id: int, player_key: str, source_type: str) -> int: ...
    def insert_chunks(
        self,
        *,
        workspace_id: int,
        player_key: str,
        source_type: str,
        texts_with_refs: list[tuple[str, str, dict[str, Any]]],
        embeddings: list[list[float]],
        hashes: list[str],
    ) -> int: ...
    def search(
        self,
        *,
        workspace_id: int,
        player_key: str,
        query_embedding: Sequence[float],
        top_k: int,
    ) -> list[dict[str, Any]]: ...


class PgVectorStore:
    def __init__(self, db: Session) -> None:
        self.db = db

    def delete_workspace_source_type(self, *, workspace_id: int, source_type: str) -> int:
        q = delete(PlayerChunk).where(
            PlayerChunk.workspace_id == workspace_id,
            PlayerChunk.source_type == source_type,
        )
        res = self.db.execute(q)
        self.db.commit()
        return res.rowcount or 0

    def delete_player_source_type(self, *, workspace_id: int, player_key: str, source_type: str) -> int:
        q = delete(PlayerChunk).where(
            PlayerChunk.workspace_id == workspace_id,
            PlayerChunk.player_key == player_key,
            PlayerChunk.source_type == source_type,
        )
        res = self.db.execute(q)
        self.db.commit()
        return res.rowcount or 0

    def insert_chunks(
        self,
        *,
        workspace_id: int,
        player_key: str,
        source_type: str,
        texts_with_refs: list[tuple[str, str, dict[str, Any]]],
        embeddings: list[list[float]],
        hashes: list[str],
    ) -> int:
        refs = [t[1] for t in texts_with_refs]
        unique_hashes = list(dict.fromkeys(hashes))
        rows = self.db.execute(
            select(PlayerChunk.source_ref, PlayerChunk.content_hash).where(
                PlayerChunk.workspace_id == workspace_id,
                PlayerChunk.player_key == player_key,
                PlayerChunk.source_type == source_type,
                PlayerChunk.source_ref.in_(refs),
            )
        ).all()
        existing_hashes = {(sr, ch) for sr, ch in rows}
        existing_sql_dedupe: set[str] = set()
        if unique_hashes:
            rows_dedupe = self.db.execute(
                select(PlayerChunk.content_hash, PlayerChunk.metadata_json).where(
                    PlayerChunk.workspace_id == workspace_id,
                    PlayerChunk.player_key == player_key,
                    PlayerChunk.source_type == source_type,
                    PlayerChunk.content_hash.in_(unique_hashes),
                )
            ).all()
        else:
            rows_dedupe = []
        for ch, mj in rows_dedupe:
            try:
                m = json.loads(mj or "{}")
            except json.JSONDecodeError:
                m = {}
            if str(m.get("vector_format") or "") != "player_memory_v2":
                continue
            rid = int(m.get("reviewee_id") or 0)
            eid = int(m.get("evaluation_id") or 0)
            if rid > 0 and eid >= 0:
                existing_sql_dedupe.add(f"{rid}|{eid}|{ch}")
        written = 0
        for i, (text, ref, meta) in enumerate(texts_with_refs):
            if (ref, hashes[i]) in existing_hashes:
                continue
            h = hashes[i]
            dk: str | None = None
            if str(meta.get("vector_format") or "") == "player_memory_v2":
                rid = int(meta.get("reviewee_id") or 0)
                eid = int(meta.get("evaluation_id") or 0)
                if rid > 0 and eid >= 0:
                    dk = f"{rid}|{eid}|{h}"
            if dk is not None and dk in existing_sql_dedupe:
                continue
            body = str(meta.get("chunk_text") or text)
            row = PlayerChunk(
                workspace_id=workspace_id,
                player_key=player_key,
                source_type=source_type,
                source_ref=ref,
                content=body,
                embedding=embeddings[i],
                metadata_json=json.dumps(meta, ensure_ascii=True, default=str),
                content_hash=hashes[i],
            )
            self.db.add(row)
            written += 1
            existing_hashes.add((ref, h))
            if dk is not None:
                existing_sql_dedupe.add(dk)
        self.db.commit()
        return written

    def search(
        self,
        *,
        workspace_id: int,
        player_key: str,
        query_embedding: Sequence[float],
        top_k: int,
    ) -> list[dict[str, Any]]:
        distance = PlayerChunk.embedding.cosine_distance(list(query_embedding))
        stmt = (
            select(PlayerChunk.content, PlayerChunk.metadata_json, PlayerChunk.source_type, PlayerChunk.source_ref)
            .where(
                PlayerChunk.workspace_id == workspace_id,
                PlayerChunk.player_key == player_key,
            )
            .order_by(distance)
            .limit(top_k)
        )
        rows = self.db.execute(stmt).all()
        out: list[dict[str, Any]] = []
        for content, meta_j, st, sr in rows:
            out.append(
                {
                    "content": content,
                    "metadata": json.loads(meta_j or "{}"),
                    "source_type": st,
                    "source_ref": sr,
                }
            )
        return out


class PineconeVectorStore:
    def __init__(self, db: Session) -> None:
        settings = get_player_memory_settings(db)
        api_key = str(settings.get("pinecone_api_key") or "")
        index_name = str(settings.get("pinecone_index") or "")
        namespace = _normalize_namespace(str(settings.get("pinecone_namespace") or "player-memory"))
        if not api_key or not index_name:
            raise RuntimeError("Pinecone is not configured in admin settings.")
        self.namespace = namespace
        self.index = Pinecone(api_key=api_key).Index(index_name)

    def delete_workspace_source_type(self, *, workspace_id: int, source_type: str) -> int:
        try:
            self.index.delete(
                namespace=self.namespace,
                filter={"workspace_id": {"$eq": workspace_id}, "source_type": {"$eq": source_type}},
            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            # Pinecone returns 404 when the namespace has never had vectors yet.
            if "namespace not found" in msg or "(404)" in msg:
                return 0
            raise
        return 0

    def delete_player_source_type(self, *, workspace_id: int, player_key: str, source_type: str) -> int:
        try:
            pid: int | None = int(player_key) if str(player_key).strip().isdigit() else None
            if pid is not None:
                player_clause: dict[str, Any] = {
                    "$or": [
                        {"player_id": {"$eq": pid}},
                        {"player_key": {"$eq": player_key}},
                        {"reviewee_id": {"$eq": pid}},
                    ]
                }
            else:
                player_clause = {"player_key": {"$eq": player_key}}
            self.index.delete(
                namespace=self.namespace,
                filter={
                    "$and": [
                        {"workspace_id": {"$eq": workspace_id}},
                        {"source_type": {"$eq": source_type}},
                        player_clause,
                    ]
                },
            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if "namespace not found" in msg or "(404)" in msg:
                return 0
            raise
        return 0

    def insert_chunks(
        self,
        *,
        workspace_id: int,
        player_key: str,
        source_type: str,
        texts_with_refs: list[tuple[str, str, dict[str, Any]]],
        embeddings: list[list[float]],
        hashes: list[str],
    ) -> int:
        vectors: list[dict[str, Any]] = []
        for i, (text, ref, meta) in enumerate(texts_with_refs):
            if meta.get("vector_format") == "player_memory_v2" and meta.get("vector_id"):
                vid = str(meta["vector_id"])
                md = _pinecone_player_memory_metadata(
                    workspace_id=workspace_id,
                    player_key=player_key,
                    source_type=source_type,
                    source_ref=ref,
                    content_hash=hashes[i],
                    meta=meta,
                )
            else:
                vid = _pinecone_id(
                    workspace_id=workspace_id,
                    player_key=player_key,
                    source_type=source_type,
                    source_ref=ref,
                    content_hash=hashes[i],
                )
                md = {
                    "workspace_id": workspace_id,
                    "player_key": player_key,
                    "source_type": source_type,
                    "source_ref": ref,
                    "content": text,
                    "content_hash": hashes[i],
                    "meta_json": json.dumps(meta, ensure_ascii=True, default=str),
                }
            vectors.append({"id": vid, "values": embeddings[i], "metadata": md})
        if vectors:
            self.index.upsert(vectors=vectors, namespace=self.namespace)
        return len(vectors)

    def search(
        self,
        *,
        workspace_id: int,
        player_key: str,
        query_embedding: Sequence[float],
        top_k: int,
    ) -> list[dict[str, Any]]:
        pid = int(player_key) if str(player_key).strip().isdigit() else None
        if pid is not None:
            player_filt: dict[str, Any] = {
                "$or": [
                    {"player_key": {"$eq": player_key}},
                    {"player_id": {"$eq": pid}},
                    {"reviewee_id": {"$eq": pid}},
                ]
            }
        else:
            player_filt = {"player_key": {"$eq": player_key}}
        result = self.index.query(
            namespace=self.namespace,
            vector=list(query_embedding),
            top_k=top_k,
            include_metadata=True,
            filter={"$and": [{"workspace_id": {"$eq": workspace_id}}, player_filt]},
        )
        matches = getattr(result, "matches", None) or []
        out: list[dict[str, Any]] = []
        for m in matches:
            md = dict((m.metadata or {}))
            meta: dict[str, Any]
            if str(md.get("vector_format") or "") == "player_memory_v2":
                meta = _pinecone_flat_metadata_to_app_meta(md)
            else:
                try:
                    meta = json.loads(str(md.get("meta_json") or "{}"))
                except json.JSONDecodeError:
                    meta = {}
            body = str(md.get("text") or md.get("content") or "")
            out.append(
                {
                    "content": body,
                    "metadata": meta,
                    "source_type": str(md.get("source_type") or ""),
                    "source_ref": str(md.get("source_ref") or ""),
                }
            )
        return out


def get_vector_store(db: Session) -> VectorStore:
    settings = get_player_memory_settings(db)
    backend = str(settings.get("vector_backend") or "pinecone").strip().lower()
    if backend == "pgvector":
        return PgVectorStore(db)
    return PineconeVectorStore(db)


def _pinecone_flat_metadata_to_app_meta(md: dict[str, Any]) -> dict[str, Any]:
    """Rebuild app-side metadata dict from flat Pinecone fields (for RAG context)."""
    tags = md.get("tags")
    if not isinstance(tags, list):
        tags = []
    tags = [str(t) for t in tags]
    return {
        "vector_format": "player_memory_v2",
        "player_id": md.get("player_id"),
        "profile_id": md.get("profile_id"),
        "player_key": md.get("player_key"),
        "player_name": md.get("player_name"),
        "reviewee_id": md.get("reviewee_id"),
        "reviewee_name": md.get("reviewee_name"),
        "evaluation_id": md.get("evaluation_id"),
        "club_name": md.get("club_name"),
        "completion_date": md.get("completion_date"),
        "chunk_index": md.get("chunk_index"),
        "chunk_type": md.get("chunk_type"),
        "source": md.get("source"),
        "chunk_text": md.get("text"),
        "summary": md.get("summary"),
        "tags": tags,
        "form_id": md.get("form_id"),
        "sport": md.get("sport"),
        "age_group": md.get("age_group"),
    }


def _pinecone_truncate_str(value: Any, max_len: int) -> str:
    s = str(value or "")
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _pinecone_player_memory_metadata(
    *,
    workspace_id: int,
    player_key: str,
    source_type: str,
    source_ref: str,
    content_hash: str,
    meta: dict[str, Any],
) -> dict[str, Any]:
    """Flat Pinecone metadata for sql_sync player_memory_v2 (size-bounded)."""
    tags = meta.get("tags")
    if not isinstance(tags, list):
        tags = []
    tags = [str(t) for t in tags if str(t).strip()][:16]
    pid = int(meta.get("player_id") or 0)
    prof = int(meta.get("profile_id") or 0)
    md: dict[str, Any] = {
        "vector_format": "player_memory_v2",
        "workspace_id": workspace_id,
        "player_id": pid,
        "player_key": player_key,
        "player_name": _pinecone_truncate_str(meta.get("player_name"), 256),
        "reviewee_id": int(meta.get("reviewee_id") or 0),
        "reviewee_name": _pinecone_truncate_str(meta.get("reviewee_name"), 256),
        "evaluation_id": int(meta.get("evaluation_id") or 0),
        "club_name": _pinecone_truncate_str(meta.get("club_name"), 256),
        "completion_date": _pinecone_truncate_str(meta.get("completion_date"), 64),
        "chunk_index": int(meta.get("chunk_index") or 0),
        "chunk_type": _pinecone_truncate_str(meta.get("chunk_type"), 64),
        "source": _pinecone_truncate_str(meta.get("source") or "evaluation", 64),
        "text": _pinecone_truncate_str(meta.get("chunk_text"), 32000),
        "summary": _pinecone_truncate_str(meta.get("summary"), 4000),
        "form_id": int(meta.get("form_id") or 0),
        "sport": _pinecone_truncate_str(meta.get("sport"), 128),
        "age_group": _pinecone_truncate_str(meta.get("age_group"), 128),
        "source_type": source_type,
        "source_ref": _pinecone_truncate_str(source_ref, 512),
        "content_hash": content_hash,
    }
    if tags:
        md["tags"] = tags
    if prof > 0:
        md["profile_id"] = prof
    return md


def _pinecone_id(*, workspace_id: int, player_key: str, source_type: str, source_ref: str, content_hash: str) -> str:
    raw = f"{workspace_id}|{player_key}|{source_type}|{source_ref}|{content_hash}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"pm_{digest}"


def _normalize_namespace(raw: str) -> str:
    ns = (raw or "").strip()
    # Pinecone data-plane in older API versions rejects __default__ namespace.
    if not ns or ns == "__default__":
        return "player-memory"
    return ns
