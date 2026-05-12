from __future__ import annotations

import json
import os
from typing import Any

from sqlalchemy.orm import Session

from app.models.player_memory import PlayerMemorySettings
from app.services.secure_settings import decrypt_setting_value, encrypt_setting_value


DEFAULT_TABLES = ["profile", "video", "evaluation", "club"]


def default_player_memory_settings() -> dict[str, Any]:
    return {
        "vector_backend": (os.getenv("PLAYER_MEMORY_VECTOR_BACKEND") or "pinecone").strip().lower(),
        "sql_database_url": (os.getenv("PLAYER_CONTEXT_DATABASE_URL") or "").strip(),
        "sql_query": (os.getenv("PLAYER_CONTEXT_SQL") or "").strip(),
        "watermark_column": (os.getenv("PLAYER_CONTEXT_WATERMARK_COLUMN") or "").strip(),
        "table_names": DEFAULT_TABLES,
        "player_key_columns": (os.getenv("PLAYER_KEY_COLUMNS") or "player_id").strip(),
        "pinecone_api_key": (os.getenv("PINECONE_API_KEY") or "").strip(),
        "pinecone_index": (
            os.getenv("PINECONE_INDEX") or os.getenv("PLAYER_MEMORY_PINECONE_INDEX") or "player_memories"
        ).strip(),
        "pinecone_namespace": (os.getenv("PINECONE_NAMESPACE") or "player-memory").strip(),
        "top_k": int(os.getenv("PLAYER_MEMORY_TOP_K", "12")),
        "context_max_chars": int(os.getenv("PLAYER_MEMORY_CONTEXT_MAX_CHARS", "12000")),
        "chunk_max_tokens": int(os.getenv("PLAYER_MEMORY_CHUNK_MAX_TOKENS", "650")),
        "chunk_overlap_ratio": float(os.getenv("PLAYER_MEMORY_CHUNK_OVERLAP_RATIO", "0.12")),
    }


def get_player_memory_settings(db: Session) -> dict[str, Any]:
    row = db.query(PlayerMemorySettings).filter(PlayerMemorySettings.singleton_key == "global").one_or_none()
    settings = default_player_memory_settings()
    if row is None or not (row.encrypted_json or "").strip():
        return settings
    payload = json.loads(decrypt_setting_value(row.encrypted_json))
    settings.update(payload)
    settings.pop("destination_sheet_name", None)
    return settings


def upsert_player_memory_settings(db: Session, *, settings: dict[str, Any], updated_by: str) -> dict[str, Any]:
    row = db.query(PlayerMemorySettings).filter(PlayerMemorySettings.singleton_key == "global").one_or_none()
    merged = default_player_memory_settings()
    merged.update(settings)
    encrypted = encrypt_setting_value(json.dumps(merged, ensure_ascii=True))
    if row is None:
        row = PlayerMemorySettings(
            singleton_key="global",
            encrypted_json=encrypted,
            updated_by=updated_by,
        )
        db.add(row)
    else:
        row.encrypted_json = encrypted
        row.updated_by = updated_by
    db.commit()
    return merged


def get_masked_player_memory_settings(db: Session) -> dict[str, Any]:
    settings = get_player_memory_settings(db)
    masked = dict(settings)
    raw_db = str(masked.get("sql_database_url") or "")
    raw_pk = str(masked.get("pinecone_api_key") or "")
    masked["sql_database_url"] = _mask_secret(raw_db)
    masked["pinecone_api_key"] = _mask_secret(raw_pk)
    return masked


def _mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"
