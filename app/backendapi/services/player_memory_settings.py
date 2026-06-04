from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from backendapi.models.player_memory import PlayerMemorySettings
from backendapi.core.logger import info as log_info
from backendapi.services.secure_settings import decrypt_setting_value, encrypt_setting_value


DEFAULT_TABLES = ["profile", "video", "evaluation", "club"]
DEFAULT_SQL_PATH = Path(__file__).resolve().parent / "player_context_sql_default.sql"
_LEGACY_VECTOR_BACKENDS = frozenset({"pinecone", "pgvector", "postgres"})


def _load_default_sql() -> str:
    if DEFAULT_SQL_PATH.is_file():
        return DEFAULT_SQL_PATH.read_text(encoding="utf-8").strip()
    return ""


def _normalize_vector_backend(value: str | None) -> str:
    backend = str(value or "firestore").strip().lower()
    if backend in _LEGACY_VECTOR_BACKENDS:
        return "firestore"
    return backend or "firestore"


def sql_has_single_player_bind(sql_query: str) -> bool:
    sql = sql_query or ""
    if ":player_user_id" in sql:
        return True
    return ":first_name" in sql and ":last_name" in sql


def effective_player_context_sql(settings: dict[str, Any]) -> str:
    """Return SQL for sync; fall back to bundled default when saved query lacks player binds."""
    saved = str(settings.get("sql_query") or "").strip()
    if saved and sql_has_single_player_bind(saved):
        return saved
    default = _load_default_sql()
    if default and sql_has_single_player_bind(default):
        if saved:
            log_info(
                "player_memory_sql_fallback_to_default",
                reason="saved_sql_missing_player_bind",
            )
        return default
    return saved or default


def default_player_memory_settings() -> dict[str, Any]:
    return {
        "vector_backend": _normalize_vector_backend(os.getenv("PLAYER_MEMORY_VECTOR_BACKEND")),
        "gcp_project_id": (os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or "").strip(),
        "gcp_firestore_database": (os.getenv("GCP_FIRESTORE_DATABASE") or "(default)").strip(),
        "vector_collection_personal": (
            os.getenv("FIRESTORE_COLLECTION_PERSONAL") or "player_personal_context"
        ).strip(),
        "vector_collection_shared": (os.getenv("FIRESTORE_COLLECTION_SHARED") or "shared_context").strip(),
        "sql_database_url": (os.getenv("PLAYER_CONTEXT_DATABASE_URL") or "").strip(),
        "sql_query": (os.getenv("PLAYER_CONTEXT_SQL") or _load_default_sql()).strip(),
        "watermark_column": (os.getenv("PLAYER_CONTEXT_WATERMARK_COLUMN") or "").strip(),
        "table_names": DEFAULT_TABLES,
        "player_key_columns": (os.getenv("PLAYER_KEY_COLUMNS") or "player_user_id,reviewee_id,player_id").strip(),
        "top_k": int(os.getenv("PLAYER_MEMORY_TOP_K", "12")),
        "shared_top_k": int(os.getenv("PLAYER_MEMORY_SHARED_TOP_K", "6")),
        "context_max_chars": int(os.getenv("PLAYER_MEMORY_CONTEXT_MAX_CHARS", "12000")),
        "shared_context_max_chars": int(os.getenv("PLAYER_MEMORY_SHARED_CONTEXT_MAX_CHARS", "8000")),
        "chunk_max_tokens": int(os.getenv("PLAYER_MEMORY_CHUNK_MAX_TOKENS", "650")),
        "chunk_overlap_ratio": float(os.getenv("PLAYER_MEMORY_CHUNK_OVERLAP_RATIO", "0.12")),
    }


def _decrypt_settings_payload(encrypted_json: str) -> dict[str, Any] | None:
    try:
        return json.loads(decrypt_setting_value(encrypted_json))
    except RuntimeError:
        return None


def player_memory_settings_decrypt_failed(db: Session) -> bool:
    row = db.query(PlayerMemorySettings).filter(PlayerMemorySettings.singleton_key == "global").one_or_none()
    if row is None or not (row.encrypted_json or "").strip():
        return False
    return _decrypt_settings_payload(row.encrypted_json) is None


def _overlay_env_defaults(settings: dict[str, Any]) -> dict[str, Any]:
    """Prefer .env for GCP/Firestore when DB row is empty or still on legacy (default) database."""
    env = default_player_memory_settings()
    for key in (
        "gcp_project_id",
        "gcp_firestore_database",
        "vector_collection_personal",
        "vector_collection_shared",
    ):
        env_val = str(env.get(key) or "").strip()
        cur = str(settings.get(key) or "").strip()
        if not env_val:
            continue
        if not cur:
            settings[key] = env_val
        elif key == "gcp_firestore_database" and cur == "(default)" and env_val != "(default)":
            settings[key] = env_val
    return settings


def get_player_memory_settings(db: Session) -> dict[str, Any]:
    row = db.query(PlayerMemorySettings).filter(PlayerMemorySettings.singleton_key == "global").one_or_none()
    settings = default_player_memory_settings()
    if row is None or not (row.encrypted_json or "").strip():
        return _overlay_env_defaults(settings)
    payload = _decrypt_settings_payload(row.encrypted_json)
    if payload is None:
        log_info(
            "player_memory_settings_decrypt_failed",
            hint=(
                "PLAYER_MEMORY_SETTINGS_MASTER_KEY does not match the encrypted row in the database. "
                "Using env defaults until you re-save settings from the admin panel."
            ),
        )
        return _overlay_env_defaults(settings)
    settings.update(payload)
    settings.pop("destination_sheet_name", None)
    settings["vector_backend"] = _normalize_vector_backend(settings.get("vector_backend"))
    return _overlay_env_defaults(settings)


def upsert_player_memory_settings(db: Session, *, settings: dict[str, Any], updated_by: str) -> dict[str, Any]:
    row = db.query(PlayerMemorySettings).filter(PlayerMemorySettings.singleton_key == "global").one_or_none()
    merged = get_player_memory_settings(db)
    merged.update(settings)
    merged["vector_backend"] = _normalize_vector_backend(merged.get("vector_backend"))
    merged = _overlay_env_defaults(merged)
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
    masked["sql_database_url"] = _mask_secret(raw_db)
    return masked


def _mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"
