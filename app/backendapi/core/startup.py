from __future__ import annotations

from sqlalchemy import inspect, text

from backendapi.core.logger import error, info
from backendapi.db import Base, engine


def prepare_gcp_firestore_adc() -> None:
    """Unset empty GOOGLE_APPLICATION_CREDENTIALS before any Firestore client is created."""
    from backendapi.services.gcp_firestore_vector_store import _prepare_gcp_credentials

    _prepare_gcp_credentials()


def init_database_schema() -> None:
    """Run once at API startup (not at import) so uvicorn workers can boot before DB is ready."""
    prepare_gcp_firestore_adc()
    Base.metadata.create_all(bind=engine)
    _ensure_player_memory_schema()
    _ensure_sync_enabled_column()
    _ensure_google_oauth_state_code_verifier_column()
    info("database_schema_initialized")


def _ensure_player_memory_schema() -> None:
    from backendapi.models.player_memory import (
        PlayerDirectoryEntry,
        PlayerMemorySettings,
        PlayerPersonalContextOverlay,
        SqlSyncCursor,
    )

    SqlSyncCursor.__table__.create(bind=engine, checkfirst=True)
    PlayerMemorySettings.__table__.create(bind=engine, checkfirst=True)
    PlayerDirectoryEntry.__table__.create(bind=engine, checkfirst=True)
    PlayerPersonalContextOverlay.__table__.create(bind=engine, checkfirst=True)


def _ensure_sync_enabled_column() -> None:
    with engine.begin() as connection:
        inspector = inspect(connection)
        columns = {col["name"] for col in inspector.get_columns("user_sync_settings")}
        if "sync_enabled" in columns:
            return
        if engine.dialect.name == "sqlite":
            connection.execute(
                text(
                    "ALTER TABLE user_sync_settings "
                    "ADD COLUMN sync_enabled BOOLEAN NOT NULL DEFAULT 0"
                )
            )
            return
        connection.execute(
            text(
                "ALTER TABLE user_sync_settings "
                "ADD COLUMN sync_enabled BOOLEAN NOT NULL DEFAULT FALSE"
            )
        )


def _ensure_google_oauth_state_code_verifier_column() -> None:
    with engine.begin() as connection:
        inspector = inspect(connection)
        columns = {col["name"] for col in inspector.get_columns("google_oauth_states")}
        if "code_verifier" in columns:
            return
        connection.execute(text("ALTER TABLE google_oauth_states ADD COLUMN code_verifier TEXT"))
