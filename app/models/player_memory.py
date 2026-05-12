from __future__ import annotations

from datetime import UTC, datetime

from pgvector.sqlalchemy import Vector
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class PlayerChunk(Base):
    """Per-player text chunks with embeddings for RAG (Postgres + pgvector)."""

    __tablename__ = "player_chunks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), nullable=False, index=True)
    player_key: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    source_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source_ref: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    content: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[list[float]] = mapped_column(Vector(3072), nullable=False)
    metadata_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class SqlSyncCursor(Base):
    """Watermark for incremental reads from external PLAYER_CONTEXT database."""

    __tablename__ = "sql_sync_cursors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), nullable=False, index=True)
    source_name: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    watermark_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    __table_args__ = (UniqueConstraint("workspace_id", "source_name", name="uq_sql_sync_cursor_ws_source"),)


class PlayerMemorySettings(Base):
    """Global encrypted admin-managed settings for player memory pipeline."""

    __tablename__ = "player_memory_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    singleton_key: Mapped[str] = mapped_column(String(64), nullable=False, default="global", unique=True, index=True)
    encrypted_json: Mapped[str] = mapped_column(Text, nullable=False, default="")
    updated_by: Mapped[str] = mapped_column(String(255), nullable=False, default="system")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


class PlayerDirectoryEntry(Base):
    """Workspace-local mapping from player name to canonical player_key/id."""

    __tablename__ = "player_directory_entries"
    __table_args__ = (UniqueConstraint("workspace_id", "name_key", name="uq_player_directory_ws_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    workspace_id: Mapped[int] = mapped_column(ForeignKey("workspaces.id"), nullable=False, index=True)
    player_key: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    name_key: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    display_name: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )
