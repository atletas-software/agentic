from __future__ import annotations

import re

from sqlalchemy.orm import Session

from app.models.player_memory import PlayerDirectoryEntry


def normalize_name_key(first_name: str, last_name: str) -> str:
    full = f"{(first_name or '').strip()} {(last_name or '').strip()}".strip().lower()
    return re.sub(r"\s+", " ", full)


def upsert_player_directory_entry(
    *,
    db: Session,
    workspace_id: int,
    player_key: str,
    first_name: str,
    last_name: str,
) -> None:
    name_key = normalize_name_key(first_name, last_name)
    if not name_key:
        return
    row = (
        db.query(PlayerDirectoryEntry)
        .filter(
            PlayerDirectoryEntry.workspace_id == workspace_id,
            PlayerDirectoryEntry.name_key == name_key,
        )
        .one_or_none()
    )
    display_name = f"{first_name.strip()} {last_name.strip()}".strip()
    if row is None:
        db.add(
            PlayerDirectoryEntry(
                workspace_id=workspace_id,
                player_key=player_key,
                name_key=name_key,
                display_name=display_name,
            )
        )
    else:
        row.player_key = player_key
        row.display_name = display_name
    # Flush so the next upsert in this transaction sees pending INSERTs for the same name_key.
    db.flush()


def resolve_player_key_by_name(*, db: Session, workspace_id: int, first_name: str, last_name: str) -> str | None:
    name_key = normalize_name_key(first_name, last_name)
    if not name_key:
        return None
    row = (
        db.query(PlayerDirectoryEntry)
        .filter(
            PlayerDirectoryEntry.workspace_id == workspace_id,
            PlayerDirectoryEntry.name_key == name_key,
        )
        .one_or_none()
    )
    if row is None:
        return None
    return row.player_key
