from __future__ import annotations

import re

from sqlalchemy.orm import Session

from backendapi.models.player_memory import PlayerDirectoryEntry


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


def display_names_by_player_keys(
    *,
    db: Session,
    workspace_id: int,
    player_keys: list[str],
) -> dict[str, str]:
    if not player_keys:
        return {}
    rows = (
        db.query(PlayerDirectoryEntry)
        .filter(
            PlayerDirectoryEntry.workspace_id == workspace_id,
            PlayerDirectoryEntry.player_key.in_(player_keys),
        )
        .all()
    )
    out: dict[str, str] = {}
    for row in rows:
        pk = str(row.player_key or "").strip()
        name = str(row.display_name or "").strip()
        if pk and name and pk not in out:
            out[pk] = name
    return out


def search_players_by_name_fragment(
    *,
    db: Session,
    workspace_id: int,
    fragment: str,
    limit: int = 8,
) -> list[dict[str, str]]:
    """Fuzzy match on display_name / name_key (e.g. 'rayyan' → 'rayyan ahmed')."""
    needle = (fragment or "").strip().lower()
    if len(needle) < 2:
        return []
    rows = (
        db.query(PlayerDirectoryEntry)
        .filter(PlayerDirectoryEntry.workspace_id == workspace_id)
        .all()
    )
    scored: list[tuple[int, dict[str, str]]] = []
    for row in rows:
        display = str(row.display_name or "").strip()
        name_key = str(row.name_key or "").strip()
        pk = str(row.player_key or "").strip()
        if not pk:
            continue
        hay = f"{display} {name_key}".lower()
        if needle not in hay:
            continue
        score = 0
        if name_key == needle or display.lower() == needle:
            score += 100
        elif display.lower().startswith(needle) or name_key.startswith(needle):
            score += 50
        elif f" {needle}" in f" {name_key}" or f" {needle}" in f" {display.lower()}":
            score += 30
        else:
            score += 10
        scored.append((score, {"player_key": pk, "display_name": display or pk}))
    scored.sort(key=lambda x: (-x[0], x[1]["display_name"]))
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for _, item in scored:
        if item["player_key"] in seen:
            continue
        seen.add(item["player_key"])
        out.append(item)
        if len(out) >= max(1, limit):
            break
    return out


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
