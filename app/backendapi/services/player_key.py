from __future__ import annotations

import hashlib
import os
import re
from typing import Iterable


def _normalize_header(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().lower())


def player_key_column_names() -> list[str]:
    raw = os.getenv(
        "PLAYER_KEY_COLUMNS",
        "first and last name,team color,team number",
    )
    return [c.strip().lower() for c in raw.split(",") if c.strip()]


def row_dict_from_sheet(headers: list[str], row: list[str]) -> dict[str, str]:
    hnorm = [_normalize_header(h) for h in headers]
    rd: dict[str, str] = {}
    for i, key in enumerate(hnorm):
        if i < len(row):
            rd[key] = str(row[i]).strip()
        else:
            rd[key] = ""
    return rd


def row_dict_from_sql(labels: list[str], row: tuple) -> dict[str, str]:
    rd: dict[str, str] = {}
    for i, label in enumerate(labels):
        key = _normalize_header(label)
        rd[key] = str(row[i]).strip() if i < len(row) and row[i] is not None else ""
    return rd


def normalize_player_key(row: dict[str, str], columns: Iterable[str] | None = None) -> str:
    """Stable composite key from configured logical columns (normalized header names)."""
    cols = list(columns) if columns is not None else player_key_column_names()
    parts: list[str] = []
    for col in cols:
        key = col.strip().lower()
        val = str(row.get(key, "")).strip().lower()
        parts.append(val or "_")
    joined = "|".join(parts)
    max_len = int(os.getenv("PLAYER_KEY_MAX_DISPLAY_LEN", "256"))
    if len(joined) <= max_len:
        return joined
    digest = hashlib.sha256(joined.encode("utf-8")).hexdigest()[:24]
    return f"h:{digest}"
