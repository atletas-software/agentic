from __future__ import annotations

import hashlib
import re
from typing import Any

from backendapi.services.context_scope import CONTEXT_SCOPE_SHARED
from backendapi.services.text_chunking import chunk_text_by_tokens

# Canonical shared-context columns (sheet header → internal key).
SHARED_CONTEXT_FIELDS: tuple[tuple[str, str], ...] = (
    ("position", "Position"),
    ("category", "Category"),
    ("part_of_the_field", "Part of the Field"),
    ("title", "Title"),
    ("description", "Description"),
)

SHARED_CONTEXT_VECTOR_FORMAT = "shared_context_v2"


def _normalize_header(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().lower())


_HEADER_ALIASES: dict[str, str] = {
    "part of field": "part_of_the_field",
    "field part": "part_of_the_field",
    "part of the pitch": "part_of_the_field",
    "pos": "position",
    "cat": "category",
    "desc": "description",
}


def header_to_shared_field(header: str) -> str | None:
    n = _normalize_header(header)
    if not n:
        return None
    for field_key, label in SHARED_CONTEXT_FIELDS:
        if _normalize_header(label) == n:
            return field_key
    return _HEADER_ALIASES.get(n)


def map_shared_context_columns(headers: list[str]) -> dict[str, int]:
    """Map internal field keys to column indexes. Unknown columns are ignored."""
    col_map: dict[str, int] = {}
    for idx, header in enumerate(headers):
        field = header_to_shared_field(header)
        if field and field not in col_map:
            col_map[field] = idx
    return col_map


def rows_from_sheet_values(
    headers: list[str],
    data_rows: list[list[Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Parse sheet rows into structured shared-context records.
    Returns (records, warnings).
    """
    col_map = map_shared_context_columns(headers)
    warnings: list[str] = []
    expected = [label for _, label in SHARED_CONTEXT_FIELDS]
    missing_labels = [label for key, label in SHARED_CONTEXT_FIELDS if key not in col_map]
    if missing_labels:
        warnings.append(f"Missing expected columns (will be empty): {', '.join(missing_labels)}")

    records: list[dict[str, Any]] = []
    for sheet_row_num, raw in enumerate(data_rows, start=2):
        record: dict[str, Any] = {key: "" for key, _ in SHARED_CONTEXT_FIELDS}
        for field_key, col_idx in col_map.items():
            cell = raw[col_idx] if col_idx < len(raw) else ""
            record[field_key] = str(cell or "").strip().replace("\n", " ").replace("\t", " ")
        if not any(str(record[k] or "").strip() for k, _ in SHARED_CONTEXT_FIELDS):
            continue
        # Require at least title or description so empty rubric lines are skipped.
        if not str(record.get("title") or "").strip() and not str(record.get("description") or "").strip():
            continue
        record["sheet_row"] = sheet_row_num
        records.append(record)

    if not records and data_rows:
        warnings.append(
            f"No usable rows after filtering to columns: {', '.join(expected)} "
            "(each row needs Title and/or Description)."
        )
    return records, warnings


def _row_fingerprint(record: dict[str, Any]) -> str:
    parts = [str(record.get(k) or "") for k, _ in SHARED_CONTEXT_FIELDS]
    parts.append(str(record.get("sheet_row") or ""))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


def format_shared_context_embed_text(record: dict[str, Any]) -> str:
    """Human-readable block used for embedding and LLM retrieval."""
    lines = ["Shared coaching context (organization-wide knowledge base):"]
    for field_key, label in SHARED_CONTEXT_FIELDS:
        value = str(record.get(field_key) or "").strip()
        if value:
            lines.append(f"{label}: {value}")
    return "\n".join(lines)


def shared_context_metadata(record: dict[str, Any], *, origin: str = "google_sheet") -> dict[str, Any]:
    meta: dict[str, Any] = {
        "vector_format": SHARED_CONTEXT_VECTOR_FORMAT,
        "context_scope": CONTEXT_SCOPE_SHARED,
        "chunk_type": "coaching_rubric_row",
        "origin": origin,
        "sheet_row": record.get("sheet_row"),
    }
    for field_key, _ in SHARED_CONTEXT_FIELDS:
        meta[field_key] = str(record.get(field_key) or "").strip()
    return meta


def structured_chunks_from_shared_records(
    records: list[dict[str, Any]],
    *,
    max_tokens: int = 650,
) -> list[tuple[str, str, dict[str, Any]]]:
    """
    One primary chunk per sheet row; split only if a single row exceeds token budget.
    Returns (embed_text, source_ref, metadata).
    """
    out: list[tuple[str, str, dict[str, Any]]] = []
    for record in records:
        body = format_shared_context_embed_text(record)
        sheet_row = int(record.get("sheet_row") or 0)
        fp = _row_fingerprint(record)
        meta = shared_context_metadata(record)
        parts = chunk_text_by_tokens(body, max_tokens=max_tokens)
        if len(parts) <= 1:
            ref = f"shared_sheet:row_{sheet_row}:{fp}"
            out.append((body, ref, {**meta, "chunk_part": 1, "chunk_parts": 1}))
            continue
        for part_idx, part in enumerate(parts, start=1):
            ref = f"shared_sheet:row_{sheet_row}:{fp}:part_{part_idx}"
            out.append(
                (
                    part,
                    ref,
                    {**meta, "chunk_part": part_idx, "chunk_parts": len(parts)},
                )
            )
    return out


def structured_chunks_from_shared_sheet_tsv(
    text: str,
    *,
    max_tokens: int = 650,
) -> list[tuple[str, str, dict[str, Any]]]:
    """Parse tab-separated sheet export (header + rows) into structured chunks."""
    lines = [ln for ln in (text or "").splitlines() if ln.strip()]
    if len(lines) < 2:
        return []
    headers = [c.strip() for c in lines[0].split("\t")]
    data_rows = [[cell.strip() for cell in row.split("\t")] for row in lines[1:]]
    records, _warnings = rows_from_sheet_values(headers, data_rows)
    return structured_chunks_from_shared_records(records, max_tokens=max_tokens)


def format_shared_context_sheet_summary(records: list[dict[str, Any]], *, max_chars: int) -> str:
    """Compact structured text for direct LLM injection (non-vector path)."""
    blocks: list[str] = []
    for record in records:
        blocks.append(format_shared_context_embed_text(record))
    text = "\n\n".join(blocks).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1] + "…"
