from __future__ import annotations

import os
from typing import Iterator

import tiktoken


def _encoder():
    model = os.getenv("PLAYER_MEMORY_CHUNK_ENCODING", "cl100k_base")
    return tiktoken.get_encoding(model)


def chunk_text_by_tokens(
    text: str,
    *,
    max_tokens: int | None = None,
    overlap_ratio: float | None = None,
) -> list[str]:
    """Split long text into overlapping token windows (paragraph-aware when possible)."""
    max_tok = max_tokens or int(os.getenv("PLAYER_MEMORY_CHUNK_MAX_TOKENS", "650"))
    overlap = overlap_ratio if overlap_ratio is not None else float(os.getenv("PLAYER_MEMORY_CHUNK_OVERLAP_RATIO", "0.12"))
    raw = (text or "").strip()
    if not raw:
        return []

    enc = _encoder()
    tokens = enc.encode(raw)
    if len(tokens) <= max_tok:
        return [raw]

    overlap_tokens = max(1, int(max_tok * overlap))
    chunks: list[str] = []
    start = 0
    while start < len(tokens):
        end = min(start + max_tok, len(tokens))
        piece = enc.decode(tokens[start:end]).strip()
        if piece:
            chunks.append(piece)
        if end >= len(tokens):
            break
        start = max(0, end - overlap_tokens)
    return chunks


def chunk_sheet_row_document(headers: list[str], row: list[str]) -> list[str]:
    """One primary chunk from header:value lines; splits wide rows."""
    lines = []
    for i, h in enumerate(headers):
        val = row[i] if i < len(row) else ""
        lines.append(f"{h}: {val}")
    doc = "\n".join(lines)
    return chunk_text_by_tokens(doc)
