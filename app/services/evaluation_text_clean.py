from __future__ import annotations

import json
import re
from typing import Any


_TAG_RE = re.compile(r"<[^>]+>", re.I | re.S)
_IFRAME_RE = re.compile(r"<iframe\b[^>]*>.*?</iframe>", re.I | re.S)
_EMBED_RE = re.compile(r"<embed\b[^>]*>", re.I)
_OBJECT_RE = re.compile(r"<object\b[^>]*>.*?</object>", re.I | re.S)
_EMPTY_P_RE = re.compile(r"<p>\s*(<br\s*/?>|\s)*</p>", re.I)
_WS_RE = re.compile(r"[ \t\r\f\v]+")
_NL_RUN_RE = re.compile(r"\n{3,}")


def _is_na_placeholder(s: str) -> bool:
    t = s.strip().lower()
    return t in {"", "n/a", "na", "none", "null", "—", "-"}


def strip_html_and_embeds(raw: str) -> str:
    s = _IFRAME_RE.sub(" ", raw)
    s = _EMBED_RE.sub(" ", s)
    s = _OBJECT_RE.sub(" ", s)
    s = _EMPTY_P_RE.sub(" ", s)
    s = _TAG_RE.sub(" ", s)
    return s


def normalize_whitespace(s: str) -> str:
    s = _WS_RE.sub(" ", s)
    s = _NL_RUN_RE.sub("\n\n", s)
    return s.strip()


def _flatten_json_value(v: Any, depth: int = 0) -> str:
    if depth > 12:
        return ""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, (int, float, bool)):
        return str(v)
    if isinstance(v, list):
        parts = [_flatten_json_value(x, depth + 1) for x in v]
        return "\n".join(p for p in parts if p.strip())
    if isinstance(v, dict):
        lines: list[str] = []
        for k, val in v.items():
            inner = _flatten_json_value(val, depth + 1)
            inner = normalize_whitespace(strip_html_and_embeds(inner))
            if inner and not _is_na_placeholder(inner):
                lines.append(f"{k}: {inner}")
        return "\n".join(lines)
    return str(v)


def flatten_json_field_for_text(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    try:
        parsed = json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return s
    return _flatten_json_value(parsed)


def format_sql_cell_for_embedding(column_key: str, raw: str) -> str:
    """Prepare one SQL cell for summarization / embedding (no raw HTML/JSON blobs)."""
    v = str(raw or "").strip()
    if not v or _is_na_placeholder(v):
        return ""
    key = column_key.strip().lower()
    if "values_json" in key or key.endswith("_json"):
        v = flatten_json_field_for_text(v)
    v = strip_html_and_embeds(v)
    v = normalize_whitespace(v)
    if _is_na_placeholder(v):
        return ""
    return v
