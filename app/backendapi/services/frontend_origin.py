"""Public Next.js origin(s) from FRONTEND_BASE_URL (comma-separated CORS list)."""

from __future__ import annotations

import os


def parse_frontend_origins(raw: str | None = None) -> list[str]:
    text = raw if raw is not None else (os.getenv("FRONTEND_BASE_URL") or "http://localhost:3000")
    return [part.strip().rstrip("/") for part in text.split(",") if part.strip()]


def frontend_cors_origins() -> list[str]:
    return parse_frontend_origins() or ["http://localhost:3000"]


def frontend_public_origin(raw: str | None = None) -> str:
    """User-facing origin for redirects and share/review links.

    Prefers https, then a non-localhost host, then the first listed origin.
    """
    parts = parse_frontend_origins(raw)
    if not parts:
        return "http://localhost:3000"
    https = [origin for origin in parts if origin.startswith("https://")]
    if https:
        return https[0]
    for origin in parts:
        host = origin.split("://", 1)[-1].split("/")[0].split(":")[0]
        if host not in {"localhost", "127.0.0.1"}:
            return origin
    return parts[0]
