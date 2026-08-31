"""Public URL helpers for feedback review pages (frontend vs internal API host)."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse, urlunparse

from fastapi import Request


def first_origin(raw: str) -> str | None:
    first = (raw or "").split(",")[0].strip().rstrip("/")
    return first or None


def frontend_origin() -> str | None:
    """Next.js origin for share/review links. Prefers https, then a non-localhost host."""
    parts = [
        part.strip().rstrip("/")
        for part in (os.getenv("FRONTEND_BASE_URL") or "").split(",")
        if part.strip()
    ]
    if not parts:
        return None
    https = [origin for origin in parts if origin.startswith("https://")]
    if https:
        return https[0]
    for origin in parts:
        host = origin.split("://", 1)[-1].split("/")[0].split(":")[0]
        if host not in {"localhost", "127.0.0.1"}:
            return origin
    return parts[0]


def public_origin() -> str | None:
    """Configured public origin (scheme + host[+port]) without trailing slash, or None."""
    return first_origin(os.getenv("PUBLIC_BASE_URL") or "")


def rewrite_origin(url: str) -> str:
    """If PUBLIC_BASE_URL is set, swap scheme+netloc so clients get the public host."""
    public_base = public_origin()
    if not public_base:
        return url
    pub = urlparse(public_base)
    parsed = urlparse(url)
    return urlunparse(
        (
            pub.scheme or parsed.scheme,
            pub.netloc or parsed.netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )


def public_url_for(request: Request, name: str, **path_params: Any) -> str:
    """request.url_for with origin rewritten to PUBLIC_BASE_URL when set."""
    return rewrite_origin(str(request.url_for(name, **path_params)))


def public_review_url(review_id: str) -> str:
    """Build user-facing /review/{id} on the frontend host."""
    origin = frontend_origin() or public_origin()
    if origin:
        return f"{origin}/review/{review_id}"
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    return f"http://{host}:{port}/review/{review_id}"


def absolute_url(request: Request, path: str) -> str:
    public_base = public_origin()
    if public_base:
        return f"{public_base}{path if path.startswith('/') else '/' + path}"
    base = str(request.base_url).rstrip("/")
    return f"{base}{path if path.startswith('/') else '/' + path}"


def viewer_url(request: Request, path: str) -> str:
    """Share/review URLs that users open in the browser (Next.js host)."""
    origin = frontend_origin()
    normalized = path if path.startswith("/") else f"/{path}"
    if origin:
        return f"{origin}{normalized}"
    return absolute_url(request, normalized)
