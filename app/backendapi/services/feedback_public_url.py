"""User-facing feedback review URLs (opened in the Next.js frontend)."""

from __future__ import annotations

import os

from backendapi.services.frontend_origin import frontend_public_origin, parse_frontend_origins


def _first_origin(raw: str) -> str | None:
    first = (raw or "").split(",")[0].strip().rstrip("/")
    return first or None


def feedback_public_base_url() -> str | None:
    """Public origin for browser links (no trailing slash).

    Prefer FRONTEND_BASE_URL — Next.js serves /review, /jobs, and /share and
    rewrites them to the API. FEEDBACK_PUBLIC_BASE_URL / PUBLIC_BASE_URL are
    fallbacks for older :8000-only deploys.
    """
    if parse_frontend_origins(os.getenv("FRONTEND_BASE_URL") or ""):
        return frontend_public_origin()
    for key in ("FEEDBACK_PUBLIC_BASE_URL", "PUBLIC_BASE_URL"):
        origin = _first_origin(os.getenv(key) or "")
        if origin:
            return origin
    return None


def feedback_public_review_url(review_id: str) -> str | None:
    base = feedback_public_base_url()
    if not base or not review_id:
        return None
    return f"{base}/review/{review_id}"


def feedback_public_watch_url(review_id: str) -> str | None:
    base = feedback_public_base_url()
    if not base or not review_id:
        return None
    return f"{base}/jobs/{review_id}"


def feedback_public_status_url(review_id: str) -> str | None:
    base = feedback_public_base_url()
    if not base or not review_id:
        return None
    return f"{base}/api/reviews/{review_id}/status"
