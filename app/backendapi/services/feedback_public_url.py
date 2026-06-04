"""User-facing feedback agent URLs (review player, job status) via the platform origin."""

from __future__ import annotations

import os


def feedback_public_base_url() -> str | None:
    """Public origin for browser links (no trailing slash).

    Prefer FEEDBACK_PUBLIC_BASE_URL on the platform app (RunPod :8000 proxy).
    Falls back to PUBLIC_BASE_URL when set on the feedback agent host.
    """
    for key in ("FEEDBACK_PUBLIC_BASE_URL", "PUBLIC_BASE_URL"):
        raw = (os.getenv(key) or "").strip().rstrip("/")
        if raw:
            return raw
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
