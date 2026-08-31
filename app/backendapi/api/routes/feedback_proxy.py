"""Feedback review UI + JSON API on the platform API (no separate :5055 proxy).

The Next.js frontend rewrites /review, /jobs, /share, and /api/reviews to these routes.
"""

from __future__ import annotations

from agents.feedback.routes import router

__all__ = ["router"]
