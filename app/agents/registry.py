"""Pluggable feedback-agent registry.

Backend/worker always call ``get_feedback_agent().run_review(...)``.
Swap implementations with ``FEEDBACK_AGENT_VERSION`` (default: ``v1``).

To add a newer agent:
  1. Create ``agents/feedback_v2/`` (copy from ``feedback/`` or start fresh).
  2. Implement ``FeedbackAgent`` in ``agents/feedback_v2/agent_entry.py``.
  3. Register it: ``register_feedback_agent("v2", FeedbackAgentV2)``.
  4. Set ``FEEDBACK_AGENT_VERSION=v2`` in ``app/backendapi/.env``.
"""

from __future__ import annotations

import os
from typing import Any, Callable, Protocol


class FeedbackAgent(Protocol):
    """Contract every feedback-agent version must satisfy."""

    version: str

    def run_review(
        self,
        review_id: str,
        payload: dict[str, Any],
        *,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        """Run a full review and return the review document dict."""
        ...


_REGISTRY: dict[str, Callable[[], FeedbackAgent]] = {}
_ENSURED = False


def register_feedback_agent(version: str, factory: Callable[[], FeedbackAgent]) -> None:
    key = (version or "").strip().lower()
    if not key:
        raise ValueError("Feedback agent version must be a non-empty string")
    _REGISTRY[key] = factory


def list_feedback_agents() -> list[str]:
    _ensure_builtins_registered()
    return sorted(set(_REGISTRY.keys()))


def resolve_feedback_agent_version(explicit: str | None = None) -> str:
    raw = (explicit if explicit is not None else os.getenv("FEEDBACK_AGENT_VERSION", "v1")).strip()
    return (raw or "v1").lower()


def get_feedback_agent(version: str | None = None) -> FeedbackAgent:
    """Return the configured feedback agent (default ``FEEDBACK_AGENT_VERSION`` / ``v1``)."""
    _ensure_builtins_registered()
    key = resolve_feedback_agent_version(version)
    # ``default`` always aliases the built-in v1 package.
    if key == "default":
        key = "v1"
    factory = _REGISTRY.get(key)
    if factory is None:
        known = ", ".join(list_feedback_agents()) or "(none)"
        raise RuntimeError(
            f"Unknown FEEDBACK_AGENT_VERSION={key!r}. Registered: {known}. "
            "Add your package under agents/ and call register_feedback_agent(...)."
        )
    agent = factory()
    return agent


def _ensure_builtins_registered() -> None:
    global _ENSURED
    if _ENSURED:
        return
    # Import side-effect: registers FeedbackAgentV1 as "v1" / "default".
    from agents.feedback.agent_entry import register as register_v1

    register_v1()
    # Optional newer packages self-register when importable.
    try:
        from agents.feedback_v2.agent_entry import register as register_v2

        register_v2()
    except ImportError:
        pass
    _ENSURED = True
