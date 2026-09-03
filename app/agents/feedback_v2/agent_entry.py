"""Feedback agent v2 — drop-in replacement template.

Implement ``FeedbackAgentV2.run_review``, then set::

    FEEDBACK_AGENT_VERSION=v2

See ``docs/feedback-agents-yolo-pose.md`` for full plug-in steps.
"""

from __future__ import annotations

from typing import Any, Callable

from agents.registry import register_feedback_agent


class FeedbackAgentV2:
    """Replace this stub with your improved agent."""

    version = "v2"

    def run_review(
        self,
        review_id: str,
        payload: dict[str, Any],
        *,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError(
            "FeedbackAgentV2 is a template. Implement run_review() in "
            "agents/feedback_v2/agent_entry.py, then set FEEDBACK_AGENT_VERSION=v2."
        )


def register() -> None:
    register_feedback_agent("v2", FeedbackAgentV2)
