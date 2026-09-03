"""Run feedback review jobs in-process via the pluggable agent registry."""

from __future__ import annotations

from typing import Any, Callable

from agents.feedback.agent_entry import (
    coaching_focus_from_payload,
    load_review_job,
    load_review_json,
    request_review_cancel,
    review_job_path,
    review_json_path,
)
from agents.registry import get_feedback_agent, resolve_feedback_agent_version

# Re-export helpers used by cancel / status paths in the worker.
__all__ = [
    "coaching_focus_from_payload",
    "load_review_job",
    "load_review_json",
    "request_review_cancel",
    "review_job_path",
    "review_json_path",
    "run_feedback_review_inprocess",
]


def run_feedback_review_inprocess(
    review_id: str,
    payload: dict[str, Any],
    *,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """
    Execute a feedback review synchronously in the API/worker process.

    Selects the agent implementation from ``FEEDBACK_AGENT_VERSION``
    (see ``agents.registry``). Writes job.json + review.json under the
    agent package DATA_DIR (same layout as the standalone HTTP agent).
    """
    agent = get_feedback_agent()
    if on_progress is not None:
        on_progress(
            {
                "feedback_agent_version": getattr(agent, "version", resolve_feedback_agent_version()),
                "status": "running",
            }
        )
    return agent.run_review(
        review_id,
        payload,
        on_progress=on_progress,
        cancel_check=cancel_check,
    )
