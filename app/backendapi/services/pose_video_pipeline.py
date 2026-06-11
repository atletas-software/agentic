"""Platform worker wrapper around ``yolo_model.pipeline.run_pose_pipeline``.

In Docker production the slim worker image has no torch. With ``POSE_PIPELINE_REMOTE_ONLY=true``
(default), pose runs in feedback-agent. Set ``POSE_API_BASE_URL`` on feedback-agent to offload
YOLO to a RunPod GPU host instead of local CPU inference.
"""

from __future__ import annotations

import os
from pathlib import Path


def pose_pipeline_remote_only() -> bool:
    """When true, worker must not run YOLO; feedback-agent runs pose from video_url."""
    raw = (os.getenv("POSE_PIPELINE_REMOTE_ONLY") or "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def pose_pipeline_enabled() -> bool:
    raw = (os.getenv("FEEDBACK_USE_POSE_PIPELINE") or "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def worker_should_run_pose_pipeline() -> bool:
    """True only when pose is enabled and this process is allowed to run YOLO locally."""
    if not pose_pipeline_enabled():
        return False
    if pose_pipeline_remote_only():
        return False
    explicit = (os.getenv("POSE_PIPELINE_RUN_ON_WORKER") or "").strip().lower()
    if explicit in {"1", "true", "yes", "on"}:
        return True
    if explicit in {"0", "false", "no", "off"}:
        return False
    return False


def run_pose_pipeline_for_job(video_url: str, *, agent_job_id: int) -> str:
    """Download video, run YOLO pose detection, return absolute pose JSON path."""
    if pose_pipeline_remote_only():
        raise RuntimeError(
            "POSE_PIPELINE_REMOTE_ONLY is set; run pose on feedback-agent, not the worker."
        )
    from yolo_model.pipeline.runner import run_pose_pipeline

    job_key = f"job_{agent_job_id}"
    device = (os.getenv("YOLO_POSE_DEVICE") or "cpu").strip()
    out = run_pose_pipeline(video_url, job_key=job_key, device=device)
    return str(out)
