"""Platform worker wrapper around ``yolo_model.pipeline.run_pose_pipeline``."""

from __future__ import annotations

import os
from pathlib import Path


def run_pose_pipeline_for_job(video_url: str, *, agent_job_id: int) -> str:
    """Download video, run YOLO pose detection, return absolute pose JSON path."""
    from yolo_model.pipeline.runner import run_pose_pipeline

    job_key = f"job_{agent_job_id}"
    device = (os.getenv("YOLO_POSE_DEVICE") or "cpu").strip()
    out = run_pose_pipeline(video_url, job_key=job_key, device=device)
    return str(out)


def pose_pipeline_enabled() -> bool:
    raw = (os.getenv("FEEDBACK_USE_POSE_PIPELINE") or "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}
