"""Submit video to POSE_API_BASE_URL (RunPod GPU), poll until pose JSON is ready."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx


def pose_api_base_url() -> str | None:
    raw = (os.getenv("POSE_API_BASE_URL") or "").strip().rstrip("/")
    return raw or None


def pose_api_configured() -> bool:
    return pose_api_base_url() is not None


def _pose_api_headers() -> dict[str, str]:
    headers: dict[str, str] = {"Content-Type": "application/json"}
    key = (os.getenv("POSE_API_KEY") or "").strip()
    if key:
        headers["Authorization"] = f"Bearer {key}"
    return headers


def _poll_pose_job_until_done(
    base: str,
    job_id: str,
    *,
    on_status: Callable[[dict[str, Any]], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> None:
    poll_timeout = float(os.getenv("POSE_API_POLL_TIMEOUT_SECONDS", "10800"))
    poll_interval = float(os.getenv("POSE_API_POLL_INTERVAL_SECONDS", "3"))
    http_timeout = float(os.getenv("POSE_API_HTTP_TIMEOUT_SECONDS", "300"))
    status_url = f"{base}/pose/jobs/{job_id}/status"
    deadline = time.monotonic() + poll_timeout
    last_transport: str | None = None

    with httpx.Client(timeout=http_timeout, headers=_pose_api_headers()) as client:
        while time.monotonic() < deadline:
            if cancel_check is not None and cancel_check():
                try:
                    client.post(f"{base}/pose/jobs/{job_id}/cancel")
                except httpx.HTTPError:
                    pass
                raise RuntimeError("Cancelled by user during remote pose job.")

            try:
                sr = client.get(status_url)
            except httpx.RequestError as exc:
                last_transport = str(exc)
                time.sleep(poll_interval)
                continue

            if sr.status_code == 404:
                time.sleep(poll_interval)
                continue
            if sr.status_code != 200:
                raise RuntimeError(f"Pose status poll HTTP {sr.status_code}: {sr.text[:1200]}")

            payload = sr.json()
            if on_status is not None:
                progress = {
                    "phase": payload.get("phase") or "pose_remote",
                    "progress_detail": payload.get("progress_detail")
                    or f"Remote pose job: {payload.get('status') or 'running'}",
                }
                on_status(progress)

            st = str(payload.get("status") or "")
            if st == "completed":
                return
            if st == "failed":
                raise RuntimeError(str(payload.get("error") or "remote_pose_failed"))

            time.sleep(poll_interval)

    extra = f" Last error: {last_transport}" if last_transport else ""
    raise RuntimeError(f"Remote pose job timed out after {poll_timeout:.0f}s.{extra}")


def _fetch_remote_pose_result(base: str, job_id: str) -> dict[str, Any]:
    http_timeout = float(os.getenv("POSE_API_HTTP_TIMEOUT_SECONDS", "300"))
    url = f"{base}/pose/jobs/{job_id}/result"
    with httpx.Client(timeout=http_timeout, headers=_pose_api_headers()) as client:
        resp = client.get(url)
    if resp.status_code != 200:
        raise RuntimeError(f"Pose result fetch HTTP {resp.status_code}: {resp.text[:1200]}")
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("Pose result was not a JSON object.")
    return data


def resolve_pose_data_for_video(
    video_url: str,
    *,
    job_key: str,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """
    Return pose JSON dict for ``video_url``.

    When ``POSE_API_BASE_URL`` is set, submits to the remote GPU API and polls.
    Otherwise runs ``run_pose_pipeline`` locally and loads the JSON file.
    """
    base = pose_api_base_url()
    if not base:
        from yolo_model.pipeline.runner import run_pose_pipeline
        from yolo_model.pose_feedback.engine import load_pose_json

        if on_progress is not None:
            on_progress(
                {
                    "phase": "pose_local",
                    "progress_detail": "Running YOLO pose pipeline locally…",
                }
            )
        out_path = run_pose_pipeline(video_url, job_key=job_key)
        return load_pose_json(Path(out_path))

    http_timeout = float(os.getenv("POSE_API_HTTP_TIMEOUT_SECONDS", "300"))
    if on_progress is not None:
        on_progress(
            {
                "phase": "pose_remote_submit",
                "progress_detail": "Submitting video to remote pose API (RunPod)…",
            }
        )

    with httpx.Client(timeout=http_timeout, headers=_pose_api_headers()) as client:
        resp = client.post(
            f"{base}/pose/jobs",
            json={"video_url": video_url, "job_key": job_key},
        )
    if resp.status_code >= 400:
        raise RuntimeError(f"Pose API submit HTTP {resp.status_code}: {resp.text[:1200]}")

    created = resp.json()
    job_id = str(created.get("id") or "").strip()
    if not job_id:
        raise RuntimeError("Pose API did not return a job id.")

    _poll_pose_job_until_done(
        base,
        job_id,
        on_status=on_progress,
        cancel_check=cancel_check,
    )
    return _fetch_remote_pose_result(base, job_id)
