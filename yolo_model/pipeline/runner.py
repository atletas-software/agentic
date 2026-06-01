"""Run highlight + pose detection; write schema-v3 pose JSON."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

from yolo_model.pipeline.video_download import ensure_local_video

LOG = logging.getLogger("yolo.pipeline.runner")


def _repo_root() -> Path:
    # yolo_model/pipeline/runner.py → repo root is three levels up
    return Path(__file__).resolve().parents[2]


def resolve_highlight_weights() -> Path:
    candidates = [
        os.getenv("YOLO_HIGHLIGHT_WEIGHTS", "").strip(),
        os.getenv("VIDEO_HIGHLIGHT_YOLO_WEIGHTS", "").strip(),
        "agents/feedback/models/highlight_yolo_v1.pt",
        "runs/detect/yolo_model/runs/athlete_focus_v1_1_0/weights/best.pt",
        "yolo_model/weights/highlight_yolo_v1.0.0.pt",
    ]
    root = _repo_root()
    for raw in candidates:
        if not raw:
            continue
        p = Path(raw)
        if not p.is_absolute():
            p = root / p
        if p.is_file():
            return p.resolve()
    raise FileNotFoundError(
        "Highlight YOLO weights not found. Set YOLO_HIGHLIGHT_WEIGHTS to best.pt path."
    )


def _output_root() -> Path:
    raw = (os.getenv("POSE_PIPELINE_OUTPUT_DIR") or "runs/pose").strip()
    root = Path(raw)
    if not root.is_absolute():
        root = _repo_root() / root
    root.mkdir(parents=True, exist_ok=True)
    return root


def run_pose_pipeline(
    video_url: str,
    *,
    job_key: str,
    device: str | None = None,
    skip_video_output: bool = True,
) -> Path:
    """
    Download (if needed), run ``detect_pose_video_yolov8``, return pose JSON path.
    """
    local_video = ensure_local_video(video_url, job_key=job_key)
    weights = resolve_highlight_weights()
    out_dir = _output_root() / job_key
    out_dir.mkdir(parents=True, exist_ok=True)
    output_json = out_dir / "pose_results.json"

    dev = (device or os.getenv("YOLO_POSE_DEVICE") or "cpu").strip()
    pose_weights = (os.getenv("YOLO_POSE_WEIGHTS") or "yolov8x-pose.pt").strip()

    cmd = [
        sys.executable,
        "-m",
        "yolo_model.scripts.detect_pose_video_yolov8",
        "--video",
        str(local_video),
        "--weights",
        str(weights),
        "--pose-weights",
        pose_weights,
        "--output-json",
        str(output_json),
        "--device",
        dev,
    ]
    if skip_video_output:
        pass  # no --output-video
    else:
        cmd.extend(["--output-video", str(out_dir / "pose_annotated.mp4")])

    LOG.info("Running pose pipeline: %s", " ".join(cmd))
    proc = subprocess.run(
        cmd,
        cwd=str(_repo_root()),
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(_repo_root())},
    )
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "")[-4000:]
        hint = ""
        if "ultralytics" in tail or "torch" in tail.lower():
            hint = " Reinstall PyTorch: bash scripts/install-torch.sh (from repo root, venv active)."
        raise RuntimeError(f"Pose pipeline failed (exit {proc.returncode}): {tail}{hint}")

    if not output_json.is_file():
        raise RuntimeError(f"Pose JSON was not created: {output_json}")

    LOG.info("Pose JSON written: %s", output_json)
    return output_json.resolve()
