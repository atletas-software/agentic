"""Runtime checks for YOLO weights and ML deps (feedback-agent / ops)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def _app_root() -> Path:
    return Path(__file__).resolve().parents[1]


def check_yolo_runtime() -> dict[str, Any]:
    """Report whether highlight + pose stack can run in this container."""
    root = _app_root()
    out: dict[str, Any] = {
        "app_root": str(root),
        "torch_available": False,
        "ultralytics_available": False,
        "highlight_weights_path": None,
        "highlight_weights_ok": False,
        "pose_weights_env": (os.getenv("YOLO_POSE_WEIGHTS") or "yolov8n-pose.pt").strip(),
        "pose_device": (os.getenv("YOLO_POSE_DEVICE") or "cpu").strip(),
        "ready_for_pose_pipeline": False,
        "errors": [],
    }
    try:
        import torch  # noqa: F401

        out["torch_available"] = True
    except ImportError:
        out["errors"].append("torch not installed")

    try:
        import ultralytics  # noqa: F401

        out["ultralytics_available"] = True
    except ImportError:
        out["errors"].append("ultralytics not installed")

    try:
        from yolo_model.pipeline.runner import resolve_highlight_weights

        wp = resolve_highlight_weights()
        out["highlight_weights_path"] = str(wp)
        out["highlight_weights_ok"] = wp.is_file()
        if not wp.is_file():
            out["errors"].append(f"highlight weights missing: {wp}")
    except Exception as exc:  # noqa: BLE001
        out["errors"].append(f"highlight weights: {exc}")

    out["ready_for_pose_pipeline"] = (
        out["torch_available"]
        and out["ultralytics_available"]
        and out["highlight_weights_ok"]
    )
    return out
