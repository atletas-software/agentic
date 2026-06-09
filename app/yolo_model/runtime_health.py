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

    out["highlight_weights_env"] = (os.getenv("YOLO_HIGHLIGHT_WEIGHTS") or "").strip() or None
    candidates = [
        out["highlight_weights_env"],
        "/app/agents/feedback/models/highlight_yolo_v1.pt",
        "/run/models/highlight_yolo_v1.pt",
        str(root / "yolo_model" / "artifacts" / "train" / "highlight_v1.1.0" / "weights" / "best.pt"),
    ]
    seen: set[str] = set()
    checked: list[dict[str, Any]] = []
    for raw in candidates:
        if not raw or raw in seen:
            continue
        seen.add(raw)
        p = Path(raw)
        checked.append({"path": str(p), "exists": p.is_file(), "size_bytes": p.stat().st_size if p.is_file() else 0})
    out["highlight_weights_checked"] = checked

    try:
        from yolo_model.pipeline.runner import resolve_highlight_weights

        wp = resolve_highlight_weights()
        out["highlight_weights_path"] = str(wp)
        out["highlight_weights_ok"] = wp.is_file()
        if not wp.is_file():
            out["errors"].append(f"highlight weights missing: {wp}")
    except Exception as exc:  # noqa: BLE001
        out["errors"].append(f"highlight weights: {exc}")
        for row in checked:
            if row.get("exists"):
                out["highlight_weights_path"] = row["path"]
                out["highlight_weights_ok"] = True
                out["errors"].append(
                    "resolve_highlight_weights failed but a weights file exists on disk — check YOLO_HIGHLIGHT_WEIGHTS env"
                )
                break

    out["ready_for_pose_pipeline"] = (
        out["torch_available"]
        and out["ultralytics_available"]
        and out["highlight_weights_ok"]
    )
    return out
