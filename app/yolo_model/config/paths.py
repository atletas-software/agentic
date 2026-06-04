"""Canonical paths under ``yolo_model/``."""

from __future__ import annotations

from pathlib import Path

_YOLO_MODEL_DIR = Path(__file__).resolve().parents[1]
_HIGHLIGHT_VERSION = "v1.1.0"
_TRAIN_RUN = f"highlight_{_HIGHLIGHT_VERSION}"


def yolo_model_dir() -> Path:
    return _YOLO_MODEL_DIR


def artifacts_dir() -> Path:
    """Git-ignored training outputs and runtime pose JSON."""
    return _YOLO_MODEL_DIR / "artifacts"


def datasets_dir() -> Path:
    return _YOLO_MODEL_DIR / "datasets"


def exports_dir() -> Path:
    """Staged ``best.pt`` copies before promoting to ``agents/feedback/models/``."""
    return _YOLO_MODEL_DIR / "exports"


def default_dataset_yaml() -> Path:
    return datasets_dir() / "athlete_focus" / _HIGHLIGHT_VERSION / "data.yaml"


def default_highlight_weights() -> Path:
    return artifacts_dir() / "train" / _TRAIN_RUN / "weights" / "best.pt"


def default_pose_output_dir() -> Path:
    return artifacts_dir() / "pose"


def posture_kb_path() -> Path:
    return _YOLO_MODEL_DIR / "config" / "posture_guidelines.yaml"


def default_export_weights() -> Path:
    return exports_dir() / f"highlight_yolo_{_HIGHLIGHT_VERSION}.pt"
