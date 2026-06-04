"""YOLOv8 highlight-overlay detector.

Wraps an Ultralytics YOLOv8 model behind a small, dependency-light API:

    detector = get_default_detector()           # lazy singleton
    detections = detector.predict_paths([...])  # one Detection per input image

The model is loaded once per process; subsequent calls are cheap. CPU inference
is the default and what we deploy — a fine-tuned ``yolov8n`` runs at ~30ms /
640px frame on a modern CPU, which is fast enough for batched scans of long
videos.

Configuration (all optional, with safe defaults):

- ``VIDEO_HIGHLIGHT_YOLO_WEIGHTS`` — path to ``best.pt`` (default
  ``agents/feedback/models/highlight_yolo_v1.pt``).
- ``VIDEO_HIGHLIGHT_YOLO_CONF`` — confidence threshold (default 0.45).
- ``VIDEO_HIGHLIGHT_YOLO_IOU`` — NMS IoU (default 0.5).
- ``VIDEO_HIGHLIGHT_YOLO_IMGSZ`` — inference image size (default 640).
- ``VIDEO_HIGHLIGHT_YOLO_BATCH`` — batch size (default 16).
- ``VIDEO_HIGHLIGHT_YOLO_DEVICE`` — device string (default ``cpu``).

If the weights file is missing or ``ultralytics`` is not installed, all detector
calls raise ``HighlightDetectorUnavailable`` so callers can fall back cleanly.
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import numpy as np

LOG = logging.getLogger("highlight.yolo")

_DEFAULT_WEIGHTS = "agents/feedback/models/highlight_yolo_v1.pt"


class HighlightDetectorUnavailable(RuntimeError):
    """Raised when the YOLO weights or ultralytics package are missing."""


@dataclass
class Detection:
    """One highlight-overlay detection for a single frame.

    bbox is normalized (x, y, w, h) in [0, 1] anchored at the top-left so it
    survives any later resize / crop.
    """

    found: bool
    confidence: float = 0.0
    bbox: dict[str, float] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def center_x(self) -> float:
        return float(self.bbox.get("x", 0.0)) + float(self.bbox.get("w", 0.0)) / 2.0

    @property
    def center_y(self) -> float:
        return float(self.bbox.get("y", 0.0)) + float(self.bbox.get("h", 0.0)) / 2.0

    @property
    def radius(self) -> float:
        """Bbox half-diagonal (normalized by min dim) — a useful "radius" proxy."""
        w = float(self.bbox.get("w", 0.0))
        h = float(self.bbox.get("h", 0.0))
        return max(w, h) / 2.0

    def to_legacy_overlay_dict(self) -> dict[str, Any]:
        """Match the shape returned by ``video_utils.detect_highlight_overlay``."""
        if not self.found:
            return {"found": False, "method": "yolo"}
        return {
            "found": True,
            "method": "yolo",
            "center_x": round(self.center_x, 4),
            "center_y": round(self.center_y, 4),
            "radius": round(max(self.radius, 0.03), 4),
            "score": round(float(self.confidence), 4),
            "bbox": {
                "x": round(float(self.bbox.get("x", 0.0)), 4),
                "y": round(float(self.bbox.get("y", 0.0)), 4),
                "width": round(float(self.bbox.get("w", 0.0)), 4),
                "height": round(float(self.bbox.get("h", 0.0)), 4),
            },
        }


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        LOG.warning("Bad float for %s=%r, falling back to %s", name, raw, default)
        return default


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        LOG.warning("Bad int for %s=%r, falling back to %s", name, raw, default)
        return default


class HighlightDetector:
    """Thread-safe lazy wrapper around an Ultralytics YOLOv8 model."""

    def __init__(
        self,
        *,
        weights_path: Optional[str | Path] = None,
        conf: Optional[float] = None,
        iou: Optional[float] = None,
        imgsz: Optional[int] = None,
        batch_size: Optional[int] = None,
        device: Optional[str] = None,
    ) -> None:
        self.weights_path = Path(weights_path or os.getenv("VIDEO_HIGHLIGHT_YOLO_WEIGHTS") or _DEFAULT_WEIGHTS)
        self.conf = float(conf if conf is not None else _env_float("VIDEO_HIGHLIGHT_YOLO_CONF", 0.45))
        self.iou = float(iou if iou is not None else _env_float("VIDEO_HIGHLIGHT_YOLO_IOU", 0.5))
        self.imgsz = int(imgsz if imgsz is not None else _env_int("VIDEO_HIGHLIGHT_YOLO_IMGSZ", 640))
        self.batch_size = int(batch_size if batch_size is not None else _env_int("VIDEO_HIGHLIGHT_YOLO_BATCH", 16))
        self.device = (device if device is not None else (os.getenv("VIDEO_HIGHLIGHT_YOLO_DEVICE") or "cpu")).strip() or "cpu"
        self._model: Any = None
        self._lock = threading.Lock()

    def _load_model(self) -> Any:
        if self._model is not None:
            return self._model
        with self._lock:
            if self._model is not None:
                return self._model
            if not self.weights_path.is_file():
                raise HighlightDetectorUnavailable(
                    f"YOLO weights not found at {self.weights_path}. "
                    f"Train via yolo_model/scripts/train.py and copy best.pt there, "
                    f"or set VIDEO_HIGHLIGHT_YOLO_WEIGHTS."
                )
            try:
                from ultralytics import YOLO  # type: ignore
            except ImportError as exc:
                raise HighlightDetectorUnavailable(
                    "ultralytics is not installed in this environment. Add it to "
                    "app/agents/requirements.txt."
                ) from exc
            LOG.info("Loading highlight YOLO weights: %s (device=%s)", self.weights_path, self.device)
            self._model = YOLO(str(self.weights_path))
            return self._model

    def is_available(self) -> bool:
        try:
            self._load_model()
            return True
        except HighlightDetectorUnavailable:
            return False

    def predict_paths(self, image_paths: Sequence[Path]) -> list[Detection]:
        """Run inference on a list of image files. Returns one Detection per input."""
        if not image_paths:
            return []
        model = self._load_model()
        out: list[Detection] = []
        for chunk in _chunked(list(image_paths), self.batch_size):
            results = model.predict(
                source=[str(p) for p in chunk],
                conf=self.conf,
                iou=self.iou,
                imgsz=self.imgsz,
                device=self.device,
                verbose=False,
            )
            for path, res in zip(chunk, results):
                out.append(_result_to_detection(res, source=str(path)))
        return out

    def predict_arrays(self, images: Sequence[np.ndarray]) -> list[Detection]:
        """Run inference on a list of HxWx3 BGR arrays (cv2-style)."""
        if not images:
            return []
        model = self._load_model()
        out: list[Detection] = []
        for chunk in _chunked(list(images), self.batch_size):
            results = model.predict(
                source=list(chunk),
                conf=self.conf,
                iou=self.iou,
                imgsz=self.imgsz,
                device=self.device,
                verbose=False,
            )
            for res in results:
                out.append(_result_to_detection(res, source="array"))
        return out


def _chunked(items: list, size: int) -> Iterable[list]:
    size = max(1, int(size))
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _result_to_detection(result: Any, *, source: str) -> Detection:
    """Convert one ultralytics ``Results`` object into our Detection dataclass.

    Picks the single highest-confidence box (we only care about the most
    prominent overlay in any given frame). Returns an empty Detection if no box
    exceeded the conf threshold.
    """
    boxes = getattr(result, "boxes", None)
    if boxes is None or len(boxes) == 0:
        return Detection(found=False, raw={"source": source})

    xyxyn = getattr(boxes, "xyxyn", None)
    conf = getattr(boxes, "conf", None)
    if xyxyn is None or conf is None:
        return Detection(found=False, raw={"source": source, "reason": "missing_tensors"})

    try:
        conf_np = conf.detach().cpu().numpy().reshape(-1)
        xyxyn_np = xyxyn.detach().cpu().numpy().reshape(-1, 4)
    except AttributeError:
        conf_np = np.asarray(conf).reshape(-1)
        xyxyn_np = np.asarray(xyxyn).reshape(-1, 4)

    if conf_np.size == 0:
        return Detection(found=False, raw={"source": source})

    best_idx = int(np.argmax(conf_np))
    x1, y1, x2, y2 = (float(v) for v in xyxyn_np[best_idx].tolist())
    x1 = max(0.0, min(1.0, x1))
    y1 = max(0.0, min(1.0, y1))
    x2 = max(0.0, min(1.0, x2))
    y2 = max(0.0, min(1.0, y2))
    if x2 <= x1 or y2 <= y1:
        return Detection(found=False, raw={"source": source, "reason": "degenerate_box"})

    return Detection(
        found=True,
        confidence=float(conf_np[best_idx]),
        bbox={
            "x": x1,
            "y": y1,
            "w": x2 - x1,
            "h": y2 - y1,
        },
        raw={"source": source, "candidates": int(conf_np.size)},
    )


_DEFAULT_SINGLETON: Optional[HighlightDetector] = None
_DEFAULT_LOCK = threading.Lock()


def get_default_detector() -> HighlightDetector:
    """Process-wide singleton, configured from env. Cheap to call repeatedly."""
    global _DEFAULT_SINGLETON
    if _DEFAULT_SINGLETON is None:
        with _DEFAULT_LOCK:
            if _DEFAULT_SINGLETON is None:
                _DEFAULT_SINGLETON = HighlightDetector()
    return _DEFAULT_SINGLETON


def detect_highlight_overlay_yolo(image_path: Path) -> dict[str, Any]:
    """Drop-in replacement for ``video_utils.detect_highlight_overlay`` (single-image path).

    Useful for code paths that don't yet batch (e.g. per-frame meta enrichment).
    Falls through to ``{"found": False, "method": "yolo", "error": ...}`` if the
    detector is unavailable — callers can decide whether to fall back to HSV.
    """
    try:
        det = get_default_detector().predict_paths([image_path])[0]
        return det.to_legacy_overlay_dict()
    except HighlightDetectorUnavailable as exc:
        return {"found": False, "method": "yolo", "error": str(exc)}
