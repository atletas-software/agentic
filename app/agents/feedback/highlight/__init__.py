"""YOLO-based highlight-overlay detection + per-event extraction pipeline.

Public entry points:

- ``run_yolo_pipeline`` (in ``pipeline.py``) — orchestrates detect → events →
  per-event asset extraction.
- ``HighlightDetector`` (in ``yolo_detector.py``) — lazy-loaded YOLOv8 wrapper.
- ``HighlightEvent`` (in ``event_extractor.py``) — single-event dataclass.

Nothing in this package imports the legacy HSV-based detector at module-load
time, so it is safe to import even when the YOLO weights / ultralytics aren't
available (errors surface only on first use).
"""

from agents.feedback.highlight.event_extractor import HighlightEvent, build_events
from agents.feedback.highlight.pipeline import PipelineResult, run_yolo_pipeline
from agents.feedback.highlight.yolo_detector import (
    Detection,
    HighlightDetector,
    get_default_detector,
)

__all__ = [
    "Detection",
    "HighlightDetector",
    "HighlightEvent",
    "PipelineResult",
    "build_events",
    "get_default_detector",
    "run_yolo_pipeline",
]
