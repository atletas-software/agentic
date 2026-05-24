"""End-to-end YOLO highlight pipeline orchestrator.

Glue layer that ties together the four building blocks:

  cache.get_local_video   ──►   probe.run_two_pass_probe   ──►   event_extractor.build_events
                                                                       │
                                                                       ▼
                                                              event_assets.build_event_assets

Callers (currently ``review_agent._try_circle_segment_episode_review``) get back
a :class:`PipelineResult` that contains the full event list with their on-disk
assets plus a debug payload suitable for the ``generation_debug`` block in
``review.json``.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from agents.feedback.highlight.cache import CachedVideo, get_local_video
from agents.feedback.highlight.event_assets import (
    EventAssets,
    build_event_assets,
    write_events_index,
)
from agents.feedback.highlight.event_extractor import HighlightEvent, build_events
from agents.feedback.highlight.probe import ProbeResult, run_two_pass_probe
from agents.feedback.highlight.yolo_detector import (
    HighlightDetector,
    HighlightDetectorUnavailable,
    get_default_detector,
)
from agents.feedback.video_utils import probe_duration

LOG = logging.getLogger("highlight.pipeline")


@dataclass
class PipelineResult:
    events: list[EventAssets]
    duration_sec: float
    cached: CachedVideo
    probes: ProbeResult
    events_index_path: Optional[Path] = None
    timings: dict[str, float] = field(default_factory=dict)

    def to_debug_dict(self) -> dict[str, Any]:
        return {
            "detector": "yolo",
            "duration_sec": round(float(self.duration_sec), 3),
            "video_cached": bool(self.cached.cached),
            "video_cache_path": (str(self.cached.cache_path) if self.cached.cache_path else None),
            "video_cache_bytes": int(self.cached.bytes_downloaded),
            "coarse_probe_count": len(self.probes.coarse),
            "coarse_on_count": sum(1 for p in self.probes.coarse if p.found),
            "fine_probe_count": len(self.probes.fine),
            "fine_on_count": sum(1 for p in self.probes.fine if p.found),
            "candidate_windows": [
                {"t_lo": round(float(lo), 3), "t_hi": round(float(hi), 3)}
                for lo, hi in self.probes.candidate_windows
            ],
            "event_count": len(self.events),
            "events": [
                {
                    "index": e.event.index,
                    "t_on": e.event.t_on,
                    "t_off": e.event.t_off,
                    "t_lo": e.event.t_lo,
                    "t_hi": e.event.t_hi,
                    "anchor_sec": e.event.anchor_sec,
                    "mean_conf": round(e.event.mean_conf, 4),
                    "peak_conf": round(e.event.peak_conf, 4),
                    "frame_count": len(e.frame_paths),
                    "clip_mode": e.clip_mode,
                }
                for e in self.events
            ],
            "timings": {**self.probes.timings, **self.timings},
        }


def run_yolo_pipeline(
    *,
    video_url: str,
    base_dir: Path,
    detector: Optional[HighlightDetector] = None,
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> PipelineResult:
    """Run cache → probe → events → assets end-to-end.

    Raises :class:`HighlightDetectorUnavailable` if the YOLO weights or
    ``ultralytics`` are missing — callers can catch and fall back to the
    legacy HSV pipeline.
    """
    base_dir.mkdir(parents=True, exist_ok=True)
    detector = detector or get_default_detector()
    if not detector.is_available():
        raise HighlightDetectorUnavailable("YOLO highlight detector is not available.")

    cached = get_local_video(video_url)
    video_input = cached.ffmpeg_input

    if on_progress:
        on_progress(
            {
                "phase": "ffprobe",
                "progress_detail": "Reading duration…",
            }
        )

    t0 = time.time()
    duration_sec = probe_duration(video_input)
    if duration_sec <= 0:
        raise RuntimeError(f"Video duration is invalid: {duration_sec}")
    duration_probe_sec = round(time.time() - t0, 2)

    if on_progress:
        on_progress(
            {
                "phase": "highlight_yolo_probe",
                "progress_detail": (
                    f"Duration {duration_sec:.1f}s — running two-pass YOLO probe…"
                ),
            }
        )

    probes_dir = base_dir / "highlight_probes"
    probes_dir.mkdir(parents=True, exist_ok=True)
    probes = run_two_pass_probe(
        video_input=video_input,
        duration_sec=duration_sec,
        output_root=probes_dir,
        detector=detector,
        on_progress=on_progress,
        cancel_check=cancel_check,
    )

    if cancel_check is not None and cancel_check():
        raise RuntimeError("Review cancelled by user")

    if on_progress:
        on_progress(
            {
                "phase": "highlight_yolo_events",
                "progress_detail": "Smoothing per-frame detections into events…",
            }
        )

    t0 = time.time()
    raw_events: list[HighlightEvent] = build_events(
        probes.merged_sorted(),
        duration_sec=duration_sec,
    )
    events_build_sec = round(time.time() - t0, 2)

    if on_progress:
        on_progress(
            {
                "phase": "highlight_yolo_assets",
                "segment_total": len(raw_events),
                "segment_current": 0,
                "progress_detail": f"Found {len(raw_events)} event(s). Extracting frames + clips…",
            }
        )

    events_root = base_dir / "events"
    events_root.mkdir(parents=True, exist_ok=True)
    asset_records: list[EventAssets] = []
    t0 = time.time()
    for ev in raw_events:
        if cancel_check is not None and cancel_check():
            raise RuntimeError("Review cancelled by user")
        record = build_event_assets(
            event=ev,
            video_input=video_input,
            base_dir=events_root,
            fine_probes=probes.fine,
            detector=detector,
            on_progress=on_progress,
        )
        asset_records.append(record)
        if on_progress:
            on_progress(
                {
                    "phase": "highlight_yolo_assets",
                    "segment_total": len(raw_events),
                    "segment_current": ev.index,
                    "progress_detail": (
                        f"Event {ev.index}/{len(raw_events)} done "
                        f"({len(record.frame_paths)} frames, clip={record.clip_mode})."
                    ),
                }
            )
    assets_build_sec = round(time.time() - t0, 2)

    index_path = write_events_index(events=asset_records, base_dir=events_root, duration_sec=duration_sec)

    return PipelineResult(
        events=asset_records,
        duration_sec=duration_sec,
        cached=cached,
        probes=probes,
        events_index_path=index_path,
        timings={
            "duration_probe_sec": duration_probe_sec,
            "events_build_sec": events_build_sec,
            "assets_build_sec": assets_build_sec,
        },
    )
