"""Per-event asset extraction.

For each :class:`HighlightEvent` we build:

1. A folder of JPEG frames covering ``[t_on - pad, t_off + pad]`` at a tunable
   fps (default 3 fps → about 18 stills for a 6-second window). One ffmpeg
   call per event, not one per frame.
2. A short mp4 clip of the same window. We try a stream-copy first (fast, no
   re-encode) and fall back to libx264 re-encode if copy fails (which can
   happen when the start time is not on a keyframe).
3. ``event_meta.json`` — every saved frame's timestamp + the YOLO bounding box
   of the circled player on that frame. Boxes are filled from the existing
   fine-pass probes when available, otherwise a fresh YOLO inference runs on
   the saved frames.
4. Optional annotated preview JPEGs with the bbox drawn (good for quick visual
   review and as input to a downstream vision model).
5. A top-level ``events.json`` index listing every event and its asset paths.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from agents.feedback.highlight.event_extractor import HighlightEvent
from agents.feedback.highlight.ffmpeg_batch import sample_range_uniform
from agents.feedback.highlight.probe import ProbeSample
from agents.feedback.highlight.yolo_detector import (
    Detection,
    HighlightDetector,
    HighlightDetectorUnavailable,
)
from agents.feedback.video_utils import (
    _ffmpeg_http_global_options,  # noqa: PLC2701 — shared utility
    _jpeg_output_usable,  # noqa: PLC2701
    draw_bbox_overlay,
    require_binary,
)

LOG = logging.getLogger("highlight.assets")


@dataclass
class EventAssets:
    event: HighlightEvent
    frames_dir: Path
    frame_paths: list[Path] = field(default_factory=list)
    frame_timestamps: list[float] = field(default_factory=list)
    frame_bboxes: list[Optional[dict[str, float]]] = field(default_factory=list)
    clip_path: Optional[Path] = None
    clip_mode: str = "none"             # "copy" | "reencode" | "none"
    annotated_dir: Optional[Path] = None
    annotated_paths: list[Path] = field(default_factory=list)
    meta_path: Optional[Path] = None


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _truthy(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _event_frame_fps() -> float:
    return max(0.5, _env_float("VIDEO_HIGHLIGHT_EVENT_FRAME_FPS", 3.0))


def _event_frame_width() -> int:
    return max(160, _env_int("VIDEO_HIGHLIGHT_EVENT_FRAME_WIDTH", 960))


def _ffmpeg_extract_timeout_sec() -> Optional[float]:
    raw = (os.getenv("VIDEO_FFMPEG_EXTRACT_TIMEOUT_SEC") or "180").strip().lower()
    if raw in {"", "0", "none", "off"}:
        return None
    try:
        return float(raw)
    except ValueError:
        return 180.0


def _run_ffmpeg(cmd: list[str], *, what: str) -> tuple[int, str]:
    LOG.debug("$ %s", " ".join(cmd))
    try:
        proc = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=False,
            timeout=_ffmpeg_extract_timeout_sec(),
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"{what} timed out.") from exc
    stderr_tail = (proc.stderr or b"").decode("utf-8", errors="replace").strip()[-2000:]
    return proc.returncode, stderr_tail


def _extract_clip(
    *,
    video_input: str,
    t_lo: float,
    duration: float,
    output_path: Path,
) -> str:
    """Try -c copy first, then re-encode. Returns the mode that succeeded."""
    require_binary("ffmpeg")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    http_opts = _ffmpeg_http_global_options(video_input)

    copy_cmd = [
        "ffmpeg",
        "-y",
        *http_opts,
        "-ss",
        f"{t_lo:.3f}",
        "-i",
        video_input,
        "-t",
        f"{duration:.3f}",
        "-c",
        "copy",
        "-avoid_negative_ts",
        "make_zero",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    rc, err = _run_ffmpeg(copy_cmd, what="ffmpeg clip (-c copy)")
    if rc == 0 and output_path.is_file() and output_path.stat().st_size > 1024:
        return "copy"
    LOG.info("Clip -c copy failed (rc=%d); re-encoding. stderr: %s", rc, err[:400])
    if output_path.exists():
        try:
            output_path.unlink()
        except OSError:
            pass

    reencode_cmd = [
        "ffmpeg",
        "-y",
        *http_opts,
        "-ss",
        f"{t_lo:.3f}",
        "-i",
        video_input,
        "-t",
        f"{duration:.3f}",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "23",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    rc, err = _run_ffmpeg(reencode_cmd, what="ffmpeg clip (re-encode)")
    if rc == 0 and output_path.is_file() and output_path.stat().st_size > 1024:
        return "reencode"
    LOG.warning("Clip re-encode failed (rc=%d). stderr: %s", rc, err[:400])
    return "none"


def _nearest_probe_bbox(
    timestamp: float,
    probes: Sequence[ProbeSample],
    *,
    max_delta_sec: float,
) -> Optional[dict[str, float]]:
    if not probes:
        return None
    best: Optional[ProbeSample] = None
    best_d = float("inf")
    for p in probes:
        if not p.found or not p.detection.bbox:
            continue
        d = abs(p.timestamp_sec - timestamp)
        if d < best_d and d <= max_delta_sec:
            best = p
            best_d = d
    if best is None:
        return None
    box = dict(best.detection.bbox)
    box["conf"] = float(best.detection.confidence)
    return box


def _annotate_frame(*, image_path: Path, output_path: Path, bbox: dict[str, float], label: str) -> Path:
    """Reuse the existing bbox drawer for consistency with the rest of the UI."""
    overlay_bbox = {
        "x": float(bbox.get("x", 0.0)),
        "y": float(bbox.get("y", 0.0)),
        "width": float(bbox.get("w", 0.0)),
        "height": float(bbox.get("h", 0.0)),
    }
    return draw_bbox_overlay(
        image_path=image_path,
        output_path=output_path,
        bbox=overlay_bbox,
        label=label,
    )


def build_event_assets(
    *,
    event: HighlightEvent,
    video_input: str,
    base_dir: Path,
    fine_probes: Sequence[ProbeSample],
    detector: Optional[HighlightDetector] = None,
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
) -> EventAssets:
    """Materialize one event's outputs on disk and return the asset record."""
    require_binary("ffmpeg")
    event_dir = base_dir / f"event_{event.index:02d}"
    frames_dir = event_dir / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)

    if on_progress:
        on_progress(
            {
                "phase": "highlight_event_assets",
                "progress_detail": (
                    f"Event {event.index}: extracting frames + clip for "
                    f"[{event.t_lo:.2f}s, {event.t_hi:.2f}s]"
                ),
            }
        )

    sampled = sample_range_uniform(
        video_url=video_input,
        t_lo=event.t_lo,
        t_hi=event.t_hi,
        output_dir=frames_dir,
        prefix="frame",
        fps=_event_frame_fps(),
        frame_width=_event_frame_width(),
    )
    frame_paths = [s.image_path for s in sampled]
    frame_timestamps = [s.timestamp_sec for s in sampled]

    bboxes: list[Optional[dict[str, float]]] = []
    fallback_indices: list[int] = []
    period = 1.0 / max(0.1, _event_frame_fps())
    delta_tol = max(period, 0.5)
    for i, ts in enumerate(frame_timestamps):
        box = _nearest_probe_bbox(ts, fine_probes, max_delta_sec=delta_tol)
        bboxes.append(box)
        if box is None:
            fallback_indices.append(i)

    if fallback_indices and detector is not None:
        try:
            preds: list[Detection] = detector.predict_paths([frame_paths[i] for i in fallback_indices])
        except HighlightDetectorUnavailable as exc:
            LOG.warning("Fallback YOLO inference unavailable: %s", exc)
            preds = []
        for slot, det in zip(fallback_indices, preds):
            if det.found and det.bbox:
                bboxes[slot] = {
                    "x": float(det.bbox.get("x", 0.0)),
                    "y": float(det.bbox.get("y", 0.0)),
                    "w": float(det.bbox.get("w", 0.0)),
                    "h": float(det.bbox.get("h", 0.0)),
                    "conf": float(det.confidence),
                }

    clip_path = event_dir / "clip.mp4"
    if _truthy("VIDEO_HIGHLIGHT_EVENT_WRITE_CLIP", True):
        mode = _extract_clip(
            video_input=video_input,
            t_lo=event.t_lo,
            duration=max(0.1, event.t_hi - event.t_lo),
            output_path=clip_path,
        )
    else:
        mode = "skipped"
        if clip_path.exists():
            try:
                clip_path.unlink()
            except OSError:
                pass

    annotated_dir: Optional[Path] = None
    annotated_paths: list[Path] = []
    if _truthy("VIDEO_HIGHLIGHT_EVENT_WRITE_ANNOTATED", True):
        annotated_dir = event_dir / "annotated"
        annotated_dir.mkdir(parents=True, exist_ok=True)
        for path, ts, box in zip(frame_paths, frame_timestamps, bboxes):
            if not box:
                continue
            annotated_path = annotated_dir / f"annot_{path.stem}.jpg"
            label = f"event {event.index} t={ts:.2f}s conf={box.get('conf', 0):.2f}"
            try:
                _annotate_frame(image_path=path, output_path=annotated_path, bbox=box, label=label)
                annotated_paths.append(annotated_path)
            except Exception as exc:  # noqa: BLE001
                LOG.debug("Annotation failed for %s: %s", path, exc)

    meta = {
        "event_index": event.index,
        "t_on": event.t_on,
        "t_off": event.t_off,
        "t_lo": event.t_lo,
        "t_hi": event.t_hi,
        "anchor_sec": event.anchor_sec,
        "duration_sec": event.duration_sec,
        "mean_conf": round(event.mean_conf, 4),
        "peak_conf": round(event.peak_conf, 4),
        "clip_mode": mode,
        "clip_file": clip_path.name if mode != "none" and clip_path.exists() else None,
        "frame_fps": _event_frame_fps(),
        "frame_width": _event_frame_width(),
        "frames": [
            {
                "file": p.name,
                "timestamp_sec": ts,
                "bbox": (
                    {k: round(float(v), 4) for k, v in box.items()}
                    if box
                    else None
                ),
            }
            for p, ts, box in zip(frame_paths, frame_timestamps, bboxes)
        ],
        "annotated_dir": annotated_dir.name if annotated_dir is not None else None,
    }
    meta_path = event_dir / "event_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    # Final sanity sweep: keep only JPEGs that are actually decodable.
    surviving_paths: list[Path] = []
    surviving_ts: list[float] = []
    surviving_boxes: list[Optional[dict[str, float]]] = []
    for p, ts, box in zip(frame_paths, frame_timestamps, bboxes):
        if _jpeg_output_usable(p):
            surviving_paths.append(p)
            surviving_ts.append(ts)
            surviving_boxes.append(box)

    return EventAssets(
        event=event,
        frames_dir=frames_dir,
        frame_paths=surviving_paths,
        frame_timestamps=surviving_ts,
        frame_bboxes=surviving_boxes,
        clip_path=clip_path if mode != "none" and clip_path.exists() else None,
        clip_mode=mode,
        annotated_dir=annotated_dir,
        annotated_paths=annotated_paths,
        meta_path=meta_path,
    )


def write_events_index(*, events: Sequence[EventAssets], base_dir: Path, duration_sec: float) -> Path:
    """Write a top-level ``events.json`` listing each event's directory + key metadata."""
    base_dir.mkdir(parents=True, exist_ok=True)
    index_path = base_dir / "events.json"
    index = {
        "duration_sec": round(float(duration_sec), 3),
        "event_count": len(events),
        "events": [
            {
                "event_index": e.event.index,
                "t_on": e.event.t_on,
                "t_off": e.event.t_off,
                "t_lo": e.event.t_lo,
                "t_hi": e.event.t_hi,
                "anchor_sec": e.event.anchor_sec,
                "duration_sec": e.event.duration_sec,
                "mean_conf": round(e.event.mean_conf, 4),
                "peak_conf": round(e.event.peak_conf, 4),
                "probe_count": e.event.probe_count,
                "frame_count": len(e.frame_paths),
                "frames_dir": str(e.frames_dir.relative_to(base_dir)),
                "meta_file": (str(e.meta_path.relative_to(base_dir)) if e.meta_path else None),
                "clip_file": (str(e.clip_path.relative_to(base_dir)) if e.clip_path else None),
                "clip_mode": e.clip_mode,
                "annotated_dir": (
                    str(e.annotated_dir.relative_to(base_dir))
                    if e.annotated_dir is not None
                    else None
                ),
            }
            for e in events
        ],
    }
    index_path.write_text(json.dumps(index, indent=2), encoding="utf-8")
    return index_path
