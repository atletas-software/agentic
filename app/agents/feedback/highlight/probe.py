"""Two-pass YOLO probing for highlight overlays.

Pipeline overview:

1. **Coarse pass** — sample the full video at a low frame rate (default 1 fps)
   in chunks, run YOLO batch inference on every JPEG. This finds approximate
   regions where the overlay is on.
2. **Fine pass** — for each *candidate window* (contiguous coarse ON probes,
   widened by a small padding) re-sample at a higher rate (default 5 fps i.e.
   one frame every 0.2s) and run YOLO again. This tightens the t_on / t_off
   boundaries so the downstream event extractor sees a clean signal.

Both passes feed ``HighlightDetector.predict_paths`` and produce a unified
list of ``ProbeSample(timestamp_sec, detection)``.

Performance note: a 10-minute video that used to need ~2400 single-frame
ffmpeg seeks now needs ~10 batched ``ffmpeg`` invocations plus YOLO inference
on ~600 coarse frames and ~50–200 fine frames.
"""

from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from agents.feedback.highlight.ffmpeg_batch import (
    SampledFrame,
    sample_range_uniform,
    sample_video_uniform,
)
from agents.feedback.highlight.yolo_detector import (
    Detection,
    HighlightDetector,
    HighlightDetectorUnavailable,
)

LOG = logging.getLogger("highlight.probe")


@dataclass
class ProbeSample:
    timestamp_sec: float
    image_path: Path
    detection: Detection

    @property
    def found(self) -> bool:
        return self.detection.found

    @property
    def conf(self) -> float:
        return float(self.detection.confidence)


@dataclass
class ProbeResult:
    coarse: list[ProbeSample]
    fine: list[ProbeSample]
    candidate_windows: list[tuple[float, float]]
    timings: dict[str, float]

    def merged_sorted(self) -> list[ProbeSample]:
        """Coarse + fine probes deduplicated by timestamp (fine wins), sorted by t."""
        bag: dict[float, ProbeSample] = {}
        for s in self.coarse:
            bag[round(s.timestamp_sec, 3)] = s
        for s in self.fine:
            bag[round(s.timestamp_sec, 3)] = s
        return sorted(bag.values(), key=lambda x: x.timestamp_sec)


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _coarse_fps() -> float:
    interval = _env_float("VIDEO_HIGHLIGHT_COARSE_INTERVAL_SEC", 1.0)
    return max(0.1, 1.0 / max(0.05, interval))


def _fine_fps() -> float:
    interval = _env_float("VIDEO_HIGHLIGHT_FINE_INTERVAL_SEC", 0.2)
    return max(0.1, 1.0 / max(0.05, interval))


def _coarse_chunk_sec() -> float:
    return _env_float("VIDEO_HIGHLIGHT_COARSE_CHUNK_SEC", 60.0)


def _fine_pad_sec() -> float:
    """Padding added to each coarse candidate window before re-sampling."""
    return _env_float("VIDEO_HIGHLIGHT_FINE_WINDOW_PAD_SEC", 1.0)


def _merge_window_sec() -> float:
    """Coarse ON probes within this gap are treated as the same window."""
    return _env_float("VIDEO_HIGHLIGHT_COARSE_MERGE_GAP_SEC", 1.5)


def _coarse_frame_width() -> int:
    raw = (os.getenv("VIDEO_HIGHLIGHT_COARSE_FRAME_WIDTH") or "640").strip()
    try:
        return max(160, int(raw))
    except ValueError:
        return 640


def _fine_frame_width() -> int:
    raw = (os.getenv("VIDEO_HIGHLIGHT_FINE_FRAME_WIDTH") or "640").strip()
    try:
        return max(160, int(raw))
    except ValueError:
        return 640


def _cleanup_probe_dirs() -> bool:
    raw = (os.getenv("VIDEO_HIGHLIGHT_KEEP_PROBE_FRAMES") or "false").strip().lower()
    return raw in {"0", "false", "no", "off", ""}


def _coarse_candidate_windows(
    coarse_probes: list[ProbeSample],
    *,
    pad_sec: float,
    merge_gap_sec: float,
    duration_sec: float,
) -> list[tuple[float, float]]:
    """Group consecutive ON coarse probes into windows, padded for the fine pass."""
    on_ts = [s.timestamp_sec for s in coarse_probes if s.found]
    if not on_ts:
        return []
    on_ts.sort()
    windows: list[list[float]] = [[on_ts[0], on_ts[0]]]
    for ts in on_ts[1:]:
        if ts - windows[-1][1] <= merge_gap_sec:
            windows[-1][1] = ts
        else:
            windows.append([ts, ts])

    padded: list[tuple[float, float]] = []
    for lo, hi in windows:
        padded.append(
            (
                max(0.0, lo - pad_sec),
                min(duration_sec, hi + pad_sec),
            )
        )
    # Merge windows that now overlap after padding.
    padded.sort()
    merged: list[tuple[float, float]] = []
    for lo, hi in padded:
        if merged and lo <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
        else:
            merged.append((lo, hi))
    return merged


def run_two_pass_probe(
    *,
    video_input: str,
    duration_sec: float,
    output_root: Path,
    detector: HighlightDetector,
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> ProbeResult:
    """Run coarse + fine YOLO probing across the entire video.

    ``video_input`` should be a local file path or HTTPS URL — when caching is
    on (handled by the caller) this will already be a local path.
    """
    import time as _time

    if not detector.is_available():
        raise HighlightDetectorUnavailable("YOLO highlight detector is not available.")

    output_root.mkdir(parents=True, exist_ok=True)
    coarse_dir = output_root / "coarse"
    fine_dir = output_root / "fine"
    coarse_dir.mkdir(parents=True, exist_ok=True)
    fine_dir.mkdir(parents=True, exist_ok=True)

    timings: dict[str, float] = {}

    if on_progress:
        on_progress(
            {
                "phase": "highlight_yolo_coarse_extract",
                "progress_detail": f"Coarse sampling at {_coarse_fps():.2f} fps across {duration_sec:.1f}s…",
            }
        )

    t0 = _time.time()
    coarse_sampled = sample_video_uniform(
        video_url=video_input,
        duration_sec=duration_sec,
        output_dir=coarse_dir,
        prefix="coarse",
        fps=_coarse_fps(),
        frame_width=_coarse_frame_width(),
        chunk_sec=_coarse_chunk_sec(),
    )
    timings["coarse_extract_sec"] = round(_time.time() - t0, 2)

    if cancel_check is not None and cancel_check():
        raise RuntimeError("Review cancelled by user")

    if on_progress:
        on_progress(
            {
                "phase": "highlight_yolo_coarse_infer",
                "progress_detail": f"Coarse YOLO inference on {len(coarse_sampled)} frame(s)…",
            }
        )

    t0 = _time.time()
    coarse_dets = detector.predict_paths([s.image_path for s in coarse_sampled])
    timings["coarse_infer_sec"] = round(_time.time() - t0, 2)

    coarse_probes = _make_probes(coarse_sampled, coarse_dets)
    on_count = sum(1 for p in coarse_probes if p.found)
    LOG.info("Coarse probe done: %d frames, %d on, in %.2fs", len(coarse_probes), on_count, timings["coarse_extract_sec"] + timings["coarse_infer_sec"])

    windows = _coarse_candidate_windows(
        coarse_probes,
        pad_sec=_fine_pad_sec(),
        merge_gap_sec=_merge_window_sec(),
        duration_sec=duration_sec,
    )

    fine_probes: list[ProbeSample] = []
    if windows:
        if on_progress:
            on_progress(
                {
                    "phase": "highlight_yolo_fine_extract",
                    "progress_detail": f"Found {len(windows)} candidate window(s); fine-scanning at {_fine_fps():.2f} fps…",
                }
            )

        t0 = _time.time()
        fine_sampled: list[SampledFrame] = []
        for idx, (t_lo, t_hi) in enumerate(windows, start=1):
            if cancel_check is not None and cancel_check():
                raise RuntimeError("Review cancelled by user")
            window_prefix = f"fine_{idx:03d}"
            fine_sampled.extend(
                sample_range_uniform(
                    video_url=video_input,
                    t_lo=t_lo,
                    t_hi=t_hi,
                    output_dir=fine_dir,
                    prefix=window_prefix,
                    fps=_fine_fps(),
                    frame_width=_fine_frame_width(),
                )
            )
        timings["fine_extract_sec"] = round(_time.time() - t0, 2)

        if on_progress:
            on_progress(
                {
                    "phase": "highlight_yolo_fine_infer",
                    "progress_detail": f"Fine YOLO inference on {len(fine_sampled)} frame(s)…",
                }
            )

        t0 = _time.time()
        fine_dets = detector.predict_paths([s.image_path for s in fine_sampled])
        timings["fine_infer_sec"] = round(_time.time() - t0, 2)
        fine_probes = _make_probes(fine_sampled, fine_dets)
        LOG.info("Fine probe done: %d frames, %d on, in %.2fs", len(fine_probes), sum(1 for p in fine_probes if p.found), timings["fine_extract_sec"] + timings["fine_infer_sec"])
    else:
        timings["fine_extract_sec"] = 0.0
        timings["fine_infer_sec"] = 0.0

    if _cleanup_probe_dirs():
        shutil.rmtree(coarse_dir, ignore_errors=True)
        shutil.rmtree(fine_dir, ignore_errors=True)

    return ProbeResult(
        coarse=coarse_probes,
        fine=fine_probes,
        candidate_windows=windows,
        timings=timings,
    )


def _make_probes(samples: list[SampledFrame], detections: list[Detection]) -> list[ProbeSample]:
    pairs: list[ProbeSample] = []
    for s, d in zip(samples, detections):
        pairs.append(ProbeSample(timestamp_sec=s.timestamp_sec, image_path=s.image_path, detection=d))
    pairs.sort(key=lambda x: x.timestamp_sec)
    return pairs
