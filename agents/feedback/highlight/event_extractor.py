"""Turn a noisy per-frame detection signal into clean highlight events.

A single YOLO probe per frame is *not* a clean event boundary — confidence
fluctuates frame-to-frame, the overlay can flicker briefly, and the same
highlight may span seconds. We post-process the raw probes with three classic
techniques:

1. **Hysteresis thresholding** — enter ON when conf >= ``conf_on`` (0.55) and
   stay ON until conf drops below ``conf_off`` (0.35). Suppresses flicker on
   borderline confidences.
2. **Min duration** — discard runs shorter than ``min_duration_sec`` (0.4s).
   Removes one-off false positives.
3. **Merge with IoU sanity** — merge two events whose gap is below
   ``merge_gap_sec`` (1.5s) **and** whose adjacent bboxes overlap (IoU > 0.3).
   This collapses a flickering highlight on the same player into one event but
   keeps two separate highlights on different players as separate events.

The output is a list of :class:`HighlightEvent` with t_on / t_off (the actual
visibility span) plus t_lo / t_hi (padded for downstream extraction).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Iterable

from agents.feedback.highlight.probe import ProbeSample

LOG = logging.getLogger("highlight.events")


@dataclass
class HighlightEvent:
    index: int
    t_on: float
    t_off: float
    t_lo: float                         # t_on - pad
    t_hi: float                         # t_off + pad
    mean_conf: float
    peak_conf: float
    probe_count: int
    bbox_track: list[tuple[float, dict[str, float]]] = field(default_factory=list)

    @property
    def duration_sec(self) -> float:
        return max(0.0, float(self.t_off) - float(self.t_on))

    @property
    def window_sec(self) -> float:
        return max(0.0, float(self.t_hi) - float(self.t_lo))

    @property
    def anchor_sec(self) -> float:
        return round((float(self.t_on) + float(self.t_off)) / 2.0, 3)

    def to_dict(self) -> dict:
        return {
            "index": int(self.index),
            "t_on": round(float(self.t_on), 3),
            "t_off": round(float(self.t_off), 3),
            "t_lo": round(float(self.t_lo), 3),
            "t_hi": round(float(self.t_hi), 3),
            "anchor_sec": self.anchor_sec,
            "duration_sec": round(self.duration_sec, 3),
            "window_sec": round(self.window_sec, 3),
            "mean_conf": round(float(self.mean_conf), 4),
            "peak_conf": round(float(self.peak_conf), 4),
            "probe_count": int(self.probe_count),
            "bbox_track": [
                (
                    round(float(ts), 3),
                    {k: round(float(v), 4) for k, v in box.items()},
                )
                for ts, box in self.bbox_track
            ],
        }


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


def _bbox_iou(a: dict[str, float], b: dict[str, float]) -> float:
    """IoU for two normalized (x, y, w, h) boxes."""
    if not a or not b:
        return 0.0
    ax1, ay1 = float(a.get("x", 0.0)), float(a.get("y", 0.0))
    ax2 = ax1 + float(a.get("w", 0.0))
    ay2 = ay1 + float(a.get("h", 0.0))
    bx1, by1 = float(b.get("x", 0.0)), float(b.get("y", 0.0))
    bx2 = bx1 + float(b.get("w", 0.0))
    by2 = by1 + float(b.get("h", 0.0))

    inter_x1 = max(ax1, bx1)
    inter_y1 = max(ay1, by1)
    inter_x2 = min(ax2, bx2)
    inter_y2 = min(ay2, by2)
    inter_w = max(0.0, inter_x2 - inter_x1)
    inter_h = max(0.0, inter_y2 - inter_y1)
    inter = inter_w * inter_h
    if inter <= 0:
        return 0.0
    area_a = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    area_b = max(0.0, bx2 - bx1) * max(0.0, by2 - by1)
    union = area_a + area_b - inter
    return inter / union if union > 0 else 0.0


@dataclass
class _Run:
    """Intermediate event before padding / IoU-merging."""
    t_on: float
    t_off: float
    confs: list[float] = field(default_factory=list)
    bbox_track: list[tuple[float, dict[str, float]]] = field(default_factory=list)

    @property
    def mean_conf(self) -> float:
        return sum(self.confs) / max(1, len(self.confs))

    @property
    def peak_conf(self) -> float:
        return max(self.confs) if self.confs else 0.0


def _hysteresis_runs(
    probes: list[ProbeSample],
    *,
    conf_on: float,
    conf_off: float,
) -> list[_Run]:
    runs: list[_Run] = []
    state_on = False
    current: _Run | None = None
    for probe in probes:
        c = float(probe.conf)
        bbox = dict(probe.detection.bbox) if probe.detection.bbox else {}
        if not state_on:
            if probe.found and c >= conf_on:
                state_on = True
                current = _Run(t_on=probe.timestamp_sec, t_off=probe.timestamp_sec)
                current.confs.append(c)
                if bbox:
                    current.bbox_track.append((probe.timestamp_sec, bbox))
        else:
            if probe.found and c >= conf_off:
                if current is not None:
                    current.t_off = probe.timestamp_sec
                    current.confs.append(c)
                    if bbox:
                        current.bbox_track.append((probe.timestamp_sec, bbox))
            else:
                if current is not None:
                    runs.append(current)
                state_on = False
                current = None
    if current is not None:
        runs.append(current)
    return runs


def _merge_close_runs(
    runs: list[_Run],
    *,
    merge_gap_sec: float,
    iou_threshold: float,
) -> list[_Run]:
    if not runs:
        return []
    merged: list[_Run] = [runs[0]]
    for nxt in runs[1:]:
        prev = merged[-1]
        gap = nxt.t_on - prev.t_off
        if gap > merge_gap_sec:
            merged.append(nxt)
            continue
        # IoU sanity: only merge if the two adjacent bboxes look like the same player.
        prev_box = prev.bbox_track[-1][1] if prev.bbox_track else {}
        nxt_box = nxt.bbox_track[0][1] if nxt.bbox_track else {}
        iou = _bbox_iou(prev_box, nxt_box)
        if iou >= iou_threshold or (not prev_box or not nxt_box):
            # Same player (or boxes unavailable) — merge.
            prev.t_off = max(prev.t_off, nxt.t_off)
            prev.confs.extend(nxt.confs)
            prev.bbox_track.extend(nxt.bbox_track)
        else:
            merged.append(nxt)
    return merged


def _cap_events(events: list[_Run], *, max_events: int) -> list[_Run]:
    if max_events <= 0 or len(events) <= max_events:
        return events
    ranked = sorted(events, key=lambda r: (r.mean_conf, r.peak_conf), reverse=True)[:max_events]
    ranked.sort(key=lambda r: r.t_on)
    return ranked


def build_events(
    probes: Iterable[ProbeSample],
    *,
    duration_sec: float,
    pad_sec: float | None = None,
    conf_on: float | None = None,
    conf_off: float | None = None,
    min_duration_sec: float | None = None,
    merge_gap_sec: float | None = None,
    iou_threshold: float | None = None,
    max_events: int | None = None,
) -> list[HighlightEvent]:
    """Turn raw probes into structured events with smoothing + padding."""
    probes_list = sorted(probes, key=lambda p: p.timestamp_sec)
    if not probes_list:
        return []

    pad = pad_sec if pad_sec is not None else _env_float("VIDEO_HIGHLIGHT_EVENT_PAD_SEC", 2.0)
    c_on = conf_on if conf_on is not None else _env_float("VIDEO_HIGHLIGHT_EVENT_CONF_ON", 0.55)
    c_off = conf_off if conf_off is not None else _env_float("VIDEO_HIGHLIGHT_EVENT_CONF_OFF", 0.35)
    min_dur = (
        min_duration_sec if min_duration_sec is not None else _env_float("VIDEO_HIGHLIGHT_EVENT_MIN_SEC", 0.4)
    )
    merge_gap = (
        merge_gap_sec if merge_gap_sec is not None else _env_float("VIDEO_HIGHLIGHT_EVENT_MERGE_SEC", 1.5)
    )
    iou_thr = (
        iou_threshold if iou_threshold is not None else _env_float("VIDEO_HIGHLIGHT_EVENT_MERGE_IOU", 0.3)
    )
    max_n = max_events if max_events is not None else _env_int("VIDEO_HIGHLIGHT_MAX_EVENTS", 25)

    # Defensive: conf_off must be <= conf_on, otherwise hysteresis is undefined.
    if c_off > c_on:
        c_off = max(0.0, c_on - 0.1)

    runs = _hysteresis_runs(probes_list, conf_on=c_on, conf_off=c_off)
    LOG.info("Hysteresis produced %d candidate runs", len(runs))

    runs = [r for r in runs if (r.t_off - r.t_on) >= min_dur or len(r.confs) >= 2]
    LOG.info("After min-duration filter (>=%.2fs): %d", min_dur, len(runs))

    runs = _merge_close_runs(runs, merge_gap_sec=merge_gap, iou_threshold=iou_thr)
    LOG.info("After IoU-aware merge (gap<=%.2fs, IoU>=%.2f): %d", merge_gap, iou_thr, len(runs))

    runs = _cap_events(runs, max_events=max_n)
    LOG.info("After cap (max=%d): %d", max_n, len(runs))

    events: list[HighlightEvent] = []
    for idx, run in enumerate(runs, start=1):
        t_lo = max(0.0, float(run.t_on) - pad)
        t_hi = min(float(duration_sec), float(run.t_off) + pad)
        events.append(
            HighlightEvent(
                index=idx,
                t_on=round(float(run.t_on), 3),
                t_off=round(float(run.t_off), 3),
                t_lo=round(float(t_lo), 3),
                t_hi=round(float(t_hi), 3),
                mean_conf=float(run.mean_conf),
                peak_conf=float(run.peak_conf),
                probe_count=len(run.confs),
                bbox_track=list(run.bbox_track),
            )
        )
    return events
