"""Sample frames from videos for labeling the highlight-overlay YOLO detector.

Usage from ``yolo_model/`` (recommended while iterating):

    cd yolo_model
    python -m scripts.prepare_dataset \
        --manifest data/manifest.txt \
        --output ./datasets/raw \
        --interval-sec 0.5 \
        --frame-width 640

Or from the repository root:

    python -m yolo_model.scripts.prepare_dataset \
        --manifest yolo_model/data/manifest.txt \
        --output yolo_model/datasets/raw \
        --interval-sec 0.5 \
        --frame-width 640

By default we also try the legacy HSV detector to *suggest* which frames are
likely positives, so a human can label faster. Suggestions are written to a
``suggestions.csv`` per video; they are NOT ground truth.

Notes:
  - Frames are written as ``frame_<ts>.jpg`` where <ts> is seconds * 100, e.g.
    ``frame_001275.jpg`` for 12.75s. This keeps filenames sortable and unique.
  - Negative-only videos can be passed via ``--negatives-dir`` to bias the
    dataset toward hard negatives (red jerseys, ads, etc.).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from ._repo_path import ensure_repo_root_on_path

ensure_repo_root_on_path()

from agents.feedback.video_utils import (
    detect_highlight_overlay,
    extract_frame_at_timestamp,
    probe_duration,
)

LOG = logging.getLogger("highlight.prepare_dataset")
VIDEO_EXTS = {".mp4", ".mov", ".mkv", ".webm", ".m4v", ".avi"}


@dataclass
class VideoSpec:
    source: str
    label_hint: str  # "positive_mixed" | "negative"

    @property
    def video_id(self) -> str:
        digest = hashlib.sha1(self.source.encode("utf-8")).hexdigest()[:10]
        stem = Path(self.source).stem or "video"
        return f"{stem}_{digest}"


def _iter_local_videos(directory: Path) -> Iterable[Path]:
    for p in sorted(directory.rglob("*")):
        if p.is_file() and p.suffix.lower() in VIDEO_EXTS:
            yield p


def _collect_videos(
    *,
    videos_dir: Optional[Path],
    negatives_dir: Optional[Path],
    manifest: Optional[Path],
) -> list[VideoSpec]:
    specs: list[VideoSpec] = []
    if manifest is not None:
        for raw_line in manifest.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            specs.append(VideoSpec(source=line, label_hint="positive_mixed"))
    if videos_dir is not None:
        for path in _iter_local_videos(videos_dir):
            specs.append(VideoSpec(source=str(path), label_hint="positive_mixed"))
    if negatives_dir is not None:
        for path in _iter_local_videos(negatives_dir):
            specs.append(VideoSpec(source=str(path), label_hint="negative"))
    return specs


def _sample_one_video(
    spec: VideoSpec,
    *,
    output_root: Path,
    interval_sec: float,
    frame_width: int,
    suggest: bool,
    skip_tail_sec: float,
) -> dict:
    out_dir = output_root / spec.video_id
    out_dir.mkdir(parents=True, exist_ok=True)
    duration = probe_duration(spec.source)
    if duration <= 0:
        raise RuntimeError(f"Invalid duration for {spec.source}")

    last_t = max(0.0, duration - max(0.0, skip_tail_sec))
    steps: list[float] = []
    t = 0.0
    while t <= last_t + 1e-6:
        steps.append(round(t, 3))
        t += interval_sec

    rows: list[dict[str, object]] = []
    LOG.info("Sampling %s (%.1fs, %d frames)", spec.source, duration, len(steps))
    for ts in steps:
        stamp = int(round(ts * 100))
        path = out_dir / f"frame_{stamp:08d}.jpg"
        if path.exists() and path.stat().st_size > 1024:
            continue
        try:
            extract_frame_at_timestamp(spec.source, ts, path, frame_width=frame_width)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Skipping ts=%.2fs (%s): %s", ts, spec.source, exc)
            continue

        suggestion = {"hsv_found": False, "method": "", "conf": 0.0}
        if suggest and spec.label_hint != "negative":
            try:
                det = detect_highlight_overlay(path)
                suggestion["hsv_found"] = bool(det.get("found"))
                suggestion["method"] = str(det.get("method") or "")
                suggestion["conf"] = float(det.get("score") or 0.0)
            except Exception:  # noqa: BLE001
                # The suggestion is best-effort; never fail sampling because of it.
                pass

        rows.append(
            {
                "file": path.name,
                "timestamp_sec": ts,
                "label_hint": spec.label_hint,
                **suggestion,
            }
        )

    suggestions_path = out_dir / "suggestions.csv"
    with suggestions_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["file", "timestamp_sec", "label_hint", "hsv_found", "method", "conf"],
        )
        writer.writeheader()
        writer.writerows(rows)

    return {
        "video_id": spec.video_id,
        "source": spec.source,
        "duration_sec": round(duration, 2),
        "frame_count": len(rows),
        "output_dir": str(out_dir),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--videos-dir", type=Path, default=None, help="Directory of source videos (recursive).")
    parser.add_argument("--negatives-dir", type=Path, default=None, help="Directory of negative-only videos.")
    parser.add_argument("--manifest", type=Path, default=None, help="Text file: one local path or HTTPS URL per line.")
    parser.add_argument("--output", type=Path, required=True, help="Destination root (frames written under <out>/<video_id>/).")
    parser.add_argument("--interval-sec", type=float, default=0.5, help="Sample one frame every N seconds.")
    parser.add_argument("--frame-width", type=int, default=640, help="Resize width (preserves aspect).")
    parser.add_argument("--skip-tail-sec", type=float, default=0.5, help="Avoid sampling past duration - this.")
    parser.add_argument("--no-suggest", action="store_true", help="Skip HSV-based pre-labeling suggestions.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not any([args.videos_dir, args.negatives_dir, args.manifest]):
        parser.error("Provide --videos-dir, --negatives-dir, or --manifest.")

    args.output.mkdir(parents=True, exist_ok=True)

    specs = _collect_videos(
        videos_dir=args.videos_dir,
        negatives_dir=args.negatives_dir,
        manifest=args.manifest,
    )
    if not specs:
        LOG.error("No videos matched. Nothing to sample.")
        return 2

    summary: list[dict] = []
    for spec in specs:
        try:
            summary.append(
                _sample_one_video(
                    spec,
                    output_root=args.output,
                    interval_sec=args.interval_sec,
                    frame_width=args.frame_width,
                    suggest=not args.no_suggest,
                    skip_tail_sec=args.skip_tail_sec,
                )
            )
        except Exception as exc:  # noqa: BLE001
            LOG.error("Failed for %s: %s", spec.source, exc)

    LOG.info("Done. %d video(s) sampled. Total frames: %d", len(summary), sum(s["frame_count"] for s in summary))
    for item in summary:
        LOG.info("  %s -> %s (%d frames, %.1fs)", item["source"], item["output_dir"], item["frame_count"], item["duration_sec"])
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
