"""End-to-end pipeline evaluation against annotated fixture videos (gate G2).

Usage::

    python -m yolo_model.scripts.evaluate_pipeline \
        --fixtures ./yolo_model/fixtures/example_manifest.json \
        --output ./runs/highlight/v1/pipeline_eval \
        --report ./runs/highlight/v1/pipeline_report.json

For every fixture this script runs the *full* YOLO pipeline (cache → coarse →
fine → events → assets) and compares the detected events to the ground-truth
spans in the manifest. The reported metrics are:

  - per-fixture and aggregate **precision / recall / F1** at the event level
    (a prediction matches ground truth when their temporal IoU ≥ ``--iou``).
  - **mean temporal IoU** over matched pairs.
  - **false-positive-event rate** = unmatched predictions / total predictions.
  - per-fixture timings.

Gate G2 to flip ``VIDEO_HIGHLIGHT_DETECTOR=yolo`` to default:
recall ≥ 0.90 and false-positive-event rate ≤ 0.05 across the suite.

Run this from the repository root so that ``agents.feedback.highlight`` imports
resolve cleanly.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from ._repo_path import ensure_repo_root_on_path

ensure_repo_root_on_path()

LOG = logging.getLogger("highlight.evaluate_pipeline")


@dataclass
class FixtureGroundTruth:
    name: str
    video: str
    events: list[dict[str, float]]


@dataclass
class FixtureMetrics:
    name: str
    video: str
    gt_events: int
    pred_events: int
    matched: int
    precision: float
    recall: float
    f1: float
    mean_iou_temporal: float
    elapsed_sec: float
    detail: dict[str, Any] = field(default_factory=dict)


def _load_manifest(path: Path) -> list[FixtureGroundTruth]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    out: list[FixtureGroundTruth] = []
    for item in payload.get("fixtures", []):
        out.append(
            FixtureGroundTruth(
                name=str(item["name"]),
                video=str(item["video"]),
                events=[{"t_on": float(e["t_on"]), "t_off": float(e["t_off"])} for e in item.get("events", [])],
            )
        )
    return out


def _temporal_iou(a: dict[str, float], b: dict[str, float]) -> float:
    a_lo, a_hi = float(a["t_on"]), float(a["t_off"])
    b_lo, b_hi = float(b["t_on"]), float(b["t_off"])
    inter = max(0.0, min(a_hi, b_hi) - max(a_lo, b_lo))
    union = max(a_hi, b_hi) - min(a_lo, b_lo)
    if union <= 0:
        return 0.0
    return inter / union


def _match_events(
    gt: list[dict[str, float]],
    pred: list[dict[str, float]],
    *,
    iou_threshold: float,
) -> tuple[int, list[float]]:
    """Greedy best-IoU matching. Returns (#matches, [iou for matched pairs])."""
    if not gt or not pred:
        return 0, []
    pairs: list[tuple[float, int, int]] = []
    for gi, g in enumerate(gt):
        for pi, p in enumerate(pred):
            iou = _temporal_iou(g, p)
            if iou >= iou_threshold:
                pairs.append((iou, gi, pi))
    pairs.sort(reverse=True)
    used_gt: set[int] = set()
    used_pred: set[int] = set()
    ious: list[float] = []
    for iou, gi, pi in pairs:
        if gi in used_gt or pi in used_pred:
            continue
        used_gt.add(gi)
        used_pred.add(pi)
        ious.append(iou)
    return len(ious), ious


def _evaluate_fixture(fixture: FixtureGroundTruth, *, output_root: Path, iou_threshold: float) -> FixtureMetrics:
    # Lazy import so the rest of the script can run for --help even if ultralytics is missing.
    from agents.feedback.highlight import run_yolo_pipeline
    from agents.feedback.highlight.yolo_detector import HighlightDetectorUnavailable

    fixture_dir = output_root / fixture.name
    fixture_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()
    try:
        pipeline = run_yolo_pipeline(video_url=fixture.video, base_dir=fixture_dir)
    except HighlightDetectorUnavailable as exc:
        LOG.error("Detector unavailable for %s: %s", fixture.name, exc)
        return FixtureMetrics(
            name=fixture.name,
            video=fixture.video,
            gt_events=len(fixture.events),
            pred_events=0,
            matched=0,
            precision=0.0,
            recall=0.0,
            f1=0.0,
            mean_iou_temporal=0.0,
            elapsed_sec=round(time.time() - started, 2),
            detail={"error": str(exc)},
        )

    pred_events = [
        {"t_on": float(a.event.t_on), "t_off": float(a.event.t_off)}
        for a in pipeline.events
    ]
    matched, ious = _match_events(fixture.events, pred_events, iou_threshold=iou_threshold)
    precision = matched / max(1, len(pred_events)) if pred_events else (1.0 if not fixture.events else 0.0)
    recall = matched / max(1, len(fixture.events)) if fixture.events else (1.0 if not pred_events else 0.0)
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
    mean_iou = sum(ious) / len(ious) if ious else 0.0
    return FixtureMetrics(
        name=fixture.name,
        video=fixture.video,
        gt_events=len(fixture.events),
        pred_events=len(pred_events),
        matched=matched,
        precision=round(precision, 4),
        recall=round(recall, 4),
        f1=round(f1, 4),
        mean_iou_temporal=round(mean_iou, 4),
        elapsed_sec=round(time.time() - started, 2),
        detail=pipeline.to_debug_dict(),
    )


def _aggregate(metrics: list[FixtureMetrics]) -> dict[str, float]:
    total_gt = sum(m.gt_events for m in metrics)
    total_pred = sum(m.pred_events for m in metrics)
    total_matched = sum(m.matched for m in metrics)
    fp = max(0, total_pred - total_matched)
    precision = total_matched / max(1, total_pred) if total_pred else (1.0 if not total_gt else 0.0)
    recall = total_matched / max(1, total_gt) if total_gt else (1.0 if not total_pred else 0.0)
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
    fp_rate = fp / max(1, total_pred) if total_pred else 0.0
    return {
        "total_gt_events": total_gt,
        "total_pred_events": total_pred,
        "matched_events": total_matched,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "false_positive_event_rate": round(fp_rate, 4),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fixtures", type=Path, required=True, help="Path to fixtures manifest JSON.")
    parser.add_argument("--output", type=Path, default=Path("./runs/highlight/pipeline_eval"), help="Working directory for pipeline outputs.")
    parser.add_argument("--report", type=Path, default=None, help="Optional JSON report output.")
    parser.add_argument("--iou", type=float, default=0.5, help="Temporal IoU threshold for matching predictions to ground truth (default 0.5).")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.fixtures.is_file():
        LOG.error("Fixtures manifest not found at %s", args.fixtures)
        return 2

    fixtures = _load_manifest(args.fixtures)
    if not fixtures:
        LOG.error("Manifest contained no fixtures.")
        return 2
    args.output.mkdir(parents=True, exist_ok=True)

    metrics: list[FixtureMetrics] = []
    for fixture in fixtures:
        LOG.info("Evaluating fixture: %s (%s)", fixture.name, fixture.video)
        m = _evaluate_fixture(fixture, output_root=args.output, iou_threshold=args.iou)
        LOG.info(
            "  -> gt=%d pred=%d matched=%d  P=%.3f R=%.3f F1=%.3f  IoU=%.3f  time=%.1fs",
            m.gt_events, m.pred_events, m.matched, m.precision, m.recall, m.f1, m.mean_iou_temporal, m.elapsed_sec,
        )
        metrics.append(m)

    aggregate = _aggregate(metrics)
    LOG.info("Aggregate: %s", aggregate)

    promotion_gate_ok = aggregate["recall"] >= 0.9 and aggregate["false_positive_event_rate"] <= 0.05
    LOG.info("Promotion gate (recall>=0.90, fp_rate<=0.05): %s", "PASS" if promotion_gate_ok else "FAIL")

    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(
                {
                    "iou_threshold": args.iou,
                    "promotion_gate_ok": promotion_gate_ok,
                    "aggregate": aggregate,
                    "fixtures": [asdict(m) for m in metrics],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        LOG.info("Report written to %s", args.report)
    return 0 if promotion_gate_ok else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
