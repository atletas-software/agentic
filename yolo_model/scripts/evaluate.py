"""Evaluate a trained highlight-overlay YOLO model on the val split (gate G1).

Usage::

    python -m yolo_model.scripts.evaluate \
        --weights ./runs/highlight/v1/weights/best.pt \
        --data ./dataset/data.yaml

Reports precision, recall, mAP50, mAP50-95 and per-image confusion stats. Use
this BEFORE wiring a model into the agent — anything below ~0.85 recall on the
val set will probably perform worse than the legacy HSV detector on real videos.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

LOG = logging.getLogger("highlight.evaluate")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--weights", type=Path, required=True, help="Path to best.pt.")
    parser.add_argument("--data", type=Path, required=True, help="Path to data.yaml.")
    parser.add_argument("--imgsz", type=int, default=640)
    parser.add_argument("--device", default="", help="ultralytics device string.")
    parser.add_argument("--conf", type=float, default=0.001)
    parser.add_argument("--iou", type=float, default=0.6)
    parser.add_argument("--report", type=Path, default=None, help="Optional JSON report output.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        from ultralytics import YOLO
    except ImportError:
        LOG.error("ultralytics is not installed. Run: pip install ultralytics")
        return 2

    if not args.weights.is_file():
        LOG.error("Weights not found at %s", args.weights)
        return 2
    if not args.data.is_file():
        LOG.error("data.yaml not found at %s", args.data)
        return 2

    model = YOLO(str(args.weights))
    metrics = model.val(
        data=str(args.data),
        imgsz=args.imgsz,
        device=args.device or None,
        conf=args.conf,
        iou=args.iou,
        verbose=True,
    )

    summary: dict[str, object] = {
        "weights": str(args.weights),
        "data": str(args.data),
        "imgsz": args.imgsz,
        "mp": float(getattr(metrics.box, "mp", 0.0) or 0.0),
        "mr": float(getattr(metrics.box, "mr", 0.0) or 0.0),
        "map50": float(getattr(metrics.box, "map50", 0.0) or 0.0),
        "map": float(getattr(metrics.box, "map", 0.0) or 0.0),
    }
    LOG.info("precision=%(mp).3f recall=%(mr).3f mAP50=%(map50).3f mAP50-95=%(map).3f", summary)

    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        LOG.info("Report written to %s", args.report)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
