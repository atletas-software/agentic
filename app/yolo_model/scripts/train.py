"""Fine-tune YOLOv8 on the single class ``highlight_overlay``.

This script is meant to run outside the deployed feedback agent — on a developer
Mac or in a Colab/cloud GPU notebook. It deliberately keeps dependencies thin:
``ultralytics`` is imported lazily so the rest of the package stays importable
without it.

Expected dataset layout (YOLOv8 / Roboflow export format):

    <data_root>/
      data.yaml
      images/train/*.jpg
      images/val/*.jpg
      labels/train/*.txt
      labels/val/*.txt

If you exported from Roboflow choose ``YOLOv8`` format and unzip into <data_root>.
``data.yaml`` should look like::

    path: <data_root>
    train: images/train
    val: images/val
    names:
      0: highlight_overlay

Usage::

    pip install ultralytics
    python -m yolo_model.scripts.train \
        --data ./dataset/data.yaml \
        --weights yolov8n.pt \
        --epochs 80 \
        --imgsz 640 \
        --batch 16 \
        --data yolo_model/datasets/athlete_focus/v1.1.0/data.yaml \
        --name highlight_v1.1.0

The best weights land at ``yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt``.
Stage to ``yolo_model/exports/highlight_yolo_v1.1.0.pt`` and copy to
``agents/feedback/models/highlight_yolo_v1.pt`` (the runtime location exposed
via the ``VIDEO_HIGHLIGHT_YOLO_WEIGHTS`` env var).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from yolo_model.config.paths import artifacts_dir, default_dataset_yaml

LOG = logging.getLogger("highlight.train")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--data",
        type=Path,
        default=None,
        help="Path to data.yaml (default: datasets/athlete_focus/v1.1.0/data.yaml).",
    )
    parser.add_argument("--weights", default="yolov8n.pt", help="Base weights (pretrained checkpoint).")
    parser.add_argument("--epochs", type=int, default=80)
    parser.add_argument("--imgsz", type=int, default=640)
    parser.add_argument("--batch", type=int, default=16)
    parser.add_argument("--device", default="", help="ultralytics device string (e.g. 'cpu', '0', 'mps'). Empty = auto.")
    parser.add_argument(
        "--project",
        type=Path,
        default=None,
        help="Ultralytics project dir (default: absolute yolo_model/artifacts/train).",
    )
    parser.add_argument("--name", default="v1")
    parser.add_argument("--patience", type=int, default=20, help="Early-stop patience (epochs).")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--hsv-h", type=float, default=0.02, help="HSV-Hue augmentation gain (default 0.02 — overlays are color-sensitive).")
    parser.add_argument("--mosaic", type=float, default=1.0)
    parser.add_argument("--mixup", type=float, default=0.05)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    data_yaml = (args.data or default_dataset_yaml()).resolve()
    if not data_yaml.is_file():
        LOG.error("data.yaml not found at %s", data_yaml)
        return 2

    try:
        from ultralytics import YOLO
    except ImportError:
        LOG.error("ultralytics is not installed. Run: pip install ultralytics")
        return 2

    project = (args.project or (artifacts_dir() / "train")).resolve()
    project.mkdir(parents=True, exist_ok=True)

    LOG.info("Loading base weights: %s", args.weights)
    model = YOLO(args.weights)

    LOG.info(
        "Starting train: data=%s epochs=%d imgsz=%d batch=%d device=%s",
        data_yaml, args.epochs, args.imgsz, args.batch, args.device or "auto",
    )
    results = model.train(
        data=str(data_yaml),
        epochs=args.epochs,
        imgsz=args.imgsz,
        batch=args.batch,
        device=args.device or None,
        project=str(project),
        name=args.name,
        patience=args.patience,
        seed=args.seed,
        hsv_h=args.hsv_h,
        mosaic=args.mosaic,
        mixup=args.mixup,
        plots=True,
        verbose=True,
    )

    best = project / args.name / "weights" / "best.pt"
    if best.is_file():
        LOG.info("Training complete. Best weights at %s", best)
        LOG.info("Stage as yolo_model/exports/highlight_yolo_v1.1.0.pt, then copy to agents/feedback/models/highlight_yolo_v1.pt for runtime.")
    else:
        LOG.warning("Training finished but best.pt not found under %s", best.parent)
    LOG.info("Summary: %s", results)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
