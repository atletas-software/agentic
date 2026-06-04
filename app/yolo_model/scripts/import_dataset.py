"""Import positive (and optional negative) image folders into a YOLOv8 dataset layout.

Typical workflow after labeling in Roboflow and downloading annotated previews,
or when you have a folder of positive frame images ready for training prep::

    cd yolo_model
    python -m scripts.import_dataset \
        --version v1.0.0 \
        --val-ratio 0.2

By default the script reads from:

- ``yolo_model/datasets/sources/positives``
- ``yolo_model/datasets/sources/negatives`` (optional, empty is fine)

Override with ``--positives-dir`` / ``--negatives-dir`` when needed.

Output layout (under ``dataset/<version>/``)::

    data.yaml
    DATASET_CARD.md
    images/train/*.jpg
    images/val/*.jpg
    labels/train/*.txt
    labels/val/*.txt

Positives get bootstrap ``highlight_overlay`` boxes from the legacy HSV/Hough
detector unless you pass ``--labels-dir`` with a Roboflow YOLO export
(images/ + labels/ mirrored by stem).

Negatives (``--negatives-dir``) are copied with empty label files.
"""

from __future__ import annotations

import argparse
import logging
import random
import shutil
import sys
from pathlib import Path

from ._repo_path import ensure_repo_root_on_path

ensure_repo_root_on_path()

from agents.feedback.video_utils import _bbox_from_circle, detect_highlight_overlay

LOG = logging.getLogger("highlight.import_dataset")

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
YOLO_MODEL_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET_ROOT = YOLO_MODEL_ROOT / "datasets"
DEFAULT_POSITIVES_DIR = DEFAULT_DATASET_ROOT / "sources" / "positives"
DEFAULT_NEGATIVES_DIR = DEFAULT_DATASET_ROOT / "sources" / "negatives"


def _iter_images(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(
        p for p in directory.iterdir()
        if p.is_file() and p.suffix.lower() in IMAGE_EXTS
    )


def _bbox_to_yolo_line(bbox: tuple[int, int, int, int], img_w: int, img_h: int) -> str:
    x, y, w, h = bbox
    cx = (x + w / 2.0) / img_w
    cy = (y + h / 2.0) / img_h
    nw = w / img_w
    nh = h / img_h
    return f"0 {cx:.6f} {cy:.6f} {nw:.6f} {nh:.6f}"


def _bootstrap_label(image_path: Path) -> str | None:
    import cv2

    result = detect_highlight_overlay(image_path)
    if not result.get("found"):
        return None
    image = cv2.imread(str(image_path))
    if image is None:
        return None
    h, w = image.shape[:2]
    bbox = result.get("bbox")
    if bbox is None:
        bbox = _bbox_from_circle(result, image)
    return _bbox_to_yolo_line(bbox, w, h)


def _copy_image(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if src.suffix.lower() == dest.suffix.lower():
        shutil.copy2(src, dest)
        return
    import cv2

    image = cv2.imread(str(src))
    if image is None:
        shutil.copy2(src, dest)
        return
    cv2.imwrite(str(dest), image)


def _copy_to_sources_if_needed(src: Path, dest_dir: Path) -> None:
    """Copy into dataset/sources unless source is already there."""
    dest = dest_dir / src.name
    try:
        if src.resolve() == dest.resolve():
            return
    except FileNotFoundError:
        # If either path cannot resolve yet, fall back to copy behavior.
        pass
    shutil.copy2(src, dest)


def _write_data_yaml(root: Path) -> None:
    content = (
        f"path: {root.resolve()}\n"
        "train: images/train\n"
        "val: images/val\n"
        "names:\n"
        "  0: highlight_overlay\n"
    )
    (root / "data.yaml").write_text(content, encoding="utf-8")


def _write_dataset_card(
    root: Path,
    *,
    version: str,
    n_train_pos: int,
    n_val_pos: int,
    n_train_neg: int,
    n_val_neg: int,
    positives_src: Path,
    negatives_src: Path | None,
    bootstrap: bool,
    labels_dir: Path | None,
    bootstrap_misses: int,
) -> None:
    lines = [
        f"# dataset {version}",
        "",
        f"- Created: auto-import via `scripts/import_dataset.py`",
        f"- Positives source: `{positives_src.resolve()}`",
        f"- Negatives source: `{negatives_src.resolve() if negatives_src else 'none'}`",
        f"- Train: {n_train_pos} positive, {n_train_neg} negative images",
        f"- Val: {n_val_pos} positive, {n_val_neg} negative images",
        f"- Labels: {'Roboflow YOLO dir' if labels_dir else 'HSV bootstrap' if bootstrap else 'MISSING — add labels before training'}",
        f"- Bootstrap misses (no box written): {bootstrap_misses}",
        "- Class: `highlight_overlay` (single class)",
        "- Label rules: see `docs/data_strategy.md`",
        "",
        "## Notes",
        "",
        "- Replace bootstrap labels with a Roboflow **YOLOv8** export when available.",
        "- Val images are a random split; for production, split by **video**, not frame.",
    ]
    (root / "DATASET_CARD.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _find_external_label(stem: str, labels_dir: Path) -> Path | None:
    for sub in ("train", "val", ""):
        base = labels_dir / sub if sub else labels_dir
        for ext in (".txt",):
            candidate = base / f"{stem}{ext}"
            if candidate.is_file():
                return candidate
    return None


def _stage_split(
    items: list[Path],
    *,
    split_name: str,
    images_out: Path,
    labels_out: Path,
    prefix: str,
    bootstrap: bool,
    labels_dir: Path | None,
    external_stems: dict[str, Path] | None,
) -> tuple[int, int]:
    """Copy images and labels. Returns (written_with_box, bootstrap_misses)."""
    written = 0
    misses = 0
    for idx, src in enumerate(items):
        stem = f"{prefix}_{idx:05d}"
        dest_img = images_out / f"{stem}.jpg"
        dest_lbl = labels_out / f"{stem}.txt"
        _copy_image(src, dest_img)

        label_text = ""
        if external_stems and src.stem in external_stems:
            label_text = external_stems[src.stem].read_text(encoding="utf-8").strip()
        elif labels_dir:
            ext_lbl = _find_external_label(src.stem, labels_dir)
            if ext_lbl:
                label_text = ext_lbl.read_text(encoding="utf-8").strip()
        elif bootstrap and prefix == "pos":
            line = _bootstrap_label(dest_img)
            if line:
                label_text = line
                written += 1
            else:
                misses += 1
                LOG.warning("No bootstrap box for %s (%s)", src.name, split_name)

        dest_lbl.write_text(
            (label_text + "\n") if label_text else "",
            encoding="utf-8",
        )
    return written, misses


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--positives-dir",
        type=Path,
        default=DEFAULT_POSITIVES_DIR,
        help="Folder of positive labeled frames (default: yolo_model/datasets/sources/positives).",
    )
    parser.add_argument(
        "--negatives-dir",
        type=Path,
        default=DEFAULT_NEGATIVES_DIR,
        help="Folder of hard-negative frames (default: yolo_model/datasets/sources/negatives).",
    )
    parser.add_argument("--labels-dir", type=Path, default=None, help="Optional Roboflow YOLO labels/ export root.")
    parser.add_argument("--version", default="v1.0.0", help="Dataset version folder name.")
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=DEFAULT_DATASET_ROOT,
        help="Parent of versioned datasets (default: yolo_model/datasets).",
    )
    parser.add_argument("--val-ratio", type=float, default=0.2, help="Fraction of images for val (0–0.5).")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--no-bootstrap-labels",
        action="store_true",
        help="Copy images only; leave positive label files empty (default: bootstrap HSV boxes).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    positives_dir = args.positives_dir.expanduser().resolve()
    if not positives_dir.is_dir():
        LOG.error("Positives directory not found: %s", positives_dir)
        return 2

    bootstrap = not args.no_bootstrap_labels
    val_ratio = max(0.05, min(0.5, args.val_ratio))

    positive_images = _iter_images(positives_dir)
    if not positive_images:
        LOG.error("No images found in %s", positives_dir)
        return 2

    negative_images: list[Path] = []
    if args.negatives_dir:
        neg_dir = args.negatives_dir.expanduser().resolve()
        if neg_dir.is_dir():
            negative_images = _iter_images(neg_dir)
        else:
            LOG.warning("Negatives directory not found, skipping: %s", neg_dir)

    yolo_root = (args.dataset_root / args.version).resolve()
    sources_pos = args.dataset_root / "sources" / "positives"
    sources_neg = args.dataset_root / "sources" / "negatives"

    # Reset version folder for a clean import
    if yolo_root.exists():
        shutil.rmtree(yolo_root)
    for sub in ("images/train", "images/val", "labels/train", "labels/val"):
        (yolo_root / sub).mkdir(parents=True, exist_ok=True)

    sources_pos.mkdir(parents=True, exist_ok=True)
    for src in positive_images:
        _copy_to_sources_if_needed(src, sources_pos)
    if negative_images:
        sources_neg.mkdir(parents=True, exist_ok=True)
        for src in negative_images:
            _copy_to_sources_if_needed(src, sources_neg)

    rng = random.Random(args.seed)
    pos_shuffled = positive_images[:]
    rng.shuffle(pos_shuffled)
    n_val_pos = max(1, int(len(pos_shuffled) * val_ratio)) if len(pos_shuffled) > 4 else 1
    val_pos = pos_shuffled[:n_val_pos]
    train_pos = pos_shuffled[n_val_pos:]

    neg_shuffled = negative_images[:]
    rng.shuffle(neg_shuffled)
    n_val_neg = max(0, int(len(neg_shuffled) * val_ratio)) if neg_shuffled else 0
    val_neg = neg_shuffled[:n_val_neg]
    train_neg = neg_shuffled[n_val_neg:]

    labels_dir = args.labels_dir.expanduser().resolve() if args.labels_dir else None
    total_written = 0
    total_misses = 0

    for split_name, pos_list, neg_list, img_sub, lbl_sub in (
        ("train", train_pos, train_neg, "images/train", "labels/train"),
        ("val", val_pos, val_neg, "images/val", "labels/val"),
    ):
        w, m = _stage_split(
            pos_list,
            split_name=split_name,
            images_out=yolo_root / img_sub,
            labels_out=yolo_root / lbl_sub,
            prefix="pos",
            bootstrap=bootstrap,
            labels_dir=labels_dir,
            external_stems=None,
        )
        total_written += w
        total_misses += m
        _stage_split(
            neg_list,
            split_name=split_name,
            images_out=yolo_root / img_sub,
            labels_out=yolo_root / lbl_sub,
            prefix="neg",
            bootstrap=False,
            labels_dir=None,
            external_stems=None,
        )

    _write_data_yaml(yolo_root)
    _write_dataset_card(
        yolo_root,
        version=args.version,
        n_train_pos=len(train_pos),
        n_val_pos=len(val_pos),
        n_train_neg=len(train_neg),
        n_val_neg=len(val_neg),
        positives_src=positives_dir,
        negatives_src=args.negatives_dir,
        bootstrap=bootstrap,
        labels_dir=labels_dir,
        bootstrap_misses=total_misses,
    )

    LOG.info("Dataset ready at %s", yolo_root)
    LOG.info("  train: %d pos + %d neg", len(train_pos), len(train_neg))
    LOG.info("  val:   %d pos + %d neg", len(val_pos), len(val_neg))
    LOG.info("  bootstrap boxes written: %d, misses: %d", total_written, total_misses)
    LOG.info("Train with: python -m scripts.train --data %s/data.yaml --device 0", yolo_root)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
