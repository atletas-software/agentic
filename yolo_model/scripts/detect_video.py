"""Run highlight-overlay YOLO detection on a video and export bbox/conf results.

This script is intentionally standalone for offline checks and demos. It does
not require wiring anything through ``agents/feedback``.

Usage (from repo root)::

    python -m yolo_model.scripts.detect_video \
        --video /path/to/input.mp4 \
        --weights yolo_model/weights/highlight_yolo_v1.1.0.pt \
        --output-json yolo_model/runs/detect_video/results.json \
        --output-csv yolo_model/runs/detect_video/results.csv \
        --device 0

JSON output contains one row per frame where at least one detection is found,
with pixel-space ``x, y, w, h`` and ``confidence``.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Any

import cv2

LOG = logging.getLogger("highlight.detect_video")


def _best_box(result: Any) -> tuple[list[float], float] | None:
    boxes = getattr(result, "boxes", None)
    if boxes is None or len(boxes) == 0:
        return None

    conf_t = getattr(boxes, "conf", None)
    xyxy_t = getattr(boxes, "xyxy", None)
    if conf_t is None or xyxy_t is None:
        return None

    conf = conf_t.detach().cpu().numpy().reshape(-1)
    xyxy = xyxy_t.detach().cpu().numpy().reshape(-1, 4)
    if conf.size == 0:
        return None

    best_idx = int(conf.argmax())
    return xyxy[best_idx].tolist(), float(conf[best_idx])


def _rows_to_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "frame_index",
                "timestamp_sec",
                "x",
                "y",
                "w",
                "h",
                "confidence",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--video", type=Path, required=True, help="Input video path.")
    parser.add_argument("--weights", type=Path, required=True, help="Path to trained YOLO .pt weights.")
    parser.add_argument("--output-json", type=Path, required=True, help="Where to write JSON detections.")
    parser.add_argument("--output-csv", type=Path, default=None, help="Optional CSV export path.")
    parser.add_argument("--save-annotated-video", type=Path, default=None, help="Optional output video with drawn detections.")
    parser.add_argument("--conf", type=float, default=0.25, help="YOLO confidence threshold.")
    parser.add_argument("--iou", type=float, default=0.5, help="YOLO NMS IoU threshold.")
    parser.add_argument("--imgsz", type=int, default=640, help="Inference image size.")
    parser.add_argument("--device", default="", help="YOLO device string, e.g. '0', 'cpu'. Empty = auto.")
    parser.add_argument("--frame-stride", type=int, default=1, help="Run inference every Nth frame.")
    parser.add_argument("--max-frames", type=int, default=0, help="Optional cap on processed frames (0 = no cap).")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.video.is_file():
        LOG.error("Video not found: %s", args.video)
        return 2
    if not args.weights.is_file():
        LOG.error("Weights not found: %s", args.weights)
        return 2

    try:
        from ultralytics import YOLO
    except ImportError:
        LOG.error("ultralytics is not installed. Run: pip install ultralytics")
        return 2

    model = YOLO(str(args.weights))

    cap = cv2.VideoCapture(str(args.video))
    if not cap.isOpened():
        LOG.error("Failed to open video: %s", args.video)
        return 2

    fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
    if fps <= 0:
        fps = 30.0
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)

    writer = None
    if args.save_annotated_video is not None:
        args.save_annotated_video.parent.mkdir(parents=True, exist_ok=True)
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(str(args.save_annotated_video), fourcc, fps, (width, height))
        if not writer.isOpened():
            LOG.error("Failed to open output writer: %s", args.save_annotated_video)
            cap.release()
            return 2

    detections: list[dict[str, Any]] = []
    frame_index = 0
    processed = 0
    stride = max(1, int(args.frame_stride))
    max_frames = max(0, int(args.max_frames))

    LOG.info(
        "Starting detection: video=%s frames=%s fps=%.2f size=%dx%d stride=%d",
        args.video,
        total_frames if total_frames > 0 else "unknown",
        fps,
        width,
        height,
        stride,
    )

    while True:
        ok, frame = cap.read()
        if not ok:
            break

        if frame_index % stride != 0:
            if writer is not None:
                writer.write(frame)
            frame_index += 1
            continue

        if max_frames and processed >= max_frames:
            break

        result = model.predict(
            source=frame,
            conf=args.conf,
            iou=args.iou,
            imgsz=args.imgsz,
            device=args.device or None,
            verbose=False,
        )[0]

        best = _best_box(result)
        if best is not None:
            (x1, y1, x2, y2), conf = best
            x1_i, y1_i = int(round(x1)), int(round(y1))
            x2_i, y2_i = int(round(x2)), int(round(y2))
            row = {
                "frame_index": frame_index,
                "timestamp_sec": round(frame_index / fps, 3),
                "x": x1_i,
                "y": y1_i,
                "w": max(0, x2_i - x1_i),
                "h": max(0, y2_i - y1_i),
                "confidence": round(conf, 6),
            }
            detections.append(row)

            if writer is not None:
                cv2.rectangle(frame, (x1_i, y1_i), (x2_i, y2_i), (0, 0, 255), 2)
                cv2.putText(
                    frame,
                    f"{conf:.2f}",
                    (x1_i, max(20, y1_i - 8)),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.6,
                    (0, 0, 255),
                    2,
                    cv2.LINE_AA,
                )

        if writer is not None:
            writer.write(frame)

        processed += 1
        frame_index += 1

    cap.release()
    if writer is not None:
        writer.release()

    payload = {
        "video": str(args.video),
        "weights": str(args.weights),
        "fps": fps,
        "frame_width": width,
        "frame_height": height,
        "total_frames": total_frames,
        "processed_frames": processed,
        "detections_found": len(detections),
        "detections": detections,
    }
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOG.info("Wrote JSON detections: %s", args.output_json)

    if args.output_csv is not None:
        _rows_to_csv(args.output_csv, detections)
        LOG.info("Wrote CSV detections: %s", args.output_csv)

    if args.save_annotated_video is not None:
        LOG.info("Wrote annotated video: %s", args.save_annotated_video)

    LOG.info("Done. Processed=%d, detections=%d", processed, len(detections))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
