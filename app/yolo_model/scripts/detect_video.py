"""Run highlight-overlay YOLO tracking on a video and export bbox/tracking results.

Features:
- YOLO detection
- ByteTrack tracking
- Stable player IDs
- Player center positions
- Annotated output video
- JSON + CSV export

Usage:

python -m yolo_model.scripts.detect_video \
    --video /workspace/agentic/match.mp4 \
    --weights /workspace/agentic/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt \
    --output-json /workspace/agentic/yolo_model/artifacts/detect_video/results.json \
    --output-csv /workspace/agentic/yolo_model/artifacts/detect_video/results.csv \
    --save-annotated-video /workspace/agentic/yolo_model/artifacts/detect_video/annotated.mp4 \
    --device cpu
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


def _rows_to_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as fh:

        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "frame_index",
                "timestamp_sec",
                "track_id",
                "x",
                "y",
                "w",
                "h",
                "center_x",
                "center_y",
                "confidence",
            ],
        )

        writer.writeheader()

        for row in rows:
            writer.writerow(row)


def main(argv: list[str] | None = None) -> int:

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--video", type=Path, required=True)
    parser.add_argument("--weights", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, default=None)
    parser.add_argument("--save-annotated-video", type=Path, default=None)

    parser.add_argument("--conf", type=float, default=0.25)
    parser.add_argument("--iou", type=float, default=0.5)
    parser.add_argument("--imgsz", type=int, default=640)

    parser.add_argument(
        "--device",
        default="cpu",
        help="cpu or cuda device id",
    )

    parser.add_argument("--frame-stride", type=int, default=1)
    parser.add_argument("--max-frames", type=int, default=0)

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
        LOG.error(
            "ultralytics is not installed. Run: pip install ultralytics"
        )
        return 2

    LOG.info("Loading YOLO model...")
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

        args.save_annotated_video.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        fourcc = cv2.VideoWriter_fourcc(*"mp4v")

        writer = cv2.VideoWriter(
            str(args.save_annotated_video),
            fourcc,
            fps,
            (width, height),
        )

        if not writer.isOpened():
            LOG.error(
                "Failed to open output writer: %s",
                args.save_annotated_video,
            )

            cap.release()
            return 2

    detections: list[dict[str, Any]] = []

    frame_index = 0
    processed = 0

    stride = max(1, int(args.frame_stride))
    max_frames = max(0, int(args.max_frames))

    LOG.info(
        "Starting tracking: video=%s frames=%s fps=%.2f size=%dx%d stride=%d",
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

        result = model.track(
            source=frame,
            persist=True,
            conf=args.conf,
            iou=args.iou,
            imgsz=args.imgsz,
            device=args.device or None,
            verbose=False,
            tracker="bytetrack.yaml",
        )[0]

        boxes = getattr(result, "boxes", None)

        if boxes is not None and len(boxes) > 0:

            xyxy_list = boxes.xyxy.detach().cpu().numpy()
            conf_list = boxes.conf.detach().cpu().numpy()

            track_ids = (
                boxes.id.int().cpu().tolist()
                if boxes.id is not None
                else [None] * len(xyxy_list)
            )

            for idx, (xyxy, conf) in enumerate(
                zip(xyxy_list, conf_list)
            ):

                x1, y1, x2, y2 = xyxy

                x1_i = int(round(x1))
                y1_i = int(round(y1))
                x2_i = int(round(x2))
                y2_i = int(round(y2))

                w = max(0, x2_i - x1_i)
                h = max(0, y2_i - y1_i)

                center_x = int((x1_i + x2_i) / 2)
                center_y = int((y1_i + y2_i) / 2)

                track_id = track_ids[idx]

                row = {
                    "frame_index": frame_index,
                    "timestamp_sec": round(frame_index / fps, 3),

                    "track_id": track_id,

                    "x": x1_i,
                    "y": y1_i,
                    "w": w,
                    "h": h,

                    "center_x": center_x,
                    "center_y": center_y,

                    "confidence": round(float(conf), 6),
                }

                detections.append(row)

                if writer is not None:

                    # bounding box
                    cv2.rectangle(
                        frame,
                        (x1_i, y1_i),
                        (x2_i, y2_i),
                        (0, 0, 255),
                        2,
                    )

                    # label
                    label = f"ID:{track_id} {conf:.2f}"

                    cv2.putText(
                        frame,
                        label,
                        (x1_i, max(20, y1_i - 10)),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.6,
                        (0, 0, 255),
                        2,
                        cv2.LINE_AA,
                    )

                    # center point
                    cv2.circle(
                        frame,
                        (center_x, center_y),
                        5,
                        (255, 0, 0),
                        -1,
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

    args.output_json.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    args.output_json.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    LOG.info("Wrote JSON detections: %s", args.output_json)

    if args.output_csv is not None:

        _rows_to_csv(
            args.output_csv,
            detections,
        )

        LOG.info("Wrote CSV detections: %s", args.output_csv)

    if args.save_annotated_video is not None:
        LOG.info(
            "Wrote annotated video: %s",
            args.save_annotated_video,
        )

    LOG.info(
        "Done. Processed=%d detections=%d",
        processed,
        len(detections),
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())