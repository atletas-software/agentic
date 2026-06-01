"""Highlight YOLO + YOLOv8-Pose pipeline for highlighted-player feedback.

Two-pass design (see .cursor/plans/pose_backend_choice.plan.md):

1. Custom highlight YOLO + ByteTrack on the full frame -> top-1 bbox of the
   red-circled player.
2. YOLOv8-Pose (off-the-shelf, COCO-trained) on the full frame -> bboxes
   and 17 keypoints for every person.
3. IoU match: pick the pose detection whose bbox most overlaps the
   highlight bbox. That is the highlighted player's pose.

Why this beats the MediaPipe approach for sports footage:

- No "ghost" pose fitted to the red ring (YOLOv8-Pose is a learned person
  detector; it won't anchor on a high-contrast circle).
- Better recall on action poses (running, mid-jump, back-facing) because
  COCO has plenty of such examples in training.
- Runs the pose model at full resolution, not on a small crop.

Output schema:

- 17 COCO keypoints per matched frame (instead of MediaPipe's 33 landmarks).
- Each keypoint has `id`, `name`, `x`, `y`, `visibility`.

Run:

python -m yolo_model.scripts.detect_pose_video_yolov8 \\
    --video /workspace/agentic/match.mp4 \\
    --weights /workspace/agentic/runs/detect/yolo_model/runs/athlete_focus_v1_1_0/weights/best.pt \\
    --pose-weights yolov8x-pose.pt \\
    --output-json /workspace/agentic/yolo_model/runs/pose/results_yolov8.json \\
    --output-video /workspace/agentic/yolo_model/runs/pose/pose_yolov8.mp4 \\
    --device cpu
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

import cv2
import numpy as np

LOG = logging.getLogger("highlight.pose_video_yolov8")


SCHEMA_VERSION = 3


COCO_KEYPOINT_NAMES = (
    "nose",
    "left_eye",
    "right_eye",
    "left_ear",
    "right_ear",
    "left_shoulder",
    "right_shoulder",
    "left_elbow",
    "right_elbow",
    "left_wrist",
    "right_wrist",
    "left_hip",
    "right_hip",
    "left_knee",
    "right_knee",
    "left_ankle",
    "right_ankle",
)


# COCO skeleton edges for visualisation (pairs of keypoint indices).
COCO_SKELETON_EDGES = (
    (0, 1), (0, 2), (1, 3), (2, 4),                    # face
    (5, 7), (7, 9), (6, 8), (8, 10),                   # arms
    (5, 6), (5, 11), (6, 12), (11, 12),                # torso
    (11, 13), (13, 15), (12, 14), (14, 16),            # legs
)


def _iou(box_a: tuple[float, float, float, float],
         box_b: tuple[float, float, float, float]) -> float:
    """IoU for two (x1, y1, x2, y2) boxes."""

    ax1, ay1, ax2, ay2 = box_a
    bx1, by1, bx2, by2 = box_b

    ix1 = max(ax1, bx1)
    iy1 = max(ay1, by1)
    ix2 = min(ax2, bx2)
    iy2 = min(ay2, by2)

    iw = max(0.0, ix2 - ix1)
    ih = max(0.0, iy2 - iy1)
    inter = iw * ih

    area_a = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    area_b = max(0.0, bx2 - bx1) * max(0.0, by2 - by1)

    union = area_a + area_b - inter
    return float(inter / union) if union > 0 else 0.0


def _containment(inner: tuple[float, float, float, float],
                 outer: tuple[float, float, float, float]) -> float:
    """Fraction of `inner`'s area that overlaps `outer` (diagnostic only).

    Returns 1.0 if `inner` is entirely inside `outer`, 0.0 if disjoint.
    """

    ax1, ay1, ax2, ay2 = inner
    bx1, by1, bx2, by2 = outer

    ix1 = max(ax1, bx1)
    iy1 = max(ay1, by1)
    ix2 = min(ax2, bx2)
    iy2 = min(ay2, by2)

    iw = max(0.0, ix2 - ix1)
    ih = max(0.0, iy2 - iy1)
    inter = iw * ih

    area_inner = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    return float(inter / area_inner) if area_inner > 0 else 0.0


def _bbox_center(b: tuple[float, float, float, float]) -> tuple[float, float]:
    return ((b[0] + b[2]) / 2.0, (b[1] + b[3]) / 2.0)


def _pad_bbox(
    x1: int,
    y1: int,
    x2: int,
    y2: int,
    pad: float,
    width: int,
    height: int,
) -> tuple[int, int, int, int]:
    """Expand a bbox by `pad` (fraction of bbox size), clamped to frame."""

    bw = x2 - x1
    bh = y2 - y1

    pad_x = int(bw * pad)
    pad_y = int(bh * pad)

    x1_p = max(0, x1 - pad_x)
    y1_p = max(0, y1 - pad_y)
    x2_p = min(width, x2 + pad_x)
    y2_p = min(height, y2 + pad_y)

    return x1_p, y1_p, x2_p, y2_p


def _point_in_bbox(p: tuple[float, float],
                   b: tuple[float, float, float, float]) -> bool:
    return b[0] <= p[0] <= b[2] and b[1] <= p[1] <= b[3]


def _center_offset_score(highlight: tuple[float, float, float, float],
                         pose: tuple[float, float, float, float]) -> float:
    """How well-centered the highlight center is inside the pose bbox.

    Returns a score in [0, 1] where 1.0 = highlight center is exactly at
    the pose-bbox center, falling off linearly to 0 at the pose-bbox edge.
    Returns 0 if the highlight center lies outside the pose bbox at all.

    This is the right matching primitive for our pipeline because the
    highlight ring is drawn ON the player (its center sits inside the
    player's body), but the ring bbox and the body bbox usually do NOT
    overlap much in IoU/containment terms.
    """
    hcx, hcy = _bbox_center(highlight)
    if not _point_in_bbox((hcx, hcy), pose):
        return 0.0

    pcx, pcy = _bbox_center(pose)
    pose_w = max(1e-6, pose[2] - pose[0])
    pose_h = max(1e-6, pose[3] - pose[1])

    nx = abs(pcx - hcx) / (pose_w / 2.0)
    ny = abs(pcy - hcy) / (pose_h / 2.0)

    return float(max(0.0, 1.0 - max(nx, ny)))


def _draw_skeleton(
    frame,
    kp_xy,
    kp_conf,
    visibility_threshold: float,
) -> None:
    """Draw COCO skeleton edges + keypoint circles on the frame in place."""

    for a, b in COCO_SKELETON_EDGES:
        if kp_conf[a] < visibility_threshold or kp_conf[b] < visibility_threshold:
            continue
        pa = (int(kp_xy[a][0]), int(kp_xy[a][1]))
        pb = (int(kp_xy[b][0]), int(kp_xy[b][1]))
        cv2.line(frame, pa, pb, (0, 255, 255), 2, cv2.LINE_AA)

    for i in range(len(kp_xy)):
        if kp_conf[i] < visibility_threshold:
            continue
        x = int(kp_xy[i][0])
        y = int(kp_xy[i][1])
        cv2.circle(frame, (x, y), 4, (0, 255, 0), -1, cv2.LINE_AA)


def main(argv: list[str] | None = None) -> int:

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--video", type=Path, required=True)
    parser.add_argument(
        "--weights",
        type=Path,
        required=True,
        help="Custom YOLO weights for the red-circle highlight detector",
    )

    parser.add_argument(
        "--pose-weights",
        type=str,
        default="yolov8x-pose.pt",
        help=(
            "YOLOv8-Pose weights. Defaults to 'yolov8x-pose.pt' which "
            "Ultralytics will auto-download on first use. Use a smaller "
            "variant (yolov8n-pose.pt / yolov8s-pose.pt) for faster CPU runs."
        ),
    )

    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-video", type=Path, default=None)

    parser.add_argument(
        "--conf",
        type=float,
        default=0.50,
        help="Highlight YOLO confidence threshold",
    )
    parser.add_argument(
        "--pose-conf",
        type=float,
        default=0.10,
        help=(
            "YOLOv8-Pose person confidence threshold. Default 0.10 (low) "
            "because the highlighted player can be small/distant in the "
            "wide-shot frame and the default 0.25 misses them."
        ),
    )
    parser.add_argument("--iou", type=float, default=0.5)

    parser.add_argument(
        "--imgsz",
        type=int,
        default=640,
        help="Inference size for the highlight YOLO (fast; red ring is easy at any scale)",
    )

    parser.add_argument(
        "--pose-imgsz",
        type=int,
        default=640,
        help=(
            "Inference size for YOLOv8-Pose. Since we now run pose on the "
            "padded crop (not the full frame), the crop is small enough "
            "that 640 is plenty - the player fills most of the inference "
            "image after Ultralytics' internal resize."
        ),
    )

    parser.add_argument(
        "--pose-pad",
        type=float,
        default=0.30,
        help=(
            "Fraction to pad the highlight bbox before cropping for pose. "
            "Generous padding gives the pose model context for limbs that "
            "extend beyond the ring (raised arms, kicking legs). Default "
            "0.30 (30%%) works well; raise to 0.50+ if you see clipped "
            "extremities, lower if neighbouring players keep getting "
            "picked up."
        ),
    )

    parser.add_argument("--device", default="cpu", help="cpu or cuda device id")

    parser.add_argument("--frame-stride", type=int, default=1)
    parser.add_argument("--max-frames", type=int, default=0)

    parser.add_argument(
        "--min-pose-visibility",
        type=float,
        default=0.40,
        help=(
            "Reject the pose if the mean visibility across the 17 keypoints "
            "is below this threshold. YOLOv8-Pose visibility scores are "
            "usually higher than MediaPipe's, so 0.40 is a sensible floor."
        ),
    )

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
        LOG.error("Highlight weights not found: %s", args.weights)
        return 2

    try:
        from ultralytics import YOLO

    except ImportError:
        LOG.error(
            "ultralytics is not installed. Run: pip install ultralytics"
        )
        return 2

    LOG.info("Loading highlight YOLO model: %s", args.weights)
    highlight_model = YOLO(str(args.weights))

    LOG.info(
        "Loading YOLOv8-Pose model: %s (auto-downloads if not local)",
        args.pose_weights,
    )
    pose_model = YOLO(args.pose_weights)

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

    if args.output_video is not None:

        args.output_video.parent.mkdir(parents=True, exist_ok=True)

        fourcc = cv2.VideoWriter_fourcc(*"mp4v")

        writer = cv2.VideoWriter(
            str(args.output_video),
            fourcc,
            fps,
            (width, height),
        )

        if not writer.isOpened():
            LOG.error(
                "Failed to open output writer: %s",
                args.output_video,
            )
            cap.release()
            return 2

    pose_results: list[dict[str, Any]] = []

    frame_index = 0
    processed = 0
    frames_with_pose = 0

    stride = max(1, int(args.frame_stride))
    max_frames = max(0, int(args.max_frames))

    LOG.info(
        "Starting pose pipeline: video=%s frames=%s fps=%.2f size=%dx%d "
        "stride=%d pose=%s pose_imgsz=%d pose_pad=%.2f pose_conf=%.2f "
        "match=crop+top1+center-in-bbox",
        args.video,
        total_frames if total_frames > 0 else "unknown",
        fps,
        width,
        height,
        stride,
        args.pose_weights,
        args.pose_imgsz,
        args.pose_pad,
        args.pose_conf,
    )

    try:
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

            # =========================
            # Pass 1: highlight YOLO + ByteTrack
            # =========================

            hl_result = highlight_model.track(
                source=frame,
                persist=True,
                conf=args.conf,
                iou=args.iou,
                imgsz=args.imgsz,
                device=args.device or None,
                verbose=False,
                tracker="bytetrack.yaml",
            )[0]

            hl_boxes = getattr(hl_result, "boxes", None)
            has_box = hl_boxes is not None and len(hl_boxes) > 0

            detected = False
            track_id: int | None = None
            conf_val: float = 0.0
            hl_bbox = (0.0, 0.0, 0.0, 0.0)

            if has_box:

                xyxy_list = hl_boxes.xyxy.detach().cpu().numpy()
                conf_list = hl_boxes.conf.detach().cpu().numpy()

                track_ids = (
                    hl_boxes.id.int().cpu().tolist()
                    if hl_boxes.id is not None
                    else [None] * len(xyxy_list)
                )

                best = int(conf_list.argmax())
                xyxy = xyxy_list[best]
                conf_val = float(conf_list[best])
                track_id = track_ids[best]

                x1_hl = max(0.0, float(xyxy[0]))
                y1_hl = max(0.0, float(xyxy[1]))
                x2_hl = min(float(width), float(xyxy[2]))
                y2_hl = min(float(height), float(xyxy[3]))

                if x2_hl > x1_hl and y2_hl > y1_hl:
                    detected = True
                    hl_bbox = (x1_hl, y1_hl, x2_hl, y2_hl)

            # =========================
            # Pass 2: YOLOv8-Pose on full frame (only when needed)
            # =========================

            keypoints_payload: list[dict[str, Any]] | None = None
            pose_quality = "no_detection"
            pose_visibility_mean: float | None = None
            pose_bbox_payload: dict[str, int] | None = None
            pose_conf_val: float | None = None
            match_iou_val: float | None = None
            match_containment_val: float | None = None
            n_people_detected: int | None = None

            if detected:

                # =========================
                # Crop the frame around the highlight + padding,
                # then run YOLOv8-Pose ONLY on that crop. Full-frame pose
                # loses recall here because the player is small relative to
                # the 1920x1080 frame; the crop gets upscaled to pose_imgsz
                # internally, so the player ends up huge in the inference
                # image - much higher recall on small/action poses.
                # =========================

                x1_i = int(round(hl_bbox[0]))
                y1_i = int(round(hl_bbox[1]))
                x2_i = int(round(hl_bbox[2]))
                y2_i = int(round(hl_bbox[3]))

                x1_p, y1_p, x2_p, y2_p = _pad_bbox(
                    x1_i, y1_i, x2_i, y2_i,
                    args.pose_pad,
                    width, height,
                )

                crop = frame[y1_p:y2_p, x1_p:x2_p]

                if crop.size == 0:
                    pose_quality = "no_pose"

                else:
                    pose_result = pose_model(
                        crop,
                        conf=args.pose_conf,
                        iou=args.iou,
                        imgsz=args.pose_imgsz,
                        device=args.device or None,
                        verbose=False,
                    )[0]

                    p_boxes = getattr(pose_result, "boxes", None)
                    p_keypts = getattr(pose_result, "keypoints", None)

                    n_people = (
                        int(p_boxes.shape[0])
                        if p_boxes is not None and p_boxes.shape[0] > 0
                        else 0
                    )
                    n_people_detected = n_people

                    if n_people == 0 or p_keypts is None:
                        pose_quality = "no_pose"

                    else:
                        # Coordinates are in CROP space; map back to frame.
                        p_xyxy = p_boxes.xyxy.detach().cpu().numpy()
                        p_conf = p_boxes.conf.detach().cpu().numpy()
                        kp_xy = p_keypts.xy.detach().cpu().numpy()
                        kp_cf = p_keypts.conf.detach().cpu().numpy()

                        # Since the crop is tight around the highlighted
                        # player, pick the single highest-confidence person.
                        # If a neighbouring player accidentally enters the
                        # crop, the center-in-highlight-bbox sanity check
                        # below filters it out.
                        best_idx = int(p_conf.argmax())

                        # Map the best pose bbox to global frame coords
                        pose_box_global = (
                            x1_p + float(p_xyxy[best_idx][0]),
                            y1_p + float(p_xyxy[best_idx][1]),
                            x1_p + float(p_xyxy[best_idx][2]),
                            y1_p + float(p_xyxy[best_idx][3]),
                        )

                        # Sanity-check: highlight centre should lie inside
                        # the chosen pose bbox (rejects neighbouring players)
                        center_score = _center_offset_score(
                            hl_bbox, pose_box_global
                        )

                        match_iou_val = _iou(hl_bbox, pose_box_global)
                        match_containment_val = _containment(
                            pose_box_global, hl_bbox
                        )

                        if center_score <= 0.0:
                            pose_quality = "no_match"

                        else:
                            pose_conf_val = float(p_conf[best_idx])

                            pose_bbox_payload = {
                                "x1": int(round(pose_box_global[0])),
                                "y1": int(round(pose_box_global[1])),
                                "x2": int(round(pose_box_global[2])),
                                "y2": int(round(pose_box_global[3])),
                            }

                            # Map keypoints to global frame coords
                            kp_xy_best = kp_xy[best_idx]
                            kp_cf_best = kp_cf[best_idx]

                            kp_xy_global = [
                                (
                                    x1_p + float(kp_xy_best[i][0]),
                                    y1_p + float(kp_xy_best[i][1]),
                                )
                                for i in range(len(kp_xy_best))
                            ]

                            visibilities = [float(c) for c in kp_cf_best]
                            pose_visibility_mean = (
                                sum(visibilities) / len(visibilities)
                                if visibilities
                                else 0.0
                            )

                            if pose_visibility_mean >= args.min_pose_visibility:

                                keypoints_payload = [
                                    {
                                        "id": i,
                                        "name": COCO_KEYPOINT_NAMES[i],
                                        "x": kp_xy_global[i][0],
                                        "y": kp_xy_global[i][1],
                                        "visibility": float(kp_cf_best[i]),
                                    }
                                    for i in range(len(kp_xy_global))
                                ]

                                pose_quality = "ok"
                                frames_with_pose += 1

                                if writer is not None:
                                    _kpg = np.array(kp_xy_global, dtype=np.float32)
                                    _draw_skeleton(
                                        frame,
                                        _kpg,
                                        kp_cf_best,
                                        args.min_pose_visibility,
                                    )

                            else:
                                pose_quality = "low_visibility"

                # Draw highlight bbox + label (always, when detected)
                if writer is not None:
                    cv2.rectangle(
                        frame,
                        (x1_i, y1_i),
                        (x2_i, y2_i),
                        (0, 0, 255),
                        2,
                    )

                    label = f"ID:{track_id} {conf_val:.2f}"
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

            # =========================
            # Record the frame
            # =========================

            if detected:
                pose_results.append(
                    {
                        "frame_index": frame_index,
                        "timestamp_sec": round(frame_index / fps, 3),
                        "detected": True,
                        "track_id": track_id,
                        "highlight_bbox": {
                            "x1": int(round(hl_bbox[0])),
                            "y1": int(round(hl_bbox[1])),
                            "x2": int(round(hl_bbox[2])),
                            "y2": int(round(hl_bbox[3])),
                        },
                        "highlight_confidence": round(conf_val, 6),
                        "pose_bbox": pose_bbox_payload,
                        "pose_confidence": (
                            round(pose_conf_val, 6)
                            if pose_conf_val is not None
                            else None
                        ),
                        "match_iou": (
                            round(match_iou_val, 4)
                            if match_iou_val is not None
                            else None
                        ),
                        "match_containment": (
                            round(match_containment_val, 4)
                            if match_containment_val is not None
                            else None
                        ),
                        "n_people_detected": n_people_detected,
                        "pose_quality": pose_quality,
                        "pose_visibility_mean": (
                            round(pose_visibility_mean, 3)
                            if pose_visibility_mean is not None
                            else None
                        ),
                        "keypoints": keypoints_payload,
                    }
                )

            else:
                pose_results.append(
                    {
                        "frame_index": frame_index,
                        "timestamp_sec": round(frame_index / fps, 3),
                        "detected": False,
                        "track_id": None,
                        "highlight_bbox": None,
                        "highlight_confidence": None,
                        "pose_bbox": None,
                        "pose_confidence": None,
                        "match_iou": None,
                        "match_containment": None,
                        "n_people_detected": None,
                        "pose_quality": "no_detection",
                        "pose_visibility_mean": None,
                        "keypoints": None,
                    }
                )

            if writer is not None:
                writer.write(frame)

            processed += 1

            if processed % 50 == 0:
                LOG.info(
                    "Processed frame %d / %s (frames_with_pose=%d)",
                    frame_index,
                    total_frames if total_frames > 0 else "?",
                    frames_with_pose,
                )

            frame_index += 1

    finally:
        cap.release()
        if writer is not None:
            writer.release()

    # =========================
    # Emit JSON
    # =========================

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "video": str(args.video),
        "highlight_weights": str(args.weights),
        "pose_weights": args.pose_weights,
        "fps": fps,
        "frame_width": width,
        "frame_height": height,
        "total_frames": total_frames,
        "processed_frames": processed,
        "frames_with_pose": frames_with_pose,
        "config": {
            "conf": args.conf,
            "pose_conf": args.pose_conf,
            "iou": args.iou,
            "match_metric": "crop+top1+center-in-bbox",
            "imgsz": args.imgsz,
            "pose_imgsz": args.pose_imgsz,
            "pose_pad": args.pose_pad,
            "frame_stride": stride,
            "max_frames": max_frames,
            "min_pose_visibility": args.min_pose_visibility,
        },
        "keypoint_names": list(COCO_KEYPOINT_NAMES),
        "notes": {
            "track_id": (
                "ByteTrack continuity marker, NOT player identity. Across "
                "this video the physical subject is the same red-circled "
                "player; a new track_id only means tracking restarted."
            ),
            "keypoints": (
                "COCO 17-keypoint format. Each entry has id (0-16), name, "
                "x/y in full-frame pixel coordinates, and visibility "
                "(0-1 confidence from YOLOv8-Pose)."
            ),
            "match_iou": (
                "IoU between highlight and chosen pose bbox. Recorded for "
                "debugging only; matching uses centre-in-bbox."
            ),
            "match_containment": (
                "Fraction of the chosen pose bbox inside the highlight "
                "bbox. Recorded for debugging only."
            ),
            "match_metric": (
                "Pose matching: the chosen pose is the one whose body bbox "
                "contains the centre of the highlight ring (with tiebreak "
                "on most-centred). Empirically robust because the ring is "
                "drawn AT the player but its bbox rarely overlaps the "
                "player's tall body bbox."
            ),
        },
        "pose_results": pose_results,
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)

    args.output_json.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    LOG.info("Wrote JSON: %s", args.output_json)

    if args.output_video is not None:
        LOG.info("Wrote annotated video: %s", args.output_video)

    LOG.info(
        "Done. processed=%d frames_with_pose=%d",
        processed,
        frames_with_pose,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
