# Pose backend choice: MediaPipe vs YOLOv8-Pose

Decision record for the auto-feedback agent's pose-estimation backend.

## TL;DR

- **How much code change?** Moderate, not a rewrite. ~80–120 lines of edits in the same script. Same overall structure (load model → loop frames → write JSON + MP4).
- **Do we need to train the pose model?** **No.** Pre-trained YOLOv8x-Pose (or MediaPipe Heavy) is enough for a generic athlete in a soccer match. You'd only retrain if you start seeing systematic failures on poses that COCO-trained models can't handle (e.g., highly stylised sport-specific poses).

## What "switching to YOLOv8-Pose" actually involves

There are two ways to wire it in. **Approach B is recommended.**

### Approach A — drop-in replacement on the crop (simpler, less robust)

Replace MediaPipe's `pose.process(rgb_crop)` with `pose_model(crop)`. Same crop, same flow, just a different pose backend.

- **Pros**: ~30 LOC change. Crop pad and masking still applicable.
- **Cons**: YOLOv8-Pose performs better on whole-frame images than on small re-cropped patches; you lose some of its accuracy advantage.

### Approach B — two-pass, full-frame pose (recommended)

Run two models on the full frame:

1. **Your custom highlight YOLO** → bounding box of the red-circled player.
2. **YOLOv8-Pose** (off-the-shelf, COCO-trained) → all people + 17 keypoints across the whole frame.
3. Pick the pose detection whose bbox has the highest IoU with the highlight bbox.

```
┌────────────┐   highlight bbox (1 per frame)
│  Frame N   │ ─────────────────────────────┐
│ 1920x1080  │                              │
└────┬───────┘                              ▼
     │                              ┌─────────────────┐
     │                              │ IoU match: pick │
     │     all-people poses         │ pose whose bbox │  → final keypoints
     └──→ ┌────────────────┐ ─────→ │ overlaps the    │
          │ YOLOv8x-Pose   │        │ highlight bbox  │
          │ (17 keypoints) │        └─────────────────┘
          └────────────────┘
```

- **Pros**: YOLOv8-Pose runs at full resolution where it's most accurate. No ghost-on-red-ring problem (it's a *learned* person detector — won't anchor on the ring even if it's bright red). Better recall on action poses.
- **Cons**: A bit more code (~80–120 LOC vs. ~30). Loads two models. ~2× the inference per frame.

## Changes you'll see in the script

1. Add an `ultralytics` YOLO loader for the pose model:

```python
pose_model = YOLO(args.pose_weights)   # e.g. yolov8x-pose.pt
```

2. Per frame, after the highlight detection:

```python
pose_result = pose_model(frame, conf=0.25, device=args.device, verbose=False)[0]
# pose_result.boxes:     person bboxes
# pose_result.keypoints: per-person keypoints (N x 17 x 3 [x, y, visibility])
```

3. IoU match: find the person bbox most overlapping with the highlight bbox, take that row of keypoints.

4. Output schema changes from **33 MediaPipe landmarks** to **17 COCO keypoints**:

```
COCO 17 keypoints:
0  nose            5  left_shoulder    11 left_hip      14 right_knee
1  left_eye        6  right_shoulder   12 right_hip     15 left_ankle
2  right_eye       7  left_elbow       13 left_knee     16 right_ankle
3  left_ear        8  right_elbow
4  right_ear       9  left_wrist
                  10  right_wrist
```

This is the more common industry standard for sports analysis; most coaching KBs you'd build on top of this use the COCO joint set.

5. No more red-overlay masking needed (delete that helper). The ghost-pose-on-red-ring problem disappears.

## Why YOLOv8-Pose will help on the current footage

From the spot-check screenshots:

- **ID:13 (standing, slight back-facing)** — MediaPipe gave low visibility because it's slightly back-turned. YOLOv8-Pose handles back-facing humans far better; COCO has plenty of training examples for it.
- **ID:8 (running away from camera, mid-stride)** — Same story. Action / motion-blurred poses are where COCO-trained pose models pull ahead.
- **ID:3 (overhead celebration pose)** — Both struggle, but YOLOv8-Pose is more robust to non-canonical poses.
- **ID:None 0.26 (red-jacket fan)** — Already filtered by `--conf 0.50`. No change.
- **Ghost skeleton on the red ring** — Goes away entirely. YOLOv8-Pose only emits keypoints when it actually detects a *person*.

Realistic expectation: pose coverage moves from ~54% (or ~50% after the visibility filter) to **~85–95%** of the 90 valid YOLO detections.

## Do you need to train the pose model? (longer answer)

**Almost certainly not.** Pre-trained pose models cover this case because:

- The "custom" part of the problem is **detecting the red-circle highlight** — a synthetic overlay, so a custom model was needed. Generic pose models can't know what the highlight looks like.
- The "generic" part is **estimating a human's joint positions** — a well-solved problem with millions of training images. Athletes in shorts/jerseys on a soccer pitch are exactly in the training distribution of COCO/MPII.

Train/finetune a pose model **only** if, after trying YOLOv8x-Pose, you see a systematic failure mode like:

- Joints behind padding/equipment never get detected (e.g., American football).
- Specific limb configurations always wrong (e.g., gymnastics inversion).
- Kid/child athletes with non-adult proportions.

For 11-on-11 soccer with adult-sized athletes in clear daylight, no training needed.

## Recommendation

Two reasonable paths forward, depending on how close to building the coaching agent:

### Path 1 — Stick with MediaPipe + the v2 patches (fastest to "good enough")

Run the v2 script. If `pose_quality: ok` lands at ~40–50 of the 90 valid detections, that's **enough to start building the coaching KB / feedback agent** on 33-landmark MediaPipe data. The schema is stable; the downstream design (joint angles → KB matching) is identical regardless of which pose backend you used.

Backend can always be swapped later. The downstream coaching agent only consumes the JSON.

### Path 2 — Switch to YOLOv8x-Pose now (better quality, before downstream code locks in)

Worth it if:

- No code yet depends on the 33-landmark MediaPipe schema.
- The standard 17-keypoint COCO schema is desired (more common in sports-analytics literature; more KB examples online).
- Coverage matters more than landmark granularity.

The switch is **best done now**, before feedback rules get built on top of MediaPipe's 33 landmarks. Doing it later means rewriting the rules too.

### Honest pick

If there's ~30 min for the code edit and the coaching agent isn't started yet, **go with YOLOv8-Pose now**. Cleaner data, fewer ghosts, smaller schema, better sports-literature alignment.

If already mid-flight on coaching code or it's a demo-tomorrow situation, **stay on MediaPipe v2** — it's enough.

## Next action

Optionally produce `yolo_model/scripts/detect_pose_video_v2.py` (YOLOv8-Pose version) as a separate script so the two can be A/B compared before committing.
