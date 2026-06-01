# Pose coaching pipeline

End-to-end flow used by the platform worker and feedback agent:

1. **Sheet / admin** — row with `video_url`, player name, `player_key`.
2. **`FEEDBACK_DELEGATE` job** (or `VIDEO_PROCESSING` + chain) — worker downloads video, runs `detect_pose_video_yolov8`, writes `runs/pose/job_<id>/pose_results.json`.
3. **One event per red-circle span** — `yolo_model/pose_feedback` groups frames and evaluates `posture_guidelines.yaml` (body keypoints only).
4. **Feedback agent** — `build_review_from_pose_json`: per event, vision + optional Pinecone/shared context → markers + overall review.

## RunPod (GPU worker)

```bash
pip install -r yolo_model/requirements.txt
# CPU torch first if needed; on GPU use CUDA torch from pytorch.org

export YOLO_HIGHLIGHT_WEIGHTS=/workspace/agentic/runs/detect/yolo_model/runs/athlete_focus_v1_1_0/weights/best.pt
export YOLO_POSE_DEVICE=cuda
export POSE_PIPELINE_OUTPUT_DIR=/workspace/agentic/runs/pose
export FEEDBACK_USE_POSE_PIPELINE=true
export FEEDBACK_AGENT_BASE_URL=http://127.0.0.1:5055
```

## Manual CLI

```bash
PYTHONPATH=. python -m yolo_model.scripts.detect_pose_video_yolov8 \
  --video match.mp4 \
  --weights "$YOLO_HIGHLIGHT_WEIGHTS" \
  --output-json runs/pose/test.json \
  --device cuda
```
