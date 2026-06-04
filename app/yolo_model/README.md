# yolo_model

Highlight-overlay YOLO training, pose pipeline, and posture feedback for the coaching stack.

## Layout

```
yolo_model/
  config/           paths.py, posture_guidelines.yaml
  datasets/         versioned Roboflow / YOLO exports
  artifacts/        git-ignored train outputs + pose JSON
  exports/          staged .pt before agents/feedback/models/
  pipeline/         runtime: download video → pose JSON
  pose_feedback/    posture KB evaluation on pose JSON
  scripts/          train, evaluate, detect CLIs
  fixtures/         pipeline gate (G2) manifests
  data/             manifest.txt for frame sampling
  docs/             training, pipeline, production guides
```

## Runtime (imported in production)

- `pipeline.run_pose_pipeline` — worker / feedback agent
- `pose_feedback` + `config/posture_guidelines.yaml`
- `scripts/detect_pose_video_yolov8.py` (subprocess)

Deployed highlight weights: `agents/feedback/models/highlight_yolo_v1.pt` (`VIDEO_HIGHLIGHT_YOLO_WEIGHTS`).

## Quick start

1. `docs/data_strategy.md` — labeling rules
2. `docs/training.md` — train on `datasets/athlete_focus/v1.1.0/data.yaml`
3. `docs/production.md` — promote `artifacts/train/.../best.pt` → `exports/` → agent models
4. `docs/pipeline.md` — pose env vars (`YOLO_HIGHLIGHT_WEIGHTS`, `POSE_PIPELINE_OUTPUT_DIR`)

## Train (from repo root)

```bash
python -m yolo_model.scripts.train \
  --data yolo_model/datasets/athlete_focus/v1.1.0/data.yaml \
  --name highlight_v1.1.0
```
