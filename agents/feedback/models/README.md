# Runtime YOLO weights

This is the **runtime location** the deployed feedback agent reads weights from. The detector at `agents/feedback/highlight/yolo_detector.py` looks up `VIDEO_HIGHLIGHT_YOLO_WEIGHTS` (default: `agents/feedback/models/highlight_yolo_v1.pt`).

## How to populate

All training, evaluation, and versioning lives in the top-level `yolo_model/` folder. After producing a trained `best.pt` there, promote it to runtime:

```bash
# Stage in yolo_model/weights/ first (see yolo_model/TRAINING_GUIDE.md):
cp ./runs/highlight/v1.0.0/weights/best.pt \
   yolo_model/weights/highlight_yolo_v1.0.0.pt

# Promote to runtime (this directory):
cp yolo_model/weights/highlight_yolo_v1.0.0.pt \
   agents/feedback/models/highlight_yolo_v1.pt

# Or — preferred for easy rollback — use a symlink:
ln -sf highlight_yolo_v1.0.0.pt agents/feedback/models/highlight_yolo_v1.pt
```

The Dockerfile (`agents/feedback/Dockerfile`) copies the entire `agents/` tree at build time, so whatever sits here ships with the image automatically.

## If the file is missing

The agent quietly falls back to the legacy HSV detector and logs a one-line warning. Setting `VIDEO_HIGHLIGHT_DETECTOR` to anything other than `yolo` also bypasses this directory entirely.

## See also

- `yolo_model/README.md` — training overview.
- `yolo_model/TRAINING_GUIDE.md` — exact training commands.
- `yolo_model/PRODUCTION_PLAYBOOK.md` — versioning, rollout, rollback procedure.
