# YOLO model weights (production)

The feedback-agent container needs a **highlight** weights file at:

```text
model-weights/highlight_yolo_v1.pt
```

This path is mounted read-only into the container as `/run/models/highlight_yolo_v1.pt`.

## Setup on the VM

1. Copy your trained `best.pt` from training (or ask whoever ran `yolo_model/scripts/train.py`):

```bash
cd /var/www/html/agentic
mkdir -p model-weights
# from your laptop (example):
# scp best.pt user@VM:/var/www/html/agentic/model-weights/highlight_yolo_v1.pt
```

2. Verify the file exists:

```bash
ls -la model-weights/highlight_yolo_v1.pt
```

3. Redeploy feedback-agent (see repo README or deploy steps from your team).

4. Verify inside the container:

```bash
docker exec athlete-agent-feedback ls -la /run/models/
curl -s http://127.0.0.1:5055/health | python3 -m json.tool
```

`pose_pipeline_ready` must be `true` before video feedback will work.

## Pose model

`yolov8n-pose.pt` is downloaded automatically by Ultralytics on first run (needs outbound internet), unless you also place it in `model-weights/` and set `YOLO_POSE_WEIGHTS=/run/models/yolov8n-pose.pt`.
