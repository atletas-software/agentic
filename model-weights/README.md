# YOLO model weights (production)

The feedback-agent container needs a **highlight** weights file at:

```text
model-weights/highlight_yolo_v1.pt
```

This path is mounted read-only into the container as `/run/models/highlight_yolo_v1.pt`.

## Source weights in this repo (committed to git)

Trained highlight weights are versioned at:

```text
app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt
```

After `git pull` on the VM, rebuild **feedback-agent** so the image bakes this file to
`/app/agents/feedback/models/highlight_yolo_v1.pt`.

## Setup on the VM (if not baked into the image)

1. Copy `best.pt` from your laptop (repo path above) **or** promote to `model-weights/`:

```bash
cd /var/www/html/agentic
mkdir -p model-weights
# From your Mac (replace VM host):
# scp app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt \
#   root@YOUR_VM:/var/www/html/agentic/model-weights/highlight_yolo_v1.pt
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
