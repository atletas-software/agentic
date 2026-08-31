# YOLO model weights (local Docker + production)

Api and worker containers read highlight weights from:

```text
/app/app/agents/feedback/models/highlight_yolo_v1.pt
```

(`docker-compose.yml` bind-mounts your checkout's `best.pt` to that path.)

## Setup before `docker compose up`

Use **one** of these on the host:

### Option A — weights in the repo (recommended after training)

```text
app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt
```

### Option B — copy to model-weights (if best.pt is not in git)

```bash
mkdir -p model-weights
cp app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt \
   model-weights/highlight_yolo_v1.pt
```

Then add this bind mount to your compose override (or symlink best.pt into the artifacts path above).

## Verify after start

```bash
curl -s http://127.0.0.1:8000/feedback-agent/health | python3 -m json.tool
```

Expect `"highlight_weights_ok": true` and `"ready_for_pose_pipeline": true`.

## Pose model

`yolov8n-pose.pt` is downloaded automatically by Ultralytics on first run (needs outbound internet in the container).

For GPU locally: set `YOLO_POSE_DEVICE=cuda` in repo-root `.env` (requires NVIDIA Container Toolkit).

## Legacy standalone feedback-agent (:5055)

Only needed if `FEEDBACK_DELEGATE_HTTP=true`. Otherwise YOLO runs in the **worker** container:

```bash
docker compose --profile feedback-agent up -d   # optional legacy service
```
