# Pose API (RunPod / GPU)

Standalone YOLO highlight + pose service. The **feedback-agent** calls it when `POSE_API_BASE_URL` is set so the main VM does not run torch on CPU.

## API contract

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/pose/jobs` | Body: `{ "video_url": "...", "job_key": "optional" }` → `{ id, status, status_url, result_url }` |
| `GET` | `/pose/jobs/{id}/status` | `queued` \| `running` \| `completed` \| `failed` + `progress_detail` |
| `GET` | `/pose/jobs/{id}/result` | Full pose JSON (schema v3, same as `pose_results.json`) |
| `POST` | `/pose/jobs/{id}/cancel` | Cooperative cancel |
| `GET` | `/health` | Weights + `YOLO_POSE_DEVICE` |

---

## RunPod setup (no custom Docker image)

Use a **RunPod PyTorch GPU pod** (SSH or web terminal). You run Python directly in the pod — no `docker build`.

### 1. Create the pod

- Template: **RunPod PyTorch** (or any CUDA GPU pod)
- GPU: RTX 4090 / A40 / similar
- Disk: ≥ 30 GB (torch + video cache)
- **Expose HTTP port `5060`** in the pod settings (TCP port → RunPod proxy)

### 2. Clone and configure

```bash
cd /workspace
git clone https://github.com/YOUR_ORG/agentic.git
cd agentic

cp app/pose_api/.env.example app/pose_api/.env
# Edit paths if needed — defaults work under /workspace/agentic
```

`app/pose_api/.env` essentials:

```env
YOLO_POSE_DEVICE=cuda
YOLO_HIGHLIGHT_WEIGHTS=app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt
DATA_DIR=/workspace/agentic/data/pose_api
POSE_PIPELINE_OUTPUT_DIR=/workspace/agentic/data/pose
PORT=5060
# POSE_API_KEY=shared-secret   # optional
```

### 3. Install and start (one command)

```bash
bash scripts/run-pose-api.sh
```

This will:

1. Create `venv/`
2. `pip install` pose API requirements
3. `bash scripts/install-torch.sh` (CUDA torch when `nvidia-smi` works)
4. Start `uvicorn pose_api.main:app` on `0.0.0.0:5060` in the background

Other commands:

```bash
bash scripts/run-pose-api.sh --setup-only   # venv + deps only
bash scripts/run-pose-api.sh --start-only   # start after setup
bash scripts/run-pose-api.sh --status       # health check
bash scripts/run-pose-api.sh --stop
tail -f logs/pose-api.log
```

### 4. Verify on the pod

```bash
curl -s http://127.0.0.1:5060/health | python3 -m json.tool
```

Expect `highlight_weights_ok: true` and `pose_device: cuda`.

### 5. RunPod proxy URL

In the RunPod console, open the proxy for port **5060**, e.g.:

`https://abcdefghijkl-5060.proxy.runpod.net`

Test from your laptop:

```bash
curl -s https://abcdefghijkl-5060.proxy.runpod.net/health
```

### 6. Point the main VM at RunPod

On the **GCP VM** (`app/agents/.env` for feedback-agent):

```env
POSE_API_BASE_URL=https://abcdefghijkl-5060.proxy.runpod.net
POSE_API_KEY=same-secret-as-runpod   # if you set POSE_API_KEY on RunPod
POSE_API_POLL_TIMEOUT_SECONDS=10800
POSE_API_POLL_INTERVAL_SECONDS=3
```

Redeploy feedback-agent:

```bash
docker compose ... up -d --force-recreate feedback-agent
```

### 7. Optional — auto-start on pod boot

RunPod **Start Command** (pod template):

```bash
cd /workspace/agentic && bash scripts/run-pose-api.sh --start-only
```

Run `bash scripts/run-pose-api.sh` once manually first so the venv exists.

---

## Flow

```
Agent Lab → worker (Firestore RAG) → feedback-agent POST /api/reviews
  → feedback-agent POST POSE_API_BASE_URL/pose/jobs
  → poll GET .../status
  → GET .../result (pose JSON)
  → build_review_from_pose_json (vision + shared + personal context)
```

---

## Local smoke test (no RunPod)

```bash
# Terminal 1
bash scripts/run-pose-api.sh

# Terminal 2 — feedback-agent with remote pose
export POSE_API_BASE_URL=http://127.0.0.1:5060
PYTHONPATH=app uvicorn agents.feedback.main:app --port 5055
```

---

## Optional: Docker image

If you prefer a container later, see `app/pose_api/Dockerfile` and `docker compose --profile pose-api up`. **Not required for RunPod.**
