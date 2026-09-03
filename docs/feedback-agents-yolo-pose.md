# Feedback stack: YOLO, Pose API, and agent versions

How to turn **`yolo_model`** and **`pose_api`** on or off, and how to **plug in a new feedback agent version** (`v2`, `v3`, …) without changing Agent Lab or the backend API contract.

Related docs:

- [`personal-and-shared-context.md`](./personal-and-shared-context.md) — RAG context attached before the agent runs
- [`app/agents/README.md`](../app/agents/README.md) — agents package layout
- [`app/yolo_model/docs/pipeline.md`](../app/yolo_model/docs/pipeline.md) — pose pipeline details

Config lives in **`app/backendapi/.env`** (and optionally repo-root `.env` / Docker Compose). Restart **API + worker** after changes. If you run the standalone feedback HTTP agent or pose-api, restart those too.

---

## Quick mental model

```
Agent Lab / API
    → enqueue FEEDBACK_DELEGATE (or VIDEO_PROCESSING → then feedback)
    → worker attaches personal + shared context
    → get_feedback_agent()          ← FEEDBACK_AGENT_VERSION (v1 | v2 | …)
    → agent.run_review(...)
         ├─ highlight path: yolo_model weights (default) OR OpenAI-only modes
         └─ pose path (optional): yolo_model locally OR pose_api (GPU)
```

| Piece | Default today | What it does |
|-------|---------------|--------------|
| Feedback agent | `FEEDBACK_AGENT_VERSION=v1` | OpenAI coaching + YOLO highlight (`agents/feedback`) |
| Highlight YOLO (`yolo_model` weights) | **On** (`VIDEO_HIGHLIGHT_DETECTOR=yolo`) | Finds circled-player moments |
| Pose pipeline | **Off** (`FEEDBACK_USE_POSE_PIPELINE=false`) | Body-keypoint / posture JSON path |
| Pose API service (`app/pose_api`) | **Not required** unless pose is on + remote URL set | GPU host that runs pose for you |

---

## 1. `yolo_model` — highlight detection (usual path)

`app/yolo_model` trains and stores highlight weights. At runtime the feedback agent loads weights (often mounted into `agents/feedback/models/highlight_yolo_v1.pt`).

### Turn highlight YOLO **on** (default)

```bash
VIDEO_HIGHLIGHT_DETECTOR=yolo

# Optional — explicit weights path (Docker usually mounts this for you)
# YOLO_HIGHLIGHT_WEIGHTS=/app/app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt
# VIDEO_HIGHLIGHT_YOLO_WEIGHTS=/app/app/agents/feedback/models/highlight_yolo_v1.pt
# VIDEO_HIGHLIGHT_YOLO_DEVICE=cpu
```

Docker Compose already mounts:

```text
app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt
  → agents/feedback/models/highlight_yolo_v1.pt
```

### Turn highlight YOLO **off** (use OpenAI vision / video instead)

Pick one:

```bash
# Frame-based OpenAI moment picking (no local YOLO highlight)
VIDEO_HIGHLIGHT_DETECTOR=openai

# Or direct OpenAI video path (Agent Lab “direct video” style)
VIDEO_HIGHLIGHT_DETECTOR=openai_video
```

You can also override **per job** via payload `highlight_detector` (Agent Lab checkbox / API field). Env is the default when the job does not override.

| Value | Behavior |
|-------|----------|
| `yolo` (default) | Uses trained highlight detector from `yolo_model` weights |
| `openai` / `gpt` / `vision` | OpenAI picks moments from frames |
| `openai_video` / `video` / `direct_video` | Direct video analysis path |
| `hsv` | Legacy color-based fallback |

### Training / updating weights (not a runtime toggle)

```bash
# Train / evaluate under app/yolo_model (see yolo_model/docs/training.md)
# Then stage weights for the running agent:
cp app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt \
   app/agents/feedback/models/highlight_yolo_v1.pt
```

Restart worker (and feedback container if separate) after replacing weights.

---

## 2. Pose pipeline + `pose_api` — optional body-keypoint path

Pose is **opt-in**. When off, feedback uses highlight YOLO (or OpenAI modes) only — no `pose_api` needed.

### Pose **off** (default)

```bash
FEEDBACK_USE_POSE_PIPELINE=false
# Leave POSE_API_BASE_URL unset
```

### Pose **on** — three deployment shapes

#### A) Remote GPU via `pose_api` (recommended for production GPU)

1. Run the pose service (`app/pose_api`) on a GPU host (Docker profile / RunPod / `scripts/run-pose-api.sh`).
2. Point the **worker / API env** at it:

```bash
FEEDBACK_USE_POSE_PIPELINE=true
POSE_PIPELINE_REMOTE_ONLY=true          # default — worker does not run YOLO locally
POSE_API_BASE_URL=https://your-pose-host:5060
# POSE_API_KEY=optional-shared-secret

# Pose model settings used by pose_api / yolo_model scripts
# YOLO_POSE_WEIGHTS=yolov8n-pose.pt
# YOLO_POSE_DEVICE=cuda
```

Flow:

```
worker (pose enabled, remote-only)
  → feedback agent sees use_pose_pipeline
  → yolo_model.pose_api.client → POST POSE_API_BASE_URL
  → pose_api runs yolo_model.pipeline.run_pose_pipeline
  → pose JSON → build_review_from_pose_json
```

#### B) Local pose inside the process that runs the agent (no separate HTTP)

```bash
FEEDBACK_USE_POSE_PIPELINE=true
POSE_PIPELINE_REMOTE_ONLY=true
# Do NOT set POSE_API_BASE_URL
```

Then `resolve_pose_data_for_video()` runs `yolo_model.pipeline.run_pose_pipeline` **in-process** (needs torch/ultralytics on that machine — heavy for the slim API worker image).

#### C) Pose on the RQ worker itself (unusual; needs torch on worker)

```bash
FEEDBACK_USE_POSE_PIPELINE=true
POSE_PIPELINE_REMOTE_ONLY=false
POSE_PIPELINE_RUN_ON_WORKER=true
YOLO_POSE_DEVICE=cpu   # or cuda
```

Worker calls `run_pose_pipeline_for_job()` and attaches `pose_json_path` before the feedback agent.

### Pose env cheat sheet

| Env | Default | Meaning |
|-----|---------|---------|
| `FEEDBACK_USE_POSE_PIPELINE` | `false` | Master switch for pose path |
| `POSE_PIPELINE_REMOTE_ONLY` | `true` | Worker must not run YOLO; agent/pose_api does |
| `POSE_PIPELINE_RUN_ON_WORKER` | unset | Only if remote-only is false; allow worker-local pose |
| `POSE_API_BASE_URL` | unset | If set + pose on → use HTTP `pose_api` |
| `POSE_API_KEY` | unset | Optional auth header for pose_api |
| `YOLO_POSE_WEIGHTS` | `yolov8n-pose.pt` | Pose checkpoint |
| `YOLO_POSE_DEVICE` | `cpu` | `cpu` / `cuda` / etc. |
| `POSE_PIPELINE_OUTPUT_DIR` | under `yolo_model/artifacts/pose` | Where pose JSON is written |

### Start / stop `pose_api` (service)

```bash
# Docker (from agentic/)
docker compose --profile pose-api up -d pose-api

# Or host script
bash scripts/run-pose-api.sh
```

Service code: `app/pose_api/main.py` (calls into `yolo_model.pipeline`).  
Client code: `app/yolo_model/pose_api/client.py`.

---

## 3. Plug in a new feedback agent version

Backend and Agent Lab always call:

```python
from agents.registry import get_feedback_agent

agent = get_feedback_agent()  # reads FEEDBACK_AGENT_VERSION
review = agent.run_review(review_id, payload, on_progress=..., cancel_check=...)
```

They do **not** import `agents.feedback` directly for job execution. That is what makes plug-in / plug-out possible.

### Versions today

| `FEEDBACK_AGENT_VERSION` | Package | Status |
|--------------------------|---------|--------|
| `v1` or `default` | `app/agents/feedback/` | Production (OpenAI + YOLO) |
| `v2` | `app/agents/feedback_v2/` | Template stub — implement then enable |

### Switch version (no code change)

```bash
# app/backendapi/.env
FEEDBACK_AGENT_VERSION=v1   # current
# FEEDBACK_AGENT_VERSION=v2 # after you implement FeedbackAgentV2
```

Restart API + worker. Confirm job `result_json` includes `"feedback_agent_version": "v2"`.

### Add a new version (detailed steps)

#### Step 1 — Create the package

```text
app/agents/feedback_v2/          # or feedback_v3, coaching_skill, …
  __init__.py
  agent_entry.py                 # REQUIRED — class + register()
  … your code, prompts, scripts …
```

You can copy from `feedback/` or start fresh. Example skill-style package (`coaching-feedback-skill-package`) can live as reference; the **runtime** entry must still be a Python `FeedbackAgent`.

#### Step 2 — Implement the contract

In `agent_entry.py`:

```python
from agents.registry import register_feedback_agent

class FeedbackAgentV2:
    version = "v2"

    def run_review(
        self,
        review_id: str,
        payload: dict,
        *,
        on_progress=None,
        cancel_check=None,
    ) -> dict:
        # Use payload fields the worker already attached:
        #   video_url, sport, player_focus, coaching_prompt / coaching_focus
        #   player_memory_context  (personal RAG)
        #   shared_context         (shared / club rubric RAG)
        #   text_only, use_pose_pipeline, pose_json_path, highlight_detector, …
        #
        # Return a platform review document, e.g.:
        return {
            "id": review_id,
            "title": "...",
            "video_url": payload.get("video_url") or "",
            "duration_sec": 0.0,
            "analysis_mode": "my-v2-mode",
            "allowed_timestamps": [],
            "video_summary": {
                "sport": payload.get("sport") or "Soccer",
                "player_focus": payload.get("player_focus") or "",
                "duration_sec": 0.0,
                "analysis_scope": payload.get("analysis_scope") or "",
            },
            "overall_assessment": {
                "strengths": [],
                "improvements": [],
                "next_focus": [],
            },
            "markers": [
                # {
                #   "id": "m0",
                #   "timestamp_sec": 12.3,
                #   "category": "coaching",
                #   "sentiment": "neutral",
                #   "label": "...",
                #   "coaching_note": "...",
                # }
            ],
        }

def register() -> None:
    register_feedback_agent("v2", FeedbackAgentV2)
```

Registry auto-imports `agents.feedback_v2.agent_entry.register` when present (`agents/registry.py`).

For a third version, either:

- add `app/agents/feedback_v3/` and extend `_ensure_builtins_registered()` to import it, **or**
- call `register_feedback_agent("v3", MyAgent)` from that package and import it once at startup.

#### Step 3 — Map skill-style params (optional)

If your new agent follows something like `coaching-feedback-skill-package`:

| Skill param | Platform `payload` |
|-------------|-------------------|
| `player_name` | `player_focus` |
| `video_path` | `video_url` |
| `prompt` | `coaching_prompt` / `coaching_focus` |
| `personal_context` | `player_memory_context` |
| `shared_context` | `shared_context` |
| feedback points `[{time, label, text}]` | `markers[]` with `timestamp_sec`, `label`, `coaching_note` |

Personal/shared retrieval stays in the **worker** (`retrieve_feedback_context`). Your agent only consumes the strings.

#### Step 4 — Enable

```bash
FEEDBACK_AGENT_VERSION=v2
```

Restart. Agent Lab UI and `/admin-api/agents-lab/feedback-reviews` stay the same.

#### Step 5 — Roll back

```bash
FEEDBACK_AGENT_VERSION=v1
```

Restart. Old agent is plugged back in immediately.

### What you do **not** need to change

- Agent Lab frontend
- `admin.py` / `agents.py` job creation
- RQ enqueue (`process_feedback_delegate_job`)
- Personal/shared RAG attachment in the worker

### What stays orthogonal to agent version

| Concern | Env / system | Note |
|---------|--------------|------|
| Highlight YOLO vs OpenAI | `VIDEO_HIGHLIGHT_DETECTOR` | Used by **v1**; v2 may ignore or reimplement |
| Pose on/off | `FEEDBACK_USE_POSE_PIPELINE` | Worker may still attach pose flags; v2 can ignore |
| Pose remote API | `POSE_API_BASE_URL` | Only if your agent (or v1) uses pose client |
| HTTP vs in-process transport | `FEEDBACK_DELEGATE_HTTP` | Legacy; not a version switch |

---

## 4. Recommended presets

### Production default (current)

```bash
FEEDBACK_AGENT_VERSION=v1
VIDEO_HIGHLIGHT_DETECTOR=yolo
FEEDBACK_USE_POSE_PIPELINE=false
# POSE_API_BASE_URL unset
```

### Try new agent, keep YOLO path inside it (if v2 still uses YOLO)

```bash
FEEDBACK_AGENT_VERSION=v2
VIDEO_HIGHLIGHT_DETECTOR=yolo
FEEDBACK_USE_POSE_PIPELINE=false
```

### Pose-heavy reviews on GPU

```bash
FEEDBACK_AGENT_VERSION=v1
FEEDBACK_USE_POSE_PIPELINE=true
POSE_PIPELINE_REMOTE_ONLY=true
POSE_API_BASE_URL=https://your-gpu-pose-api:5060
VIDEO_HIGHLIGHT_DETECTOR=yolo   # still available for non-pose jobs
```

### Disable local YOLO highlight (OpenAI-only moments)

```bash
FEEDBACK_AGENT_VERSION=v1
VIDEO_HIGHLIGHT_DETECTOR=openai
FEEDBACK_USE_POSE_PIPELINE=false
```

---

## 5. Verify

```bash
# Which agent is registered
PYTHONPATH=app python -c "from agents.registry import list_feedback_agents, resolve_feedback_agent_version as v; print(v(), list_feedback_agents())"

# Pose flags
# FEEDBACK_USE_POSE_PIPELINE / POSE_API_BASE_URL in backendapi/.env

# After a job: result_json should include feedback_agent_version
# Worker / API logs: feedback_inprocess_complete, pose_pipeline phases if enabled
```

Health helpers:

- Feedback YOLO readiness: routes that call `yolo_model.runtime_health.check_yolo_runtime`
- Pose API: `GET {POSE_API_BASE_URL}/health` (see `app/pose_api/main.py`)

---

## File map

| Path | Role |
|------|------|
| `app/agents/registry.py` | `get_feedback_agent` / `register_feedback_agent` |
| `app/agents/feedback/agent_entry.py` | **v1** implementation |
| `app/agents/feedback_v2/agent_entry.py` | **v2** template |
| `app/backendapi/services/feedback_runner.py` | Thin adapter → registry |
| `app/backendapi/workers/workspace_worker.py` | Context attach + run agent |
| `app/backendapi/services/pose_video_pipeline.py` | Pose on/off helpers |
| `app/yolo_model/` | Train, pose pipeline, pose HTTP client |
| `app/pose_api/` | Standalone GPU pose HTTP service |
| `app/agents/feedback/highlight/` | Runtime highlight YOLO used by v1 |
