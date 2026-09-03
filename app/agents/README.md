# agents/

Python feedback/YOLO packages used **in-process** by the platform API and worker.

## Plug-in / plug-out feedback agents

Backend never imports a specific agent implementation for job execution.
It always goes through:

```python
from agents.registry import get_feedback_agent

agent = get_feedback_agent()  # FEEDBACK_AGENT_VERSION (default: v1)
review = agent.run_review(review_id, payload, on_progress=..., cancel_check=...)
```

| Version | Package | Status |
|---------|---------|--------|
| `v1` / `default` | `agents/feedback/` | Production — OpenAI + YOLO |
| `v2` | `agents/feedback_v2/` | Template stub — implement then enable |

### Switch version

In `app/backendapi/.env`:

```bash
FEEDBACK_AGENT_VERSION=v1   # or v2 once implemented
```

Restart API + worker. Agent Lab and `/agents/feedback/reviews` stay the same.

### Add a newer agent

1. Copy or create `agents/feedback_v2/` (see `feedback_v2/agent_entry.py`).
2. Implement `FeedbackAgentV2.run_review` with the same contract as `FeedbackAgentV1`.
3. Keep returning the standard review document (`id`, `title`, `markers`, `overall_assessment`, …).
4. `register_feedback_agent("v2", FeedbackAgentV2)` (already wired in the template).
5. Set `FEEDBACK_AGENT_VERSION=v2`.

Contract: `agents/registry.py` → `FeedbackAgent` Protocol.

Personal + shared context are attached by the **worker** before your agent runs
(`player_memory_context`, `shared_context`). See
[`docs/personal-and-shared-context.md`](../docs/personal-and-shared-context.md).

YOLO highlight / pose_api on-off and full plug-in steps:
[`docs/feedback-agents-yolo-pose.md`](../docs/feedback-agents-yolo-pose.md).

## Layout

```
agents/
  registry.py              # get_feedback_agent() / register_feedback_agent()
  requirements.txt         # ML deps for local venv (Docker uses backendapi Dockerfile)
  feedback/                # v1 (current)
    agent_entry.py         # FeedbackAgentV1 — registry entrypoint
    routes.py              # review UI + /api/reviews (mounted on platform API :8000)
    review_agent.py        # video review orchestration
    openai_service.py      # vision + text LLM calls
    highlight/             # YOLO highlight pipeline
    models/                # runtime YOLO weights (git-ignored)
    templates/             # HTML review pages
  feedback_v2/             # plug-in template for the next agent
    agent_entry.py
```

## Configuration

Secrets and YOLO settings live in **`app/backendapi/.env`** (see `.env.example`).

```bash
FEEDBACK_AGENT_VERSION=v1
```

## Optional standalone agent (:5055)

Only needed for legacy `FEEDBACK_DELEGATE_HTTP=true`:

```bash
PYTHONPATH=app uvicorn agents.feedback.main:app --host 0.0.0.0 --port 5055
```

Normal local/Docker flow uses `make run-all` or `bash scripts/start-stack.sh` (API + worker only).

## Weights

```bash
cp yolo_model/exports/highlight_yolo_v1.1.0.pt agents/feedback/models/highlight_yolo_v1.pt
```

See `model-weights/README.md` and `yolo_model/docs/training.md`.
