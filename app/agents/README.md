# agents/

Python feedback/YOLO code used in-process by the platform API and worker.

## Layout

```
agents/
  requirements.txt       ML deps for local venv (Docker uses app/backendapi/Dockerfile)
  feedback/
    routes.py            review UI + /api/reviews (mounted on platform API :8000)
    review_agent.py      video review orchestration
    openai_service.py    vision + text LLM calls
    highlight/           YOLO highlight pipeline
    models/              runtime YOLO weights (highlight_yolo_v1.pt, git-ignored)
    templates/           HTML review pages
```

## Configuration

All secrets and YOLO settings live in **`app/backendapi/.env`** (see `app/backendapi/.env.example`).

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
