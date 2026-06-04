# agents/

Python services invoked by the platform (not the main FastAPI `app/backendapi/`).

## Layout

```
agents/
  requirements.txt       feedback agent dependencies (Docker + local venv)
  .env.example           copy to app/agents/.env for OPENAI_API_KEY, etc.
  feedback/
    main.py              FastAPI app (:5055)
    review_agent.py      video review orchestration
    openai_service.py    vision + text LLM calls
    video_utils.py       ffmpeg frames, legacy HSV highlight detection
    models.py            Pydantic review schema
    storage.py           review JSON on disk (DATA_DIR)
    highlight/           YOLO highlight pipeline (probe → events → assets)
    models/              runtime YOLO weights (highlight_yolo_v1.pt, git-ignored)
    templates/           HTML UI (index, job, review, share, calibrate)
    video_feedback_agent_system_prompt.md
    Dockerfile
```

## Run locally

```bash
cp app/agents/.env.example app/agents/.env   # set OPENAI_API_KEY
PYTHONPATH=app uvicorn agents.feedback.main:app --host 0.0.0.0 --port 5055
```

Or use `bash scripts/run.sh` from the repo root (starts API, worker, and feedback agent).

## Weights

Train in `yolo_model/`, then promote:

```bash
cp yolo_model/exports/highlight_yolo_v1.1.0.pt agents/feedback/models/highlight_yolo_v1.pt
```

See `agents/feedback/models/README.md` and `yolo_model/docs/training.md`.
