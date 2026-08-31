"""Standalone feedback agent (optional — review UI is also mounted on backendapi :8000)."""

from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from agents.feedback.routes import router as feedback_router, TEMPLATES
from agents.feedback.storage import ensure_directories
from agents.feedback.urls import frontend_origin

_AGENTS_DIR = Path(__file__).resolve().parent.parent
_BACKEND_ENV = _AGENTS_DIR.parent / "backendapi" / ".env"
load_dotenv()
if _BACKEND_ENV.is_file():
    load_dotenv(_BACKEND_ENV, override=True)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_directories()
    if not frontend_origin() and not os.getenv("PUBLIC_BASE_URL", "").strip():
        print(
            "[feedback-agent] WARNING: FRONTEND_BASE_URL is not set. "
            "Share/review links will use HOST:PORT and will not open on the Next.js frontend. "
            "Set FRONTEND_BASE_URL to the frontend origin (e.g. http://localhost:3000).",
            file=sys.stderr,
            flush=True,
        )
    yield


app = FastAPI(title="Feedback agent", lifespan=lifespan)
app.include_router(feedback_router)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return TEMPLATES.TemplateResponse("index.html", {"request": request})


@app.get("/health")
async def health() -> JSONResponse:
    from yolo_model.runtime_health import check_yolo_runtime

    yolo = check_yolo_runtime()
    return JSONResponse(
        {
            "ok": True,
            "yolo": yolo,
            "pose_pipeline_ready": bool(yolo.get("ready_for_pose_pipeline")),
        }
    )
