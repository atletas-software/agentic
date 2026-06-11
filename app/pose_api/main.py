"""RunPod / GPU host: async YOLO highlight + pose jobs over HTTP.

Contract (feedback-agent and worker use ``yolo_model.pose_api.client``):

  POST   /pose/jobs              → { id, status, status_url, result_url }
  GET    /pose/jobs/{id}/status  → job record (queued|running|completed|failed)
  GET    /pose/jobs/{id}/result  → pose JSON body (200 when completed)
  POST   /pose/jobs/{id}/cancel  → cooperative cancel
  GET    /health                 → weights + device readiness
"""

from __future__ import annotations

import os
import threading
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from pose_api.storage import (
    cancel_flag_path,
    ensure_directories,
    job_cancel_requested,
    job_dir,
    job_path,
    load_json,
    pose_result_path,
    save_json,
)

_APP_DIR = Path(__file__).resolve().parent.parent
_POSE_API_DIR = Path(__file__).resolve().parent
load_dotenv()
load_dotenv(_APP_DIR / "agents" / ".env")
load_dotenv(_POSE_API_DIR / ".env", override=True)


def _verify_api_key(request: Request) -> None:
    expected = (os.getenv("POSE_API_KEY") or "").strip()
    if not expected:
        return
    auth = (request.headers.get("Authorization") or "").strip()
    if auth != f"Bearer {expected}":
        raise HTTPException(status_code=401, detail="Invalid or missing POSE_API_KEY")


def _merge_job(job_id: str, patch: dict[str, Any]) -> None:
    job = load_json(job_path(job_id)) or {"id": job_id}
    job.update(patch)
    job.setdefault("status", "running")
    save_json(job_path(job_id), job)


def _run_pose_job(job_id: str, video_url: str, job_key: str) -> None:
    _merge_job(
        job_id,
        {
            "id": job_id,
            "status": "running",
            "video_url": video_url,
            "job_key": job_key,
            "phase": "pose_inference",
            "progress_detail": "Downloading video and running YOLO pose pipeline…",
            "error": None,
        },
    )
    try:
        if job_cancel_requested(job_id):
            raise RuntimeError("Cancelled by client.")

        from yolo_model.pipeline.runner import run_pose_pipeline

        out_path = run_pose_pipeline(video_url, job_key=job_key or f"pose_{job_id}")
        if job_cancel_requested(job_id):
            raise RuntimeError("Cancelled by client.")

        dest = pose_result_path(job_id)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(Path(out_path).read_text(encoding="utf-8"), encoding="utf-8")

        pose_preview = load_json(dest) or {}
        _merge_job(
            job_id,
            {
                "status": "completed",
                "error": None,
                "pose_json_path": str(dest),
                "frame_count": pose_preview.get("total_frames"),
                "fps": pose_preview.get("fps"),
                "phase": None,
                "progress_detail": "Pose pipeline complete.",
            },
        )
    except Exception as exc:  # noqa: BLE001
        job = load_json(job_path(job_id)) or {"id": job_id}
        job["status"] = "failed"
        job["error"] = str(exc)
        save_json(job_path(job_id), job)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_directories()
    yield


app = FastAPI(title="Pose API (YOLO)", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    out: dict[str, Any] = {"ok": True, "service": "pose-api"}
    try:
        from yolo_model.pipeline.runner import resolve_highlight_weights

        weights = resolve_highlight_weights()
        out["highlight_weights_ok"] = weights.is_file()
        out["highlight_weights_path"] = str(weights)
        out["highlight_weights_bytes"] = weights.stat().st_size if weights.is_file() else 0
    except Exception as exc:  # noqa: BLE001
        out["ok"] = False
        out["highlight_weights_ok"] = False
        out["error"] = str(exc)
    out["pose_device"] = (os.getenv("YOLO_POSE_DEVICE") or "cpu").strip()
    out["pose_weights"] = (os.getenv("YOLO_POSE_WEIGHTS") or "yolov8n-pose.pt").strip()
    return out


@app.post("/pose/jobs")
async def create_pose_job(request: Request) -> JSONResponse:
    _verify_api_key(request)
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    video_url = str(body.get("video_url") or "").strip()
    if not video_url:
        return JSONResponse({"error": "video_url is required"}, status_code=400)

    job_id = uuid.uuid4().hex[:12]
    job_key = str(body.get("job_key") or f"pose_{job_id}").strip()
    job_dir(job_id).mkdir(parents=True, exist_ok=True)
    save_json(
        job_path(job_id),
        {
            "id": job_id,
            "status": "queued",
            "video_url": video_url,
            "job_key": job_key,
            "error": None,
        },
    )

    thread = threading.Thread(
        target=_run_pose_job,
        args=(job_id, video_url, job_key),
        daemon=True,
        name=f"pose-job-{job_id}",
    )
    thread.start()

    return JSONResponse(
        {
            "id": job_id,
            "status": "queued",
            "status_url": f"/pose/jobs/{job_id}/status",
            "result_url": f"/pose/jobs/{job_id}/result",
        }
    )


@app.get("/pose/jobs/{job_id}/status")
async def pose_job_status(job_id: str, request: Request) -> JSONResponse:
    _verify_api_key(request)
    job = load_json(job_path(job_id))
    if not job:
        return JSONResponse({"error": "pose job not found"}, status_code=404)
    return JSONResponse(job)


@app.get("/pose/jobs/{job_id}/result")
async def pose_job_result(job_id: str, request: Request) -> JSONResponse:
    _verify_api_key(request)
    job = load_json(job_path(job_id))
    if not job:
        raise HTTPException(status_code=404, detail="pose job not found")
    st = str(job.get("status") or "")
    if st != "completed":
        raise HTTPException(
            status_code=409,
            detail=f"pose job not ready (status={st or 'unknown'})",
        )
    data = load_json(pose_result_path(job_id))
    if not data:
        raise HTTPException(status_code=404, detail="pose result file missing")
    return JSONResponse(data)


@app.post("/pose/jobs/{job_id}/cancel")
async def cancel_pose_job(job_id: str, request: Request) -> JSONResponse:
    _verify_api_key(request)
    job_dir(job_id).mkdir(parents=True, exist_ok=True)
    cancel_flag_path(job_id).write_text("1", encoding="utf-8")
    return JSONResponse({"ok": True, "job_id": job_id})
