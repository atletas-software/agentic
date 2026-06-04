from __future__ import annotations

import json
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.orm import Session

from backendapi.db import get_db
from backendapi.dependencies.auth import get_current_user_id
from backendapi.models.workspace import AgentJob, Workspace, WorkspaceContextItem
from backendapi.services.agent_job_cancel import (
    merge_agent_job_result_json,
    request_feedback_agent_cancel,
    set_agent_job_cancel_requested,
)
from backendapi.services.sync_queue import get_redis
from backendapi.services.workspace_enqueue import (
    enqueue_feedback_delegate_job,
    enqueue_video_processing_stub_job,
    enqueue_workspace_context_refresh,
)
from backendapi.services.workspace_service import ensure_workspace
from backendapi.workers.workspace_worker import feedback_agent_poll_progress_hint

router = APIRouter(prefix="/agents", tags=["agents"])


class FeedbackReviewRequest(BaseModel):
    video_url: str = Field(default="", max_length=8000)
    player_focus: str = ""
    sport: str = "Soccer"
    analysis_scope: str = ""
    coaching_focus: str = ""
    player_key: str = ""
    first_name: str = ""
    last_name: str = ""
    text_only: bool = False

    @model_validator(mode="after")
    def _video_or_text_only(self) -> "FeedbackReviewRequest":
        if not self.text_only and len((self.video_url or "").strip()) < 4:
            raise ValueError("video_url is required when text_only is false")
        return self


@router.post("/workspace/refresh-context")
async def refresh_workspace_context(
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    ensure_workspace(user_id=user_id, db=db)
    ok = enqueue_workspace_context_refresh(user_id)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not enqueue workspace refresh. Is Redis available?",
        )
    return {"success": True, "queued": True}


@router.get("/workspace")
async def get_workspace(
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    ws = db.query(Workspace).filter(Workspace.user_id == user_id).one_or_none()
    if ws is None:
        return {"workspace": None, "context_items": [], "agent_jobs": []}
    items = (
        db.query(WorkspaceContextItem)
        .filter(WorkspaceContextItem.workspace_id == ws.id)
        .order_by(WorkspaceContextItem.created_at.desc())
        .limit(20)
        .all()
    )
    jobs = (
        db.query(AgentJob)
        .filter(AgentJob.workspace_id == ws.id)
        .order_by(AgentJob.created_at.desc())
        .limit(20)
        .all()
    )
    return {
        "workspace": {"id": ws.id, "user_id": ws.user_id, "updated_at": ws.updated_at.isoformat() if ws.updated_at else None},
        "context_items": [
            {
                "id": i.id,
                "item_type": i.item_type,
                "content_hash": i.content_hash[:16] + "...",
                "created_at": i.created_at.isoformat() if i.created_at else None,
            }
            for i in items
        ],
        "agent_jobs": [
            {
                "id": j.id,
                "agent_type": j.agent_type,
                "status": j.status,
                "external_ref": j.external_ref,
                "created_at": j.created_at.isoformat() if j.created_at else None,
                "completed_at": j.completed_at.isoformat() if j.completed_at else None,
                "error_message": j.error_message,
                "progress_hint": feedback_agent_poll_progress_hint(j.result_json),
            }
            for j in jobs
        ],
    }


@router.get("/workspace/context/{item_id}")
async def get_context_item_payload(
    item_id: int,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    ws = db.query(Workspace).filter(Workspace.user_id == user_id).one_or_none()
    if ws is None:
        raise HTTPException(status_code=404, detail="Workspace not found.")
    item = db.get(WorkspaceContextItem, item_id)
    if item is None or item.workspace_id != ws.id:
        raise HTTPException(status_code=404, detail="Context item not found.")
    return {"id": item.id, "item_type": item.item_type, "payload": json.loads(item.payload_json)}


@router.post("/feedback/reviews")
async def create_feedback_review(
    body: FeedbackReviewRequest,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    ws = ensure_workspace(user_id=user_id, db=db)
    job = AgentJob(
        workspace_id=ws.id,
        agent_type="FEEDBACK_DELEGATE",
        status="PENDING",
        payload_json=json.dumps(body.model_dump(), ensure_ascii=True),
        created_at=datetime.now(UTC),
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    rq_rid = enqueue_feedback_delegate_job(job.id)
    if not rq_rid:
        job.status = "FAILED"
        job.error_message = "Failed to enqueue feedback job."
        job.completed_at = datetime.now(UTC)
        db.commit()
        raise HTTPException(status_code=503, detail="Could not enqueue feedback job.")
    merge_agent_job_result_json(job, {"rq_job_id": rq_rid})
    db.commit()
    return {"success": True, "agent_job_id": job.id}


@router.get("/jobs/{job_id}")
async def get_agent_job(
    job_id: int,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    ws = db.query(Workspace).filter(Workspace.user_id == user_id).one_or_none()
    if ws is None:
        raise HTTPException(status_code=404, detail="Workspace not found.")
    job = db.get(AgentJob, job_id)
    if job is None or job.workspace_id != ws.id:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {
        "id": job.id,
        "agent_type": job.agent_type,
        "status": job.status,
        "payload": json.loads(job.payload_json) if job.payload_json else None,
        "result": json.loads(job.result_json) if job.result_json else None,
        "error_message": job.error_message,
        "external_ref": job.external_ref,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }


@router.post("/jobs/{job_id}/cancel")
async def cancel_agent_job(
    job_id: int,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    from rq.job import Job

    ws = db.query(Workspace).filter(Workspace.user_id == user_id).one_or_none()
    if ws is None:
        raise HTTPException(status_code=404, detail="Workspace not found.")
    job = db.get(AgentJob, job_id)
    if job is None or job.workspace_id != ws.id:
        raise HTTPException(status_code=404, detail="Job not found.")
    st = (job.status or "").upper()
    if st in ("SUCCESS", "FAILED", "SKIPPED"):
        return {"ok": False, "reason": "job_already_finished", "status": job.status}

    merge_agent_job_result_json(
        job,
        {"cancel_requested_at": datetime.now(UTC).isoformat(), "cancel_requested": True},
    )
    set_agent_job_cancel_requested(job_id)

    review_id = (job.external_ref or "").strip()
    if not review_id and job.result_json:
        try:
            meta = json.loads(job.result_json)
            poll = meta.get("feedback_agent_poll")
            if isinstance(poll, dict):
                review_id = str(poll.get("review_id") or "").strip()
        except json.JSONDecodeError:
            review_id = ""
    if review_id:
        request_feedback_agent_cancel(review_id)

    rq_job_id: str | None = None
    if job.result_json:
        try:
            rq_job_id = str(json.loads(job.result_json).get("rq_job_id") or "").strip() or None
        except json.JSONDecodeError:
            rq_job_id = None
    if rq_job_id and st == "PENDING":
        try:
            rj = Job.fetch(rq_job_id, connection=get_redis())
            rj_status = str(rj.get_status()).lower()
            if rj_status in ("queued", "scheduled", "deferred"):
                rj.cancel()
        except Exception:  # noqa: BLE001
            pass

    if st == "PENDING":
        job.status = "FAILED"
        job.error_message = "Cancelled by user (stopped before worker started)."
        job.completed_at = datetime.now(UTC)
        db.commit()
        return {"ok": True, "stopped": "pending"}

    db.commit()
    return {"ok": True, "stopped": "running", "review_id": review_id or None}


class VideoProcessRequest(BaseModel):
    video_url: str = Field(..., min_length=4, max_length=8000)
    player_focus: str = ""
    sport: str = "Soccer"
    analysis_scope: str = ""
    coaching_focus: str = ""
    player_key: str = ""
    first_name: str = ""
    last_name: str = ""
    chain_feedback: bool = True


@router.post("/video/process")
async def enqueue_video_process(
    body: VideoProcessRequest,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    """Download video, run YOLO pose JSON, optionally chain FEEDBACK_DELEGATE."""
    ws = ensure_workspace(user_id=user_id, db=db)
    job = AgentJob(
        workspace_id=ws.id,
        agent_type="VIDEO_PROCESSING",
        status="PENDING",
        payload_json=json.dumps(body.model_dump(), ensure_ascii=True),
        created_at=datetime.now(UTC),
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    ok = enqueue_video_processing_stub_job(job.id)
    if not ok:
        job.status = "FAILED"
        job.error_message = "Failed to enqueue video job."
        job.completed_at = datetime.now(UTC)
        db.commit()
        raise HTTPException(status_code=503, detail="Could not enqueue video job.")
    return {"success": True, "agent_job_id": job.id}
