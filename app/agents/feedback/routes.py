"""Feedback review UI + JSON API routes (mounted on platform API and optional standalone agent)."""

from __future__ import annotations

import secrets
import shutil
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from agents.feedback.job_runners import (
    REVIEWS_DIR,
    calibration_path,
    coaching_focus_from_payload,
    load_job,
    review_path,
    run_pose_review_job,
    run_review_job,
    run_text_coaching_job,
)
from agents.feedback.openai_service import analyze_manual_moment
from agents.feedback.storage import load_json, save_json
from agents.feedback.urls import frontend_origin, public_url_for, viewer_url
from agents.feedback.video_utils import (
    crop_reference_patch,
    draw_bbox_overlay,
    draw_player_circle,
    extract_frame_at_timestamp,
    locate_patch_in_frame,
)

TEMPLATES = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))

router = APIRouter(include_in_schema=False)


def normalize_review(review: Dict[str, Any]) -> Dict[str, Any]:
    duration = float(review.get("duration_sec") or 0)
    if duration <= 0:
        return review
    for marker in review.get("markers", []):
        ts = float(marker.get("timestamp_sec") or 0)
        marker["timestamp_sec"] = round(max(0.0, min(ts, max(duration - 0.25, 0.0))), 2)
    return review


def marker_label(category: str, sentiment: str) -> str:
    prefix = {
        "positive": "Positive",
        "corrective": "Improve",
        "mixed": "Review",
    }.get(sentiment, "Review")
    return f"{prefix}: {category.replace('_', ' ').title()}"


def build_manual_context_sequence(
    *,
    video_url: str,
    output_dir: Path,
    timestamp_sec: float,
    context_start_sec: float,
    patch_path: Path,
) -> list[Path]:
    window = max(0.0, timestamp_sec - context_start_sec)
    if window <= 0:
        return []

    context_paths: list[Path] = []
    sample_count = 5
    marker_key = int(timestamp_sec * 100)
    for index in range(sample_count):
        sample_sec = context_start_sec + (window * index / sample_count)
        raw_context_path = output_dir / f"marker_{marker_key:08d}_context_{index + 1:02d}_raw.jpg"
        marked_context_path = output_dir / f"marker_{marker_key:08d}_context_{index + 1:02d}_marked.jpg"
        extract_frame_at_timestamp(video_url, sample_sec, raw_context_path)
        location = locate_patch_in_frame(frame_path=raw_context_path, patch_path=patch_path)
        draw_player_circle(
            image_path=raw_context_path,
            output_path=marked_context_path,
            center_x=float(location.get("center_x", 0.5)),
            center_y=float(location.get("center_y", 0.5)),
            radius=float(location.get("radius", 0.08)),
            label=f"Selected player context {sample_sec:.2f}s",
            note=str(location.get("note", "")),
            found=bool(location.get("found")),
        )
        context_paths.append(marked_context_path)
    return context_paths



@router.post("/api/reviews", name="create_review")
async def create_review(request: Request) -> JSONResponse:
    ct = (request.headers.get("content-type") or "").lower()
    if "application/json" in ct:
        try:
            payload = await request.json()
        except Exception:  # noqa: BLE001
            payload = {}
    else:
        form = await request.form()
        payload = {k: str(v) for k, v in form.items()}

    text_only = bool(payload.get("text_only"))
    video_url = (payload.get("video_url") or "").strip()
    player_focus = (payload.get("player_focus") or "").strip()
    analysis_scope = (payload.get("analysis_scope") or "").strip()
    coaching_focus = coaching_focus_from_payload(payload)
    mem = (payload.get("player_memory_context") or "").strip()
    shared = (payload.get("shared_context") or "").strip()

    if not text_only and not video_url:
        return JSONResponse({"error": "video_url is required unless text_only is true"}, status_code=400)
    if text_only:
        if not any([video_url, player_focus, analysis_scope, coaching_focus, mem, shared]):
            return JSONResponse(
                {
                    "error": "text_only requires at least one of: video_url, player_focus, analysis_scope, "
                    "coaching_focus, player_memory_context, or shared_context",
                },
                status_code=400,
            )

    review_id = uuid.uuid4().hex[:12]
    review_dir = REVIEWS_DIR / review_id
    review_dir.mkdir(parents=True, exist_ok=True)

    job_payload = {
        "video_url": video_url,
        "player_focus": player_focus,
        "sport": (payload.get("sport") or "Soccer").strip(),
        "analysis_scope": analysis_scope,
        "coaching_focus": coaching_focus,
        "coaching_prompt": coaching_focus,
        "player_memory_context": mem,
        "shared_context": shared,
        "text_only": text_only,
        "first_name": (payload.get("first_name") or "").strip(),
        "last_name": (payload.get("last_name") or "").strip(),
        "player_memory_retrieval_debug": payload.get("player_memory_retrieval_debug"),
        "shared_context_sheet_debug": payload.get("shared_context_sheet_debug"),
        "pose_json_path": (payload.get("pose_json_path") or "").strip(),
        "use_pose_pipeline": bool(payload.get("use_pose_pipeline")),
        "use_legacy_review": bool(payload.get("use_legacy_review")),
        "highlight_detector": (payload.get("highlight_detector") or "").strip(),
    }
    if text_only:
        runner = run_text_coaching_job
    elif job_payload["pose_json_path"] or job_payload["use_pose_pipeline"]:
        runner = run_pose_review_job
    else:
        runner = run_review_job
    thread = threading.Thread(
        target=runner,
        args=(review_id, job_payload),
        daemon=True,
    )
    thread.start()

    status_url = public_url_for(request, "job_status", review_id=review_id)
    watch_url = public_url_for(request, "job_page", review_id=review_id)
    return JSONResponse(
        {
            "id": review_id,
            "status": "queued",
            "status_url": status_url,
            "watch_url": watch_url,
        }
    )


@router.get("/jobs/{review_id}", response_class=HTMLResponse, name="job_page")
async def job_page(request: Request, review_id: str) -> HTMLResponse:
    return TEMPLATES.TemplateResponse("job.html", {"request": request, "review_id": review_id})


@router.get("/api/reviews/{review_id}/status", name="job_status")
async def job_status(review_id: str) -> JSONResponse:
    job = load_job(review_id)
    if not job:
        return JSONResponse({"error": "review job not found"}, status_code=404)
    return JSONResponse(job)


@router.post("/api/reviews/{review_id}/cancel")
async def cancel_review_background(review_id: str) -> JSONResponse:
    (REVIEWS_DIR / review_id).mkdir(parents=True, exist_ok=True)
    (REVIEWS_DIR / review_id / "cancel_requested").write_text("1", encoding="utf-8")
    return JSONResponse({"ok": True, "review_id": review_id})


@router.get("/review/{review_id}", response_class=HTMLResponse)
async def review_page(request: Request, review_id: str) -> HTMLResponse:
    review = load_json(review_path(review_id))
    if not review:
        job = load_job(review_id)
        st = str((job or {}).get("status") or "").lower()
        if job and st not in ("completed", "failed"):
            return RedirectResponse(url=f"/jobs/{review_id}", status_code=302)
        raise HTTPException(status_code=404)
    review = normalize_review(review)
    calibration = load_json(calibration_path(review_id))
    share_url = None
    share_path = None
    if review.get("share_token"):
        share_path = f"/share/{review['share_token']}"
        share_url = viewer_url(request, share_path)
    return TEMPLATES.TemplateResponse(
        "review.html",
        {
            "request": request,
            "review": review,
            "calibration": calibration,
            "share_url": share_url,
            "share_path": share_path,
            "frontend_origin": frontend_origin() or "",
        },
    )


@router.get("/share/{token}", response_class=HTMLResponse, name="shared_review_page")
async def shared_review_page(request: Request, token: str) -> HTMLResponse:
    for path in REVIEWS_DIR.glob("*/review.json"):
        review = load_json(path)
        if review and secrets.compare_digest(str(review.get("share_token", "")), token):
            return TEMPLATES.TemplateResponse(
                "share_review.html",
                {"request": request, "review": normalize_review(review)},
            )
    raise HTTPException(status_code=404)


@router.get("/review/{review_id}/calibrate", response_class=HTMLResponse)
async def calibration_page(
    request: Request,
    review_id: str,
    ts: Optional[str] = Query(default=None),
) -> HTMLResponse:
    review = load_json(review_path(review_id))
    if not review:
        raise HTTPException(status_code=404)
    review = normalize_review(review)
    markers = review.get("markers", [])
    if ts is not None and ts != "":
        try:
            default_ts = float(ts)
        except ValueError:
            default_ts = float(markers[0]["timestamp_sec"] if markers else 0)
    else:
        default_ts = float(markers[0]["timestamp_sec"] if markers else 0)
    return TEMPLATES.TemplateResponse(
        "calibrate.html",
        {"request": request, "review": review, "timestamp_sec": default_ts},
    )


@router.get("/api/reviews/{review_id}")
async def review_data(review_id: str) -> JSONResponse:
    review = load_json(review_path(review_id))
    if not review:
        return JSONResponse({"error": "review not found"}, status_code=404)
    return JSONResponse(normalize_review(review))


@router.post("/api/reviews/{review_id}/share")
async def create_share_link(request: Request, review_id: str) -> JSONResponse:
    review = load_json(review_path(review_id))
    if not review:
        return JSONResponse({"error": "review not found"}, status_code=404)
    if not review.get("share_token"):
        review["share_token"] = secrets.token_urlsafe(24)
        save_json(review_path(review_id), review)
    share_path = f"/share/{review['share_token']}"
    return JSONResponse(
        {
            "ok": True,
            "share_path": share_path,
            "share_url": viewer_url(request, share_path),
        }
    )


@router.delete("/api/reviews/{review_id}/markers")
async def clear_review_markers(review_id: str) -> JSONResponse:
    review = load_json(review_path(review_id))
    if not review:
        return JSONResponse({"error": "review not found"}, status_code=404)
    review["markers"] = []
    review["overall_assessment"] = {
        "strengths": [],
        "improvements": [],
        "next_focus": [],
    }
    save_json(review_path(review_id), review)
    focus_dir = REVIEWS_DIR / review_id / "focus_frames"
    if focus_dir.exists():
        shutil.rmtree(focus_dir)
    return JSONResponse({"ok": True, "markers": []})


@router.post("/api/pose-markers", name="create_pose_markers")
async def create_pose_markers(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        payload = {}
    payload["use_pose_pipeline"] = True
    payload.setdefault("text_only", False)
    review_id = uuid.uuid4().hex[:12]
    (REVIEWS_DIR / review_id).mkdir(parents=True, exist_ok=True)
    thread = threading.Thread(
        target=run_pose_review_job,
        args=(review_id, payload),
        daemon=True,
    )
    thread.start()
    status_url = public_url_for(request, "job_status", review_id=review_id)
    watch_url = public_url_for(request, "job_page", review_id=review_id)
    return JSONResponse(
        {
            "id": review_id,
            "status": "queued",
            "status_url": status_url,
            "watch_url": watch_url,
        }
    )


@router.post("/api/reviews/{review_id}/manual-feedback")
async def create_manual_feedback(request: Request, review_id: str) -> JSONResponse:
    review = load_json(review_path(review_id))
    if not review:
        return JSONResponse({"error": "review not found"}, status_code=404)

    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        payload = {}
    try:
        timestamp_sec = float(payload.get("timestamp_sec"))
        bbox = payload.get("bbox") or {}
        x = float(bbox.get("x"))
        y = float(bbox.get("y"))
        width = float(bbox.get("width"))
        height = float(bbox.get("height"))
    except (TypeError, ValueError):
        return JSONResponse({"error": "Invalid feedback payload."}, status_code=400)
    feedback_prompt = (payload.get("feedback_prompt") or "").strip()
    player_name = (payload.get("player_name") or "").strip()

    if width <= 0 or height <= 0:
        return JSONResponse({"error": "Draw a player box before requesting feedback."}, status_code=400)

    bbox = {
        "x": max(0.0, min(x, 1.0)),
        "y": max(0.0, min(y, 1.0)),
        "width": max(0.001, min(width, 1.0)),
        "height": max(0.001, min(height, 1.0)),
    }
    duration = float(review.get("duration_sec") or 0)
    timestamp_sec = max(0.0, min(timestamp_sec, max(duration - 0.25, 0.0) if duration else timestamp_sec))
    context_sec = max(0.0, timestamp_sec - 2.0)

    manual_dir = REVIEWS_DIR / review_id / "manual_feedback"
    raw_path = manual_dir / f"marker_{int(timestamp_sec * 100):08d}_raw.jpg"
    marked_path = manual_dir / f"marker_{int(timestamp_sec * 100):08d}_marked.jpg"
    patch_path = manual_dir / f"marker_{int(timestamp_sec * 100):08d}_patch.jpg"

    extract_frame_at_timestamp(review["video_url"], timestamp_sec, raw_path)
    crop_reference_patch(image_path=raw_path, bbox=bbox, output_path=patch_path)
    draw_bbox_overlay(
        image_path=raw_path,
        output_path=marked_path,
        bbox=bbox,
        label=f"Selected player at {timestamp_sec:.2f}s",
    )
    context_paths = build_manual_context_sequence(
        video_url=review["video_url"],
        output_dir=manual_dir,
        timestamp_sec=timestamp_sec,
        context_start_sec=context_sec,
        patch_path=patch_path,
    )

    feedback = analyze_manual_moment(
        context_image_paths=context_paths,
        marked_image_path=marked_path,
        player_focus=(review.get("video_summary") or {}).get("player_focus", review.get("title", "")),
        player_name=player_name,
        sport=(review.get("video_summary") or {}).get("sport", "Soccer"),
        timestamp_sec=timestamp_sec,
        context_start_sec=context_sec,
        feedback_prompt=feedback_prompt,
    )

    markers = review.setdefault("markers", [])
    marker_id = max([int(marker.get("id", 0)) for marker in markers] or [0]) + 1
    marker = {
        "id": marker_id,
        "timestamp_sec": round(timestamp_sec, 2),
        "action_type": feedback.action_type,
        "category": feedback.category,
        "sentiment": feedback.sentiment,
        "label": marker_label(feedback.category, feedback.sentiment),
        "coaching_note": feedback.coaching_note,
        "reference_clip": None,
        "diagram_request": None,
        "freeze_frame_request": {
            "title": "Manual selected player frame",
            "reason": "Player was selected manually with a bounding box.",
        },
        "manual_bbox": bbox,
        "manual_context_sec": round(context_sec, 2),
        "manual_context_window_sec": 2,
        "feedback_prompt": feedback_prompt,
        "player_name": player_name,
        "manual_image_url": public_url_for(
            request, "manual_feedback_image", review_id=review_id, marker_id=marker_id
        ),
    }
    markers.append(marker)
    markers.sort(key=lambda item: float(item.get("timestamp_sec", 0)))
    for index, item in enumerate(markers, start=1):
        item["id"] = index
    save_json(review_path(review_id), review)

    stored_marker = next(
        item
        for item in markers
        if item["timestamp_sec"] == marker["timestamp_sec"] and item["coaching_note"] == marker["coaching_note"]
    )
    stored_marker["manual_image_url"] = public_url_for(
        request, "manual_feedback_image", review_id=review_id, marker_id=stored_marker["id"]
    )
    save_json(review_path(review_id), review)
    return JSONResponse({"ok": True, "marker": stored_marker})


@router.get("/api/reviews/{review_id}/markers/{marker_id}/manual-image", name="manual_feedback_image")
async def manual_feedback_image(review_id: str, marker_id: int) -> FileResponse:
    review = load_json(review_path(review_id))
    if not review:
        raise HTTPException(status_code=404)
    marker = next((item for item in review.get("markers", []) if int(item.get("id", -1)) == marker_id), None)
    if not marker:
        raise HTTPException(status_code=404)
    timestamp_sec = float(marker.get("timestamp_sec") or 0)
    path = REVIEWS_DIR / review_id / "manual_feedback" / f"marker_{int(timestamp_sec * 100):08d}_marked.jpg"
    if not path.exists():
        raise HTTPException(status_code=404)
    return FileResponse(path, media_type="image/jpeg")


@router.get("/api/reviews/{review_id}/calibration")
async def calibration_status(review_id: str) -> JSONResponse:
    calibration = load_json(calibration_path(review_id))
    if not calibration:
        return JSONResponse({"configured": False})
    return JSONResponse({"configured": True, "calibration": calibration})


@router.post("/api/reviews/{review_id}/calibration")
async def save_calibration(review_id: str, request: Request) -> JSONResponse:
    review = load_json(review_path(review_id))
    if not review:
        return JSONResponse({"error": "review not found"}, status_code=404)

    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        payload = {}
    try:
        timestamp_sec = float(payload.get("timestamp_sec"))
        bbox = payload.get("bbox") or {}
        x = float(bbox.get("x"))
        y = float(bbox.get("y"))
        width = float(bbox.get("width"))
        height = float(bbox.get("height"))
    except (TypeError, ValueError):
        return JSONResponse({"error": "Invalid calibration payload."}, status_code=400)

    if width <= 0 or height <= 0:
        return JSONResponse({"error": "Bounding box must have positive size."}, status_code=400)

    calibration = {
        "timestamp_sec": round(timestamp_sec, 2),
        "bbox": {
            "x": max(0.0, min(x, 1.0)),
            "y": max(0.0, min(y, 1.0)),
            "width": max(0.001, min(width, 1.0)),
            "height": max(0.001, min(height, 1.0)),
        },
    }
    save_json(calibration_path(review_id), calibration)
    focus_dir = REVIEWS_DIR / review_id / "focus_frames"
    if focus_dir.exists():
        shutil.rmtree(focus_dir)
    return JSONResponse({"ok": True, "calibration": calibration})


@router.get("/api/reviews/{review_id}/calibration-frame", response_model=None)
async def calibration_frame(review_id: str, ts: str = "0") -> Response:
    review = load_json(review_path(review_id))
    if not review:
        return JSONResponse({"error": "review not found"}, status_code=404)
    try:
        timestamp_sec = float(ts or 0)
    except ValueError:
        return JSONResponse({"error": "Invalid timestamp."}, status_code=400)

    calibration_dir = REVIEWS_DIR / review_id / "calibration"
    frame_path = calibration_dir / f"frame_{int(timestamp_sec * 100):08d}.jpg"
    if not frame_path.exists():
        extract_frame_at_timestamp(review["video_url"], timestamp_sec, frame_path)
    return FileResponse(frame_path, media_type="image/jpeg")


@router.get("/api/reviews/{review_id}/markers/{marker_id}/focus-frame")
async def marker_focus_frame(request: Request, review_id: str, marker_id: int) -> JSONResponse:
    review = load_json(review_path(review_id))
    if not review:
        return JSONResponse({"error": "review not found"}, status_code=404)

    review = normalize_review(review)
    marker = next((item for item in review.get("markers", []) if int(item.get("id", -1)) == marker_id), None)
    if not marker:
        return JSONResponse({"error": "marker not found"}, status_code=404)

    focus_dir = REVIEWS_DIR / review_id / "focus_frames"
    raw_path = focus_dir / f"marker_{marker_id:02d}_raw.jpg"
    final_path = focus_dir / f"marker_{marker_id:02d}_focus.jpg"
    meta_path = focus_dir / f"marker_{marker_id:02d}_focus.json"

    calibration = load_json(calibration_path(review_id))
    if not calibration:
        return JSONResponse(
            {
                "error": "Player calibration is required first. Open the calibration page and select the player.",
            },
            status_code=400,
        )

    if not final_path.exists():
        extract_frame_at_timestamp(
            review["video_url"],
            float(marker["timestamp_sec"]),
            raw_path,
        )
        calibration_dir = REVIEWS_DIR / review_id / "calibration"
        reference_frame_path = calibration_dir / f"frame_{int(float(calibration['timestamp_sec']) * 100):08d}.jpg"
        if not reference_frame_path.exists():
            extract_frame_at_timestamp(review["video_url"], float(calibration["timestamp_sec"]), reference_frame_path)
        patch_path = calibration_dir / "reference_patch.jpg"
        crop_reference_patch(
            image_path=reference_frame_path,
            bbox=calibration["bbox"],
            output_path=patch_path,
        )
        localization = locate_patch_in_frame(frame_path=raw_path, patch_path=patch_path)
        draw_player_circle(
            image_path=raw_path,
            output_path=final_path,
            center_x=float(localization["center_x"]),
            center_y=float(localization["center_y"]),
            radius=float(localization["radius"]),
            label=marker.get("label", "Marker"),
            note=localization["note"],
            found=bool(localization["found"]),
        )
        save_json(
            meta_path,
            {
                "found": localization["found"],
                "confidence": localization["confidence"],
                "note": localization["note"],
                "score": localization["score"],
                "image_url": public_url_for(
                    request, "marker_focus_frame_image", review_id=review_id, marker_id=marker_id
                ),
            },
        )

    meta = load_json(meta_path) or {
        "found": True,
        "confidence": "low",
        "note": "",
        "image_url": public_url_for(
            request, "marker_focus_frame_image", review_id=review_id, marker_id=marker_id
        ),
    }
    return JSONResponse(meta)


@router.get(
    "/api/reviews/{review_id}/markers/{marker_id}/focus-frame/image",
    name="marker_focus_frame_image",
)
async def marker_focus_frame_image(review_id: str, marker_id: int) -> FileResponse:
    final_path = REVIEWS_DIR / review_id / "focus_frames" / f"marker_{marker_id:02d}_focus.jpg"
    if not final_path.exists():
        raise HTTPException(status_code=404)
    return FileResponse(final_path, media_type="image/jpeg")


@router.get("/feedback-agent/health")
async def feedback_health() -> JSONResponse:
    from yolo_model.runtime_health import check_yolo_runtime

    yolo = check_yolo_runtime()
    return JSONResponse(
        {
            "ok": True,
            "yolo": yolo,
            "pose_pipeline_ready": bool(yolo.get("ready_for_pose_pipeline")),
        }
    )

# Reverse-proxy compatibility when /api prefix is stripped upstream.


@router.api_route("/reviews", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def reviews_root_stripped(request: Request) -> Response:
    if request.method == "POST":
        return await create_review(request)
    return JSONResponse({"error": "method not allowed"}, status_code=405)


@router.api_route("/reviews/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def reviews_stripped(request: Request, path: str) -> Response:
    review_id = path.split("/", 1)[0]
    if request.method == "GET" and path == f"{review_id}/status":
        return await job_status(review_id)
    if request.method == "POST" and path == f"{review_id}/cancel":
        return await cancel_review_background(review_id)
    if request.method == "GET" and "/" not in path:
        return await review_data(review_id)
    return JSONResponse({"error": "not found"}, status_code=404)


@router.api_route("/pose-markers", methods=["POST", "OPTIONS"])
async def pose_markers_stripped(request: Request) -> Response:
    return await create_pose_markers(request)
