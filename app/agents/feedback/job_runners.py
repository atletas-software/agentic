"""Background review job runners (used by standalone agent and platform API routes)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from agents.feedback.openai_service import generate_text_coaching_review
from agents.feedback.review_agent import build_review, build_review_from_pose_json
from agents.feedback.storage import DATA_DIR, load_json, review_cancel_requested, save_json
from agents.feedback.urls import public_review_url

REVIEWS_DIR = DATA_DIR / "reviews"


def job_path(review_id: str) -> Path:
    return REVIEWS_DIR / review_id / "job.json"


def review_path(review_id: str) -> Path:
    return REVIEWS_DIR / review_id / "review.json"


def calibration_path(review_id: str) -> Path:
    return REVIEWS_DIR / review_id / "calibration.json"


def save_job(review_id: str, payload: Dict[str, Any]) -> None:
    save_json(job_path(review_id), payload)


def load_job(review_id: str) -> Optional[Dict[str, Any]]:
    return load_json(job_path(review_id))


def merge_job_progress(review_id: str, patch: Dict[str, Any]) -> None:
    job = load_job(review_id) or {"id": review_id}
    job.update(patch)
    job.setdefault("status", "running")
    save_job(review_id, job)


def coaching_focus_from_payload(payload: Dict[str, Any]) -> str:
    return str(payload.get("coaching_prompt") or payload.get("coaching_focus") or "").strip()


def run_text_coaching_job(review_id: str, payload: Dict[str, Any]) -> None:
    save_job(
        review_id,
        {
            "id": review_id,
            "status": "running",
            "video_url": (payload.get("video_url") or "").strip(),
            "player_focus": payload.get("player_focus", ""),
            "sport": payload.get("sport", "Soccer"),
            "mode": "text-only",
            "error": None,
        },
    )
    try:
        parsed, llm_debug = generate_text_coaching_review(
            sport=str(payload.get("sport") or "Soccer"),
            player_focus=str(payload.get("player_focus") or "Unknown player"),
            analysis_scope=str(payload.get("analysis_scope") or ""),
            coaching_focus=coaching_focus_from_payload(payload),
            video_link_for_reference=str(payload.get("video_url") or ""),
            player_memory_context=(payload.get("player_memory_context") or "").strip() or None,
            shared_context=(payload.get("shared_context") or "").strip() or None,
        )
        player_focus = str(payload.get("player_focus") or "Player").strip() or "Player"
        review: Dict[str, Any] = {
            "id": review_id,
            "title": f"Coaching notes — {player_focus}",
            "video_url": str(payload.get("video_url") or "").strip(),
            "duration_sec": 0.0,
            "analysis_mode": "text-only",
            "allowed_timestamps": [],
            "video_summary": {
                "sport": str(payload.get("sport") or "Soccer"),
                "player_focus": player_focus,
                "duration_sec": 0.0,
                "analysis_scope": str(payload.get("analysis_scope") or ""),
            },
            "overall_assessment": {
                "strengths": list(parsed.strengths or []),
                "improvements": list(parsed.improvements or []),
                "next_focus": list(parsed.next_focus or []),
            },
            "markers": [],
            "coach_narrative": parsed.coach_letter,
            "generation_debug": {
                "analysis_kind": "text-only",
                "openai": llm_debug,
                "shared_context_sheet": payload.get("shared_context_sheet_debug"),
                "player_memory_vector_retrieval": payload.get("player_memory_retrieval_debug"),
            },
        }
        save_json(review_path(review_id), review)
    except Exception as exc:  # noqa: BLE001
        job = load_job(review_id) or {"id": review_id}
        job["status"] = "failed"
        job["error"] = str(exc)
        save_job(review_id, job)
        return

    review_url = public_review_url(review_id)
    job = load_job(review_id) or {"id": review_id}
    job["status"] = "completed"
    job["error"] = None
    job["review_url"] = review_url
    job["review_title"] = review["title"]
    for k in ("phase", "progress_detail", "probe_current", "probe_estimate", "segment_current", "segment_total"):
        job.pop(k, None)
    save_job(review_id, job)


def run_pose_review_job(review_id: str, payload: Dict[str, Any]) -> None:
    save_job(
        review_id,
        {
            "id": review_id,
            "status": "running",
            "video_url": payload.get("video_url", ""),
            "player_focus": payload.get("player_focus", ""),
            "sport": payload.get("sport", "Soccer"),
            "mode": "pose-pipeline",
            "error": None,
        },
    )
    try:
        pose_json_path = (payload.get("pose_json_path") or "").strip()
        video_url = (payload.get("video_url") or "").strip()
        inline_pose = payload.get("pose_json")
        if isinstance(inline_pose, dict) and inline_pose:
            pose_data = inline_pose
        elif pose_json_path:
            from yolo_model.pose_feedback.engine import load_pose_json

            pose_data = load_pose_json(Path(pose_json_path))
        else:
            if not video_url:
                raise ValueError("pose_json_path, pose_json, or video_url is required for pose pipeline review")
            from yolo_model.pose_api.client import pose_api_configured, resolve_pose_data_for_video

            if pose_api_configured():
                merge_job_progress(
                    review_id,
                    {
                        "phase": "pose_remote",
                        "progress_detail": "Remote YOLO pose job (RunPod) — this can take many minutes…",
                    },
                )
            pose_data = resolve_pose_data_for_video(
                video_url,
                job_key=f"review_{review_id}",
                on_progress=lambda p: merge_job_progress(review_id, p),
                cancel_check=lambda: review_cancel_requested(review_id),
            )
        review = build_review_from_pose_json(
            review_id=review_id,
            video_url=video_url or str(pose_data.get("video") or ""),
            pose_data=pose_data,
            sport=payload.get("sport", "Soccer"),
            player_focus=payload.get("player_focus", "Unknown player"),
            analysis_scope=payload.get("analysis_scope", ""),
            coaching_focus=coaching_focus_from_payload(payload),
            player_memory_context=(payload.get("player_memory_context") or "").strip() or None,
            shared_context=(payload.get("shared_context") or "").strip() or None,
            player_memory_retrieval_debug=payload.get("player_memory_retrieval_debug"),
            shared_context_sheet_debug=payload.get("shared_context_sheet_debug"),
            on_progress=lambda p: merge_job_progress(review_id, p),
            cancel_check=lambda: review_cancel_requested(review_id),
        )
    except Exception as exc:  # noqa: BLE001
        job = load_job(review_id) or {"id": review_id}
        job["status"] = "failed"
        job["error"] = str(exc)
        save_job(review_id, job)
        return

    review_url = public_review_url(review_id)
    job = load_job(review_id) or {"id": review_id}
    job["status"] = "completed"
    job["error"] = None
    job["review_url"] = review_url
    job["review_title"] = review["title"]
    for k in ("phase", "progress_detail", "probe_current", "probe_estimate", "segment_current", "segment_total"):
        job.pop(k, None)
    save_job(review_id, job)


def run_review_job(review_id: str, payload: Dict[str, Any]) -> None:
    save_job(
        review_id,
        {
            "id": review_id,
            "status": "running",
            "video_url": payload["video_url"],
            "player_focus": payload.get("player_focus", ""),
            "sport": payload.get("sport", "Soccer"),
            "mode": "openai-moments"
            if (payload.get("highlight_detector") or "").strip().lower() in {"openai", "gpt", "vision"}
            else "yolo-highlight",
            "error": None,
        },
    )
    try:
        review = build_review(
            review_id=review_id,
            video_url=payload["video_url"],
            sport=payload.get("sport", "Soccer"),
            player_focus=payload.get("player_focus", "Unknown player"),
            analysis_scope=payload.get("analysis_scope", ""),
            coaching_focus=coaching_focus_from_payload(payload),
            player_memory_context=(payload.get("player_memory_context") or "").strip() or None,
            shared_context=(payload.get("shared_context") or "").strip() or None,
            player_memory_retrieval_debug=payload.get("player_memory_retrieval_debug"),
            shared_context_sheet_debug=payload.get("shared_context_sheet_debug"),
            on_progress=lambda p: merge_job_progress(review_id, p),
            cancel_check=lambda: review_cancel_requested(review_id),
            player_first_name=(payload.get("first_name") or "").strip() or None,
            player_last_name=(payload.get("last_name") or "").strip() or None,
            highlight_detector=(payload.get("highlight_detector") or "").strip() or None,
        )
    except Exception as exc:  # noqa: BLE001
        job = load_job(review_id) or {"id": review_id}
        job["status"] = "failed"
        job["error"] = str(exc)
        save_job(review_id, job)
        return

    review_url = public_review_url(review_id)
    job = load_job(review_id) or {"id": review_id}
    job["status"] = "completed"
    job["error"] = None
    job["review_url"] = review_url
    job["review_title"] = review["title"]
    for k in ("phase", "progress_detail", "probe_current", "probe_estimate", "segment_current", "segment_total"):
        job.pop(k, None)
    save_job(review_id, job)
