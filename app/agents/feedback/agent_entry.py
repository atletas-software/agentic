"""Feedback agent v1 — current OpenAI + YOLO implementation.

Registered as ``v1`` (and alias ``default``). Backend selects via
``FEEDBACK_AGENT_VERSION`` (see ``agents.registry``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from agents.feedback.storage import DATA_DIR, ensure_directories, load_json, review_cancel_requested, save_json
from agents.registry import register_feedback_agent

REVIEWS_DIR = DATA_DIR / "reviews"


def coaching_focus_from_payload(payload: dict[str, Any]) -> str:
    return str(payload.get("coaching_prompt") or payload.get("coaching_focus") or "").strip()


def review_job_path(review_id: str) -> Path:
    return REVIEWS_DIR / review_id / "job.json"


def review_json_path(review_id: str) -> Path:
    return REVIEWS_DIR / review_id / "review.json"


def request_review_cancel(review_id: str) -> None:
    rid = (review_id or "").strip()
    if not rid:
        return
    path = REVIEWS_DIR / rid / "cancel_requested"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("1", encoding="utf-8")


def load_review_job(review_id: str) -> dict[str, Any] | None:
    return load_json(review_job_path(review_id))


def load_review_json(review_id: str) -> dict[str, Any] | None:
    return load_json(review_json_path(review_id))


def _save_job(review_id: str, payload: dict[str, Any]) -> None:
    save_json(review_job_path(review_id), payload)


def _merge_job_progress(
    review_id: str,
    patch: dict[str, Any],
    on_progress: Callable[[dict[str, Any]], None] | None,
) -> None:
    job = load_review_job(review_id) or {"id": review_id}
    job.update(patch)
    job.setdefault("status", "running")
    _save_job(review_id, job)
    if on_progress is not None:
        on_progress(dict(job))


def _cancelled(cancel_check: Callable[[], bool] | None, review_id: str) -> bool:
    if cancel_check is not None and cancel_check():
        request_review_cancel(review_id)
        return True
    return review_cancel_requested(review_id)


def _run_text_coaching(
    review_id: str,
    payload: dict[str, Any],
    *,
    on_progress: Callable[[dict[str, Any]], None] | None,
    cancel_check: Callable[[], bool] | None,
) -> dict[str, Any]:
    from agents.feedback.openai_service import generate_text_coaching_review

    _merge_job_progress(
        review_id,
        {
            "status": "running",
            "video_url": (payload.get("video_url") or "").strip(),
            "player_focus": payload.get("player_focus", ""),
            "sport": payload.get("sport", "Soccer"),
            "mode": "text-only",
            "error": None,
        },
        on_progress,
    )
    if _cancelled(cancel_check, review_id):
        raise RuntimeError("Cancelled by user.")

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
    review: dict[str, Any] = {
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
    save_json(review_json_path(review_id), review)
    return review


def _run_pose_review(
    review_id: str,
    payload: dict[str, Any],
    *,
    on_progress: Callable[[dict[str, Any]], None] | None,
    cancel_check: Callable[[], bool] | None,
) -> dict[str, Any]:
    from agents.feedback.review_agent import build_review_from_pose_json

    _merge_job_progress(
        review_id,
        {
            "status": "running",
            "video_url": payload.get("video_url", ""),
            "player_focus": payload.get("player_focus", ""),
            "sport": payload.get("sport", "Soccer"),
            "mode": "pose-pipeline",
            "error": None,
        },
        on_progress,
    )

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
            _merge_job_progress(
                review_id,
                {"phase": "pose_remote", "progress_detail": "Remote YOLO pose job — this can take many minutes…"},
                on_progress,
            )
        pose_data = resolve_pose_data_for_video(
            video_url,
            job_key=f"review_{review_id}",
            on_progress=lambda p: _merge_job_progress(review_id, p, on_progress),
            cancel_check=lambda: _cancelled(cancel_check, review_id),
        )

    return build_review_from_pose_json(
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
        on_progress=lambda p: _merge_job_progress(review_id, p, on_progress),
        cancel_check=lambda: _cancelled(cancel_check, review_id),
    )


def _run_standard_review(
    review_id: str,
    payload: dict[str, Any],
    *,
    on_progress: Callable[[dict[str, Any]], None] | None,
    cancel_check: Callable[[], bool] | None,
) -> dict[str, Any]:
    from agents.feedback.review_agent import build_review

    detector = (payload.get("highlight_detector") or "").strip().lower()
    mode = "openai-video" if detector in {"openai_video", "video", "direct_video"} else (
        "openai-moments" if detector in {"openai", "gpt", "vision"} else "yolo-highlight"
    )
    _merge_job_progress(
        review_id,
        {
            "status": "running",
            "video_url": payload.get("video_url", ""),
            "player_focus": payload.get("player_focus", ""),
            "sport": payload.get("sport", "Soccer"),
            "mode": mode,
            "error": None,
        },
        on_progress,
    )
    return build_review(
        review_id=review_id,
        video_url=str(payload.get("video_url") or ""),
        sport=payload.get("sport", "Soccer"),
        player_focus=payload.get("player_focus", "Unknown player"),
        analysis_scope=payload.get("analysis_scope", ""),
        coaching_focus=coaching_focus_from_payload(payload),
        player_memory_context=(payload.get("player_memory_context") or "").strip() or None,
        shared_context=(payload.get("shared_context") or "").strip() or None,
        player_memory_retrieval_debug=payload.get("player_memory_retrieval_debug"),
        shared_context_sheet_debug=payload.get("shared_context_sheet_debug"),
        on_progress=lambda p: _merge_job_progress(review_id, p, on_progress),
        cancel_check=lambda: _cancelled(cancel_check, review_id),
        player_first_name=(payload.get("first_name") or "").strip() or None,
        player_last_name=(payload.get("last_name") or "").strip() or None,
        highlight_detector=detector or None,
    )


class FeedbackAgentV1:
    """Current production agent: YOLO highlight / pose + OpenAI coaching."""

    version = "v1"

    def run_review(
        self,
        review_id: str,
        payload: dict[str, Any],
        *,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        ensure_directories()
        (REVIEWS_DIR / review_id).mkdir(parents=True, exist_ok=True)

        text_only = bool(payload.get("text_only"))
        use_pose = bool(payload.get("pose_json_path")) or bool(payload.get("use_pose_pipeline"))

        try:
            if text_only:
                review = _run_text_coaching(
                    review_id, payload, on_progress=on_progress, cancel_check=cancel_check
                )
            elif use_pose:
                review = _run_pose_review(
                    review_id, payload, on_progress=on_progress, cancel_check=cancel_check
                )
            else:
                review = _run_standard_review(
                    review_id, payload, on_progress=on_progress, cancel_check=cancel_check
                )
        except Exception as exc:
            job = load_review_job(review_id) or {"id": review_id}
            job["status"] = "failed"
            job["error"] = str(exc)
            _save_job(review_id, job)
            if on_progress is not None:
                on_progress(dict(job))
            raise

        _merge_job_progress(
            review_id,
            {
                "status": "completed",
                "error": None,
                "review_title": review.get("title"),
                "feedback_agent_version": self.version,
            },
            on_progress,
        )
        return review


def register() -> None:
    register_feedback_agent("v1", FeedbackAgentV1)
    register_feedback_agent("default", FeedbackAgentV1)
