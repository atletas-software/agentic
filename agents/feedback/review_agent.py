from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from agents.feedback.models import VideoFeedbackReview
from agents.feedback.openai_service import analyze_storyboards
from agents.feedback.storage import DATA_DIR, save_json
from agents.feedback.video_utils import (
    create_focus_crops,
    create_storyboards,
    extract_circled_frames,
    extract_frames,
)

BASE_DIR = Path(__file__).resolve().parent


def build_review(
    *,
    review_id: str,
    video_url: str,
    sport: str,
    player_focus: str,
    analysis_scope: str,
    coaching_focus: str,
    player_memory_context: str | None = None,
    shared_context: str | None = None,
    player_memory_retrieval_debug: dict[str, Any] | None = None,
    shared_context_sheet_debug: dict[str, Any] | None = None,
) -> dict:
    base_dir = DATA_DIR / "reviews" / review_id
    frames_dir = base_dir / "frames"
    storyboards_dir = base_dir / "storyboards"
    focused_dir = base_dir / "focused_frames"

    duration_sec, circled_frames = extract_circled_frames(video_url, frames_dir)
    analysis_mode = "circled-player"
    if circled_frames:
        analysis_frames = create_focus_crops(circled_frames, focused_dir)
        allowed_timestamps = [frame.timestamp_sec for frame in analysis_frames if frame.circle_found]
    else:
        analysis_mode = "full-frame-fallback"
        duration_sec, analysis_frames = extract_frames(video_url, frames_dir)
        allowed_timestamps = []

    storyboards = create_storyboards(analysis_frames, storyboards_dir)
    prompt_text = (BASE_DIR / "video_feedback_agent_system_prompt.md").read_text(encoding="utf-8")

    review_payload, llm_debug = analyze_storyboards(
        prompt_text=prompt_text,
        sport=sport,
        player_focus=player_focus,
        duration_sec=duration_sec,
        analysis_scope=analysis_scope,
        coaching_focus=coaching_focus,
        storyboard_paths=storyboards,
        analysis_mode=analysis_mode,
        allowed_timestamps=allowed_timestamps,
        player_memory_context=player_memory_context,
        shared_context=shared_context,
    )

    review = _to_review_document(
        review_id=review_id,
        video_url=video_url,
        duration_sec=duration_sec,
        review_payload=review_payload,
        analysis_mode=analysis_mode,
        allowed_timestamps=allowed_timestamps,
    )
    review["generation_debug"] = {
        "analysis_kind": "video-storyboards",
        "openai": llm_debug,
        "shared_context_sheet": shared_context_sheet_debug,
        "player_memory_vector_retrieval": player_memory_retrieval_debug,
    }
    save_json(base_dir / "review.json", review)
    return review


def _to_review_document(
    *,
    review_id: str,
    video_url: str,
    duration_sec: float,
    review_payload: VideoFeedbackReview,
    analysis_mode: str,
    allowed_timestamps: list[float],
) -> dict:
    moments = sorted(review_payload.moments, key=lambda item: item.timestamp_sec)
    markers = []
    marker_index = 1
    used_times: set[float] = set()
    for moment in moments:
        clamped_time = round(max(0.0, min(float(moment.timestamp_sec), max(duration_sec - 0.25, 0.0))), 2)
        if allowed_timestamps:
            snapped_time = _nearest_allowed_timestamp(clamped_time, allowed_timestamps)
            if snapped_time is None:
                continue
            clamped_time = snapped_time
            if clamped_time in used_times:
                continue
            used_times.add(clamped_time)
        markers.append(
            {
                "id": marker_index,
                "timestamp_sec": clamped_time,
                "category": moment.category,
                "sentiment": moment.sentiment,
                "label": _marker_label(moment.category, moment.sentiment),
                "coaching_note": moment.coaching_note,
                "reference_clip": moment.reference_clip.model_dump() if moment.reference_clip else None,
                "diagram_request": moment.diagram_request.model_dump() if moment.diagram_request else None,
                "freeze_frame_request": moment.freeze_frame_request.model_dump() if moment.freeze_frame_request else None,
            }
        )
        marker_index += 1

    return {
        "id": review_id,
        "title": review_payload.video_summary.player_focus or "Video Review",
        "video_url": video_url,
        "duration_sec": round(duration_sec, 2),
        "analysis_mode": analysis_mode,
        "allowed_timestamps": [round(timestamp, 2) for timestamp in allowed_timestamps],
        "video_summary": review_payload.video_summary.model_dump(),
        "overall_assessment": review_payload.overall_assessment.model_dump(),
        "markers": markers,
    }


def _marker_label(category: str, sentiment: str) -> str:
    prefix = {
        "positive": "Positive",
        "corrective": "Improve",
        "mixed": "Review",
    }.get(sentiment, "Review")
    return f"{prefix}: {category.replace('_', ' ').title()}"


def _nearest_allowed_timestamp(timestamp: float, allowed_timestamps: list[float]) -> Optional[float]:
    if not allowed_timestamps:
        return None
    nearest = min(allowed_timestamps, key=lambda allowed: abs(allowed - timestamp))
    # The model sees storyboard labels, so anything far from a detected circle is not a valid marker.
    if abs(nearest - timestamp) > 20.0:
        return None
    return round(nearest, 2)
