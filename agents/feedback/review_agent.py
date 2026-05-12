from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from agents.feedback.models import VideoFeedbackReview
from agents.feedback.openai_service import analyze_storyboards, summarize_highlight_windows_for_feedback
from agents.feedback.storage import DATA_DIR, save_json
from agents.feedback.video_utils import (
    create_focus_crops,
    create_storyboards,
    enrich_highlight_temporal_context,
    extract_circled_frames,
    extract_frames,
    group_frames_by_highlight_anchor,
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
        ctx_win = float((os.getenv("VIDEO_HIGHLIGHT_CONTEXT_WINDOW_SEC") or "2").strip() or "0")
        ctx_max = int((os.getenv("VIDEO_HIGHLIGHT_CONTEXT_MAX_FRAMES") or "48").strip() or "48")
        if ctx_win > 0 and analysis_frames:
            ctx_dir = base_dir / "temporal_context"
            analysis_frames = enrich_highlight_temporal_context(
                video_url,
                analysis_frames,
                duration_sec,
                ctx_dir,
                window_sec=ctx_win,
                step_sec=1.0,
                max_frames=max(12, ctx_max),
            )
    else:
        analysis_mode = "full-frame-fallback"
        duration_sec, analysis_frames = extract_frames(video_url, frames_dir)
        allowed_timestamps = []

    storyboards = create_storyboards(analysis_frames, storyboards_dir)
    prompt_text = (BASE_DIR / "video_feedback_agent_system_prompt.md").read_text(encoding="utf-8")

    highlight_text = ""
    if analysis_mode != "circled-player":
        cap_debug: dict[str, Any] = {"outcome": "skipped", "reason": "full_frame_fallback"}
    elif not any(f.circle_found for f in analysis_frames):
        cap_debug = {"outcome": "skipped", "reason": "no_highlight_circles_in_assets"}
    else:
        summary_flag = (os.getenv("VIDEO_HIGHLIGHT_TEXT_SUMMARY") or "true").strip().lower()
        if summary_flag in {"0", "false", "no", "off"}:
            cap_debug = {"outcome": "skipped", "reason": "VIDEO_HIGHLIGHT_TEXT_SUMMARY_disabled"}
        else:
            wsec = float((os.getenv("VIDEO_HIGHLIGHT_CONTEXT_WINDOW_SEC") or "2").strip() or "2")
            groups = group_frames_by_highlight_anchor(analysis_frames, window_sec=max(wsec, 0.5))
            highlight_text, cap_debug = summarize_highlight_windows_for_feedback(
                windows=groups,
                player_focus=player_focus,
                sport=sport,
                window_sec=wsec if wsec > 0 else 2.0,
                max_windows=int((os.getenv("VIDEO_HIGHLIGHT_SUMMARY_MAX_WINDOWS") or "8").strip() or "8"),
                max_images_per_window=int((os.getenv("VIDEO_HIGHLIGHT_SUMMARY_MAX_IMAGES_PER_WINDOW") or "5").strip() or "5"),
                max_images_total=int((os.getenv("VIDEO_HIGHLIGHT_SUMMARY_MAX_IMAGES") or "24").strip() or "24"),
            )

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
        highlight_window_narrative=highlight_text or None,
    )

    review = _to_review_document(
        review_id=review_id,
        video_url=video_url,
        duration_sec=duration_sec,
        review_payload=review_payload,
        analysis_mode=analysis_mode,
        allowed_timestamps=allowed_timestamps,
    )

    ctx_win_dbg = float((os.getenv("VIDEO_HIGHLIGHT_CONTEXT_WINDOW_SEC") or "2").strip() or "0")
    ctx_max_dbg = int((os.getenv("VIDEO_HIGHLIGHT_CONTEXT_MAX_FRAMES") or "48").strip() or "48")
    video_pre: dict[str, Any] = {
        "tactical_pipeline_spec": "agents/feedback/tactical_pipeline.py (roadmap only)",
        "highlight_context_window_sec": ctx_win_dbg if circled_frames else 0.0,
        "highlight_context_max_frames": ctx_max_dbg if circled_frames else 0,
        "frames_sent_to_storyboard": len(analysis_frames),
        "storyboard_pages": len(storyboards),
    }

    review["generation_debug"] = {
        "analysis_kind": "video-storyboards",
        "openai": llm_debug,
        "shared_context_sheet": shared_context_sheet_debug,
        "player_memory_vector_retrieval": player_memory_retrieval_debug,
        "video_preprocess": video_pre,
        "video_highlight_captions": cap_debug,
    }
    review["video_context"] = {
        "description": (
            "Red-circle scan → focus crops → optional T±window neighbor frames → storyboard pages for the main model; "
            "optional second vision model summarizes each highlight window (combined text in the main prompt)."
        ),
        "combined_highlight_text": (highlight_text or "")[:60_000],
        "preprocess": video_pre,
        "highlight_caption_pass": cap_debug,
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
