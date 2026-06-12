from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any, Optional

from agents.feedback.models import ReviewMoment, VideoFeedbackReview, VideoSummary
from agents.feedback.openai_service import (
    analyze_storyboards,
    summarize_highlight_windows_for_feedback,
    synthesize_overall_from_circle_segments,
    vision_analyze_circle_segment,
)
from agents.feedback.storage import DATA_DIR, save_json
from agents.feedback.video_utils import (
    circle_visibility_segments_from_probes,
    create_focus_crops,
    create_storyboards,
    enrich_highlight_temporal_context,
    extract_circled_frames,
    extract_frames,
    extract_uniform_frames_in_range,
    FrameAsset,
    group_frames_by_highlight_anchor,
    probe_circle_timeline,
    probe_duration,
)
# YOLO pipeline lives in a separate package so a missing weights file / missing
# ultralytics install never breaks startup of the legacy code path.
try:
    from agents.feedback.highlight import (
        HighlightEvent,
        PipelineResult,
        run_yolo_pipeline,
    )
    from agents.feedback.highlight.yolo_detector import HighlightDetectorUnavailable
except Exception as _yolo_import_exc:  # noqa: BLE001 — defensive: any import error means "no yolo"
    HighlightEvent = None  # type: ignore[assignment]
    PipelineResult = None  # type: ignore[assignment]
    run_yolo_pipeline = None  # type: ignore[assignment]

    class HighlightDetectorUnavailable(RuntimeError):  # type: ignore[no-redef]
        pass

    _YOLO_IMPORT_ERROR: Exception | None = _yolo_import_exc
else:
    _YOLO_IMPORT_ERROR = None

BASE_DIR = Path(__file__).resolve().parent


def _raise_if_cancelled(cancel_check: Optional[Callable[[], bool]]) -> None:
    if cancel_check is not None and cancel_check():
        raise RuntimeError("Review cancelled by user")


def _truthy_env(name: str, default: str = "true") -> bool:
    v = (os.getenv(name) or default).strip().lower()
    return v not in {"0", "false", "no", "off"}


def _merge_frame_assets_by_path(assets: list[FrameAsset]) -> list[FrameAsset]:
    m: dict[Path, FrameAsset] = {}
    for a in assets:
        m[a.image_path] = a
    return sorted(m.values(), key=lambda x: x.timestamp_sec)


def _subsample_frame_assets(assets: list[FrameAsset], max_n: int) -> list[FrameAsset]:
    if max_n <= 0 or len(assets) <= max_n:
        return list(assets)
    n = len(assets)
    idxs = [round(i * (n - 1) / max(max_n - 1, 1)) for i in range(max_n)]
    picked: list[FrameAsset] = []
    seen: set[Path] = set()
    for i in idxs:
        a = assets[int(i)]
        if a.image_path in seen:
            continue
        seen.add(a.image_path)
        picked.append(a)
    return picked


def _selected_detector() -> str:
    raw = (os.getenv("VIDEO_HIGHLIGHT_DETECTOR") or "yolo").strip().lower()
    if raw in {"yolo", "yolov8"}:
        return "yolo"
    return "hsv"


def _try_yolo_segment_review(
    *,
    review_id: str,
    video_url: str,
    base_dir: Path,
    storyboards_dir: Path,
    sport: str,
    player_focus: str,
    analysis_scope: str,
    coaching_focus: str,
    player_memory_context: str | None,
    shared_context: str | None,
    player_memory_retrieval_debug: dict[str, Any] | None,
    shared_context_sheet_debug: dict[str, Any] | None,
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> dict | None:
    """YOLO highlight pipeline → per-event vision + overall synthesis.

    Returns ``None`` if the YOLO detector is unavailable (so the caller falls
    back to the legacy HSV path) or if no events were found.
    """
    if run_yolo_pipeline is None:
        return None
    try:
        pipeline = run_yolo_pipeline(
            video_url=video_url,
            base_dir=base_dir,
            on_progress=on_progress,
            cancel_check=cancel_check,
        )
    except HighlightDetectorUnavailable as exc:
        if on_progress:
            on_progress(
                {
                    "phase": "highlight_yolo_unavailable",
                    "progress_detail": f"YOLO detector unavailable: {exc}. Falling back to HSV.",
                }
            )
        return None

    duration_sec = float(pipeline.duration_sec)
    if not pipeline.events:
        return None

    if on_progress:
        on_progress(
            {
                "phase": "highlight_yolo_vision",
                "segment_total": len(pipeline.events),
                "segment_current": 0,
                "progress_detail": (
                    f"Running vision model on {len(pipeline.events)} event window(s)…"
                ),
            }
        )

    vision_debug: list[dict[str, Any]] = []
    episode_blocks: list[str] = []
    moments: list[ReviewMoment] = []
    used_ts: set[float] = set()
    storyboard_frame_assets: list[FrameAsset] = []
    segment_meta: list[dict[str, Any]] = []

    for asset_record in pipeline.events:
        _raise_if_cancelled(cancel_check)
        ev = asset_record.event
        idx = ev.index
        total = len(pipeline.events)

        if on_progress:
            on_progress(
                {
                    "phase": "highlight_yolo_vision",
                    "segment_total": total,
                    "segment_current": idx,
                    "progress_detail": (
                        f"Event {idx}/{total}: calling vision model on "
                        f"{len(asset_record.frame_paths)} frame(s)…"
                    ),
                }
            )

        if not asset_record.frame_paths:
            vision_debug.append(
                {
                    "event_index": idx,
                    "outcome": "skipped",
                    "reason": "no_frames",
                    "t_on": ev.t_on,
                    "t_off": ev.t_off,
                }
            )
            continue

        vis_out, vdbg = vision_analyze_circle_segment(
            frame_paths=asset_record.frame_paths,
            t_lo=ev.t_lo,
            t_on=ev.t_on,
            t_off=ev.t_off,
            t_hi=ev.t_hi,
            sport=sport,
            player_focus=player_focus,
            segment_index=idx,
            segment_total=total,
            player_crop_paths=asset_record.player_crop_paths,
            coaching_focus=coaching_focus,
            player_memory_context=player_memory_context,
            shared_context=shared_context,
        )
        vdbg["event_index"] = idx
        vdbg["mean_conf"] = round(ev.mean_conf, 4)
        vdbg["peak_conf"] = round(ev.peak_conf, 4)
        vdbg["probe_count"] = ev.probe_count
        vdbg["clip_mode"] = asset_record.clip_mode
        vdbg["player_crop_count"] = len(asset_record.player_crop_paths)
        vision_debug.append(vdbg)

        episode_blocks.append(
            "\n".join(
                [
                    f"### Episode {idx}/{total}  anchor≈{ev.anchor_sec:.2f}s  "
                    f"circle≈{ev.t_on:.2f}s–{ev.t_off:.2f}s  "
                    f"(YOLO peak conf={ev.peak_conf:.2f}, probes={ev.probe_count})",
                    f"- pitch_location: {vis_out.pitch_location}",
                    f"- category: {vis_out.category}",
                    f"- sentiment: {vis_out.sentiment}",
                    f"- coaching: {vis_out.coaching_note}",
                ]
            )
        )

        anchor_ts = round(float(ev.anchor_sec), 2)
        while anchor_ts in used_ts:
            anchor_ts = round(anchor_ts + 0.05, 2)
        used_ts.add(anchor_ts)
        loc = (vis_out.pitch_location or "").strip()
        note = (vis_out.coaching_note or "").strip()
        if loc and loc.lower() not in note.lower():
            display_note = f"Where on the pitch: {loc}\n\n{note}"
        else:
            display_note = note
        moments.append(
            ReviewMoment(
                timestamp_sec=anchor_ts,
                category=vis_out.category,
                sentiment=vis_out.sentiment,
                coaching_note=display_note,
            )
        )

        for path, ts, bbox in zip(
            asset_record.frame_paths,
            asset_record.frame_timestamps,
            asset_record.frame_bboxes,
        ):
            storyboard_frame_assets.append(
                FrameAsset(
                    timestamp_sec=float(ts),
                    image_path=path,
                    circle_found=bool(bbox),
                    circle_center_x=float(bbox.get("x", 0.0) + bbox.get("w", 0.0) / 2.0) if bbox else 0.0,
                    circle_center_y=float(bbox.get("y", 0.0) + bbox.get("h", 0.0) / 2.0) if bbox else 0.0,
                    circle_radius=float(max(bbox.get("w", 0.0), bbox.get("h", 0.0)) / 2.0) if bbox else 0.0,
                )
            )

        segment_meta.append(
            {
                "index": idx,
                "t_on": ev.t_on,
                "t_off": ev.t_off,
                "t_lo": ev.t_lo,
                "t_hi": ev.t_hi,
                "anchor_sec": ev.anchor_sec,
                "mean_conf": round(ev.mean_conf, 4),
                "peak_conf": round(ev.peak_conf, 4),
                "probe_count": ev.probe_count,
                "frame_count": len(asset_record.frame_paths),
                "clip_mode": asset_record.clip_mode,
                "frames_dir": str(asset_record.frames_dir),
                "clip_path": str(asset_record.clip_path) if asset_record.clip_path else None,
                "meta_path": str(asset_record.meta_path) if asset_record.meta_path else None,
                "annotated_dir": str(asset_record.annotated_dir) if asset_record.annotated_dir else None,
                "player_crops_dir": (
                    str(asset_record.player_crops_dir) if asset_record.player_crops_dir else None
                ),
                "player_crop_count": len(asset_record.player_crop_paths),
            }
        )

    if not moments:
        return None

    moments.sort(key=lambda m: m.timestamp_sec)
    segments_markdown = "\n\n".join(episode_blocks)

    if on_progress:
        on_progress(
            {
                "phase": "circle_overall_synthesis",
                "segment_total": len(pipeline.events),
                "segment_current": len(pipeline.events),
                "progress_detail": "Building storyboards and synthesizing overall assessment…",
            }
        )

    merged_assets = _merge_frame_assets_by_path(storyboard_frame_assets)
    max_sb_frames = int((os.getenv("VIDEO_CIRCLE_OVERALL_MAX_FRAMES_FOR_STORYBOARD") or "48").strip() or "48")
    storyboard_source = _subsample_frame_assets(merged_assets, max_sb_frames)
    storyboards = create_storyboards(storyboard_source, storyboards_dir)

    _raise_if_cancelled(cancel_check)
    prompt_text = (BASE_DIR / "video_feedback_agent_system_prompt.md").read_text(encoding="utf-8")
    overall, overall_dbg = synthesize_overall_from_circle_segments(
        prompt_tone=prompt_text,
        sport=sport,
        player_focus=player_focus,
        duration_sec=duration_sec,
        analysis_scope=analysis_scope,
        coaching_focus=coaching_focus,
        segments_markdown=segments_markdown,
        storyboard_paths=storyboards,
        player_memory_context=player_memory_context,
        shared_context=shared_context,
    )

    review_payload = VideoFeedbackReview(
        video_summary=VideoSummary(
            sport=sport,
            player_focus=player_focus,
            duration_sec=duration_sec,
            analysis_scope=analysis_scope,
        ),
        overall_assessment=overall,
        moments=moments,
    )
    allowed_timestamps = [round(float(m.timestamp_sec), 2) for m in moments]

    cap_debug: dict[str, Any] = {
        "mode": "yolo-event-episodes",
        "outcome": "success",
        "event_count": len(pipeline.events),
        "segment_definitions": segment_meta,
    }
    cap_debug.update(pipeline.to_debug_dict())

    llm_debug: dict[str, Any] = {
        "analysis_kind": "yolo-event-episodes",
        "circle_segment_vision": vision_debug,
        "circle_segment_overall": overall_dbg,
    }

    review = _to_review_document(
        review_id=review_id,
        video_url=video_url,
        duration_sec=duration_sec,
        review_payload=review_payload,
        analysis_mode="yolo-event-episodes",
        allowed_timestamps=allowed_timestamps,
    )
    review["circle_segments"] = segment_meta
    review["events_index"] = str(pipeline.events_index_path) if pipeline.events_index_path else None

    video_pre: dict[str, Any] = {
        "tactical_pipeline_spec": "agents/feedback/highlight/pipeline.py (YOLO detector)",
        "detector": "yolo",
        "events": len(pipeline.events),
        "frames_in_event_windows": sum(len(a.frame_paths) for a in pipeline.events),
        "frames_sent_to_storyboard": len(storyboard_source),
        "storyboard_pages": len(storyboards),
        "video_cached": bool(pipeline.cached.cached),
    }

    review["generation_debug"] = {
        "analysis_kind": "yolo-event-episodes",
        "openai": llm_debug,
        "shared_context_sheet": shared_context_sheet_debug,
        "player_memory_vector_retrieval": player_memory_retrieval_debug,
        "video_preprocess": video_pre,
        "video_highlight_captions": cap_debug,
    }
    review["video_context"] = {
        "description": (
            "YOLO highlight detector finds the red-circled player → per-event wide field frames + "
            "player bbox crops → vision model infers pitch location and tactical coaching (no pose/keypoints). "
            "Each highlight span becomes one timeline marker aligned with PLAYER MEMORY coaching style."
        ),
        "combined_highlight_text": segments_markdown[:60_000],
        "preprocess": video_pre,
        "highlight_caption_pass": cap_debug,
    }
    save_json(base_dir / "review.json", review)
    return review


def _try_circle_segment_episode_review(
    *,
    review_id: str,
    video_url: str,
    base_dir: Path,
    storyboards_dir: Path,
    sport: str,
    player_focus: str,
    analysis_scope: str,
    coaching_focus: str,
    player_memory_context: str | None,
    shared_context: str | None,
    player_memory_retrieval_debug: dict[str, Any] | None,
    shared_context_sheet_debug: dict[str, Any] | None,
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
    player_first_name: str | None = None,
    player_last_name: str | None = None,
) -> dict | None:
    """
    Timeline probe → contiguous circle visibility → per-episode window [t_on−pad, t_off+pad]
    → vision → one marker per episode + overall synthesis. Returns None to use legacy path.
    """
    if _selected_detector() == "yolo":
        yolo_review = _try_yolo_segment_review(
            review_id=review_id,
            video_url=video_url,
            base_dir=base_dir,
            storyboards_dir=storyboards_dir,
            sport=sport,
            player_focus=player_focus,
            analysis_scope=analysis_scope,
            coaching_focus=coaching_focus,
            player_memory_context=player_memory_context,
            shared_context=shared_context,
            player_memory_retrieval_debug=player_memory_retrieval_debug,
            shared_context_sheet_debug=shared_context_sheet_debug,
            on_progress=on_progress,
            cancel_check=cancel_check,
        )
        if yolo_review is not None:
            return yolo_review
        # Fall through to legacy HSV path on YOLO unavailable / zero events.
        if on_progress:
            on_progress(
                {
                    "phase": "highlight_yolo_fallback",
                    "progress_detail": "Falling back to HSV timeline probe.",
                }
            )

    duration_sec = probe_duration(video_url)
    _raise_if_cancelled(cancel_check)
    if on_progress:
        on_progress(
            {
                "phase": "ffprobe",
                "progress_detail": f"Video duration {duration_sec:.1f}s — starting highlight timeline scan…",
            }
        )
    probe_interval = float((os.getenv("VIDEO_CIRCLE_PROBE_INTERVAL_SEC") or "0.25").strip() or "0.25")
    probes_dir = base_dir / "circle_probes"
    probes = probe_circle_timeline(
        video_url,
        duration_sec,
        probes_dir,
        interval_sec=probe_interval,
        on_progress=on_progress,
        cancel_check=cancel_check,
        player_first_name=player_first_name,
        player_last_name=player_last_name,
    )
    raw_segments = circle_visibility_segments_from_probes(probes)
    min_probes = int((os.getenv("VIDEO_CIRCLE_MIN_SEGMENT_PROBES") or "1").strip() or "1")
    segments = [s for s in raw_segments if s[3] >= min_probes]
    min_gap = float((os.getenv("VIDEO_CIRCLE_MIN_GAP_SEC") or "3").strip() or "3")
    if min_gap > 0 and len(segments) > 1:
        merged: list[tuple[float, float, float, int]] = [segments[0]]
        for cur in segments[1:]:
            prev_on, prev_off, prev_anchor, prev_n = merged[-1]
            if (float(cur[0]) - float(prev_off)) <= min_gap:
                new_on = prev_on
                new_off = max(prev_off, float(cur[1]))
                new_anchor = round((new_on + new_off) / 2.0, 2)
                merged[-1] = (new_on, new_off, new_anchor, prev_n + int(cur[3]))
            else:
                merged.append(cur)
        segments = merged
    max_segments = int((os.getenv("VIDEO_CIRCLE_MAX_SEGMENTS") or "40").strip() or "40")
    capped = False
    if len(segments) > max_segments > 0:
        segments = segments[:max_segments]
        capped = True

    if not segments:
        return None

    if on_progress:
        on_progress(
            {
                "phase": "circle_episodes",
                "segment_total": len(segments),
                "segment_current": 0,
                "progress_detail": f"Found {len(segments)} highlight episode(s). Analyzing each window…",
            }
        )

    pad = float((os.getenv("VIDEO_CIRCLE_CONTEXT_PAD_SEC") or "2").strip() or "2")
    per_seg_frames = int((os.getenv("VIDEO_CIRCLE_SEGMENT_FRAMES") or "12").strip() or "12")
    seg_fw = int((os.getenv("VIDEO_CIRCLE_SEGMENT_FRAME_WIDTH") or "720").strip() or "720")

    vision_debug: list[dict[str, Any]] = []
    episode_blocks: list[str] = []
    moments: list[ReviewMoment] = []
    all_window_assets: list[FrameAsset] = []
    used_ts: set[float] = set()

    for idx, (t_on, t_off, anchor, n_probes) in enumerate(segments, start=1):
        _raise_if_cancelled(cancel_check)
        if on_progress:
            on_progress(
                {
                    "phase": "circle_episode_vision",
                    "segment_total": len(segments),
                    "segment_current": idx,
                    "progress_detail": f"Episode {idx}/{len(segments)}: extracting frames and calling vision model…",
                }
            )
        t_lo = max(0.0, float(t_on) - pad)
        t_hi = min(float(duration_sec), float(t_off) + pad)
        seg_dir = base_dir / "segment_frames" / f"seg_{idx:02d}"
        assets = extract_uniform_frames_in_range(
            video_url,
            t_lo,
            t_hi,
            seg_dir,
            f"f{idx:02d}",
            max_frames=max(4, per_seg_frames),
            frame_width=seg_fw,
            video_duration_sec=duration_sec,
        )
        all_window_assets.extend(assets)
        paths = [a.image_path for a in assets]
        vis_out, vdbg = vision_analyze_circle_segment(
            frame_paths=paths,
            t_lo=t_lo,
            t_on=float(t_on),
            t_off=float(t_off),
            t_hi=t_hi,
            sport=sport,
            player_focus=player_focus,
            segment_index=idx,
            segment_total=len(segments),
            coaching_focus=coaching_focus,
            player_memory_context=player_memory_context,
            shared_context=shared_context,
        )
        vdbg["probe_count_in_run"] = n_probes
        vision_debug.append(vdbg)

        episode_blocks.append(
            "\n".join(
                [
                    f"### Episode {idx}/{len(segments)}  anchor≈{anchor:.2f}s  "
                    f"circle≈{t_on:.2f}s–{t_off:.2f}s  (probes_in_run={n_probes})",
                    f"- category: {vis_out.category}",
                    f"- sentiment: {vis_out.sentiment}",
                    f"- coaching: {vis_out.coaching_note}",
                ]
            )
        )

        ts = round(float(anchor), 2)
        while ts in used_ts:
            ts = round(ts + 0.05, 2)
        used_ts.add(ts)
        moments.append(
            ReviewMoment(
                timestamp_sec=ts,
                category=vis_out.category,
                sentiment=vis_out.sentiment,
                coaching_note=vis_out.coaching_note,
            )
        )

    moments.sort(key=lambda m: m.timestamp_sec)
    segments_markdown = "\n\n".join(episode_blocks)

    if on_progress:
        on_progress(
            {
                "phase": "circle_overall_synthesis",
                "segment_total": len(segments),
                "segment_current": len(segments),
                "progress_detail": "Building storyboards and synthesizing overall assessment…",
            }
        )

    merged_assets = _merge_frame_assets_by_path(all_window_assets)
    max_sb_frames = int((os.getenv("VIDEO_CIRCLE_OVERALL_MAX_FRAMES_FOR_STORYBOARD") or "48").strip() or "48")
    storyboard_source = _subsample_frame_assets(merged_assets, max_sb_frames)
    storyboards = create_storyboards(storyboard_source, storyboards_dir)

    _raise_if_cancelled(cancel_check)
    prompt_text = (BASE_DIR / "video_feedback_agent_system_prompt.md").read_text(encoding="utf-8")
    overall, overall_dbg = synthesize_overall_from_circle_segments(
        prompt_tone=prompt_text,
        sport=sport,
        player_focus=player_focus,
        duration_sec=duration_sec,
        analysis_scope=analysis_scope,
        coaching_focus=coaching_focus,
        segments_markdown=segments_markdown,
        storyboard_paths=storyboards,
        player_memory_context=player_memory_context,
        shared_context=shared_context,
    )

    review_payload = VideoFeedbackReview(
        video_summary=VideoSummary(
            sport=sport,
            player_focus=player_focus,
            duration_sec=duration_sec,
            analysis_scope=analysis_scope,
        ),
        overall_assessment=overall,
        moments=moments,
    )
    allowed_timestamps = [round(float(m.timestamp_sec), 2) for m in moments]

    segment_meta = [
        {
            "index": i + 1,
            "t_on": round(float(s[0]), 3),
            "t_off": round(float(s[1]), 3),
            "anchor_sec": round(float(s[2]), 3),
            "probe_count": int(s[3]),
        }
        for i, s in enumerate(segments)
    ]
    cap_debug: dict[str, Any] = {
        "mode": "circle-segment-episodes",
        "outcome": "success",
        "probe_interval_sec": probe_interval,
        "probe_count": len(probes),
        "true_probe_count": sum(1 for _, ok in probes if ok),
        "episodes": len(segments),
        "segments_capped": capped,
        "max_segments_config": max_segments,
        "context_pad_sec": pad,
        "segment_definitions": segment_meta,
    }

    llm_debug: dict[str, Any] = {
        "analysis_kind": "circle-segment-episodes",
        "circle_segment_vision": vision_debug,
        "circle_segment_overall": overall_dbg,
    }

    review = _to_review_document(
        review_id=review_id,
        video_url=video_url,
        duration_sec=duration_sec,
        review_payload=review_payload,
        analysis_mode="circle-segment-episodes",
        allowed_timestamps=allowed_timestamps,
    )
    review["circle_segments"] = segment_meta

    video_pre: dict[str, Any] = {
        "tactical_pipeline_spec": "agents/feedback/video_utils.py (HSV highlight probe)",
        "circle_segment_episodes": len(segments),
        "frames_in_episode_windows": len(merged_assets),
        "frames_sent_to_storyboard": len(storyboard_source),
        "storyboard_pages": len(storyboards),
        "probe_interval_sec": probe_interval,
    }

    review["generation_debug"] = {
        "analysis_kind": "circle-segment-episodes",
        "openai": llm_debug,
        "shared_context_sheet": shared_context_sheet_debug,
        "player_memory_vector_retrieval": player_memory_retrieval_debug,
        "video_preprocess": video_pre,
        "video_highlight_captions": cap_debug,
    }
    review["video_context"] = {
        "description": (
            "Timeline probe for red highlight circle visibility → one coaching episode per contiguous on-screen span; "
            "each episode analyzes pad seconds before first probe where the circle reads as on, the span while on, "
            "and pad seconds after last on-probe. Episode notes are merged for overall assessment; storyboards sample "
            "frames from all episode windows."
        ),
        "combined_highlight_text": segments_markdown[:60_000],
        "preprocess": video_pre,
        "highlight_caption_pass": cap_debug,
    }
    save_json(base_dir / "review.json", review)
    return review


def _default_pose_kb_path() -> Path:
    return Path(__file__).resolve().parents[2] / "yolo_model" / "config" / "posture_guidelines.yaml"


def _attach_pose_events_to_markers(
    review: dict[str, Any],
    pose_events: list[dict[str, Any]],
) -> None:
    """Add pose_event metadata to markers (same order as pose highlight events)."""
    markers = review.get("markers") or []
    if len(markers) != len(pose_events):
        return
    for marker, ev in zip(markers, pose_events):
        marker["pose_marker"] = True
        marker["pose_event"] = {
            "event_index": ev.get("event_index"),
            "start_frame": ev.get("start_frame"),
            "end_frame": ev.get("end_frame"),
            "start_timestamp_sec": ev.get("start_timestamp_sec"),
            "end_timestamp_sec": ev.get("end_timestamp_sec"),
            "frames_used": ev.get("frames_used"),
            "summary_status": ev.get("summary_status"),
            "metrics": ev.get("metrics"),
            "findings": ev.get("findings"),
            "highlight_bbox": ev.get("highlight_bbox"),
            "pose_quality": ev.get("pose_quality"),
            "pose_visibility_mean": ev.get("pose_visibility_mean"),
            "track_ids": ev.get("track_ids"),
        }


def build_review_from_pose_json(
    *,
    review_id: str,
    video_url: str,
    pose_data: dict[str, Any],
    sport: str,
    player_focus: str,
    analysis_scope: str = "",
    coaching_focus: str = "",
    kb_path: Path | None = None,
    player_memory_context: str | None = None,
    shared_context: str | None = None,
    player_memory_retrieval_debug: dict[str, Any] | None = None,
    shared_context_sheet_debug: dict[str, Any] | None = None,
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> dict[str, Any]:
    """Pose JSON (YOLO highlight + body keypoints) → review via circle-segment vision agent."""
    from yolo_model.pose_feedback.engine import (
        format_pose_context_for_agent,
        generate_feedback_payload,
        load_posture_kb,
    )

    kb_file = kb_path or _default_pose_kb_path()
    if not kb_file.is_file():
        raise FileNotFoundError(f"Posture KB not found: {kb_file}")

    kb = load_posture_kb(kb_file)
    feedback = generate_feedback_payload(pose_data, kb, kb_path=str(kb_file))
    events = feedback.get("events") or []
    if not events:
        raise RuntimeError("No highlight events in pose JSON (no detected red-circle frames).")

    if pose_data.get("fps") and pose_data.get("total_frames"):
        duration_sec = float(pose_data["total_frames"]) / float(pose_data["fps"])
    else:
        duration_sec = probe_duration(video_url)

    base_dir = DATA_DIR / "reviews" / review_id
    storyboards_dir = base_dir / "storyboards"
    pad = float((os.getenv("VIDEO_CIRCLE_CONTEXT_PAD_SEC") or "2").strip() or "2")
    per_seg_frames = int((os.getenv("VIDEO_CIRCLE_SEGMENT_FRAMES") or "12").strip() or "12")
    seg_fw = int((os.getenv("VIDEO_CIRCLE_SEGMENT_FRAME_WIDTH") or "720").strip() or "720")

    vision_debug: list[dict[str, Any]] = []
    episode_blocks: list[str] = []
    moments: list[ReviewMoment] = []
    all_window_assets: list[FrameAsset] = []
    used_ts: set[float] = set()
    total = len(events)

    for ev in events:
        _raise_if_cancelled(cancel_check)
        idx = int(ev["event_index"])
        t_on = float(ev["start_timestamp_sec"])
        t_off = float(ev["end_timestamp_sec"])
        anchor = float(ev.get("anchor_timestamp_sec") or (t_on + t_off) / 2.0)

        if on_progress:
            on_progress(
                {
                    "phase": "pose_episode_vision",
                    "segment_total": total,
                    "segment_current": idx,
                    "progress_detail": (
                        f"Pose highlight {idx}/{total}: extracting frames and calling vision agent…"
                    ),
                }
            )

        t_lo = max(0.0, t_on - pad)
        t_hi = min(float(duration_sec), t_off + pad)
        seg_dir = base_dir / "pose_segment_frames" / f"seg_{idx:02d}"
        assets = extract_uniform_frames_in_range(
            video_url,
            t_lo,
            t_hi,
            seg_dir,
            f"p{idx:02d}",
            max_frames=max(4, per_seg_frames),
            frame_width=seg_fw,
            video_duration_sec=duration_sec,
        )
        all_window_assets.extend(assets)
        paths = [a.image_path for a in assets]

        pose_ctx: str | None = None
        if (os.getenv("FEEDBACK_INCLUDE_POSE_CONTEXT_IN_VISION") or "").strip().lower() in (
            "1",
            "true",
            "yes",
        ):
            pose_ctx = format_pose_context_for_agent(ev)
        vis_out, vdbg = vision_analyze_circle_segment(
            frame_paths=paths,
            t_lo=t_lo,
            t_on=t_on,
            t_off=t_off,
            t_hi=t_hi,
            sport=sport,
            player_focus=player_focus,
            segment_index=idx,
            segment_total=total,
            pose_context=pose_ctx,
            coaching_focus=coaching_focus,
            player_memory_context=player_memory_context,
            shared_context=shared_context,
        )
        vdbg["pose_summary_status"] = ev.get("summary_status")
        vdbg["pose_frames_used"] = ev.get("frames_used")
        vision_debug.append(vdbg)

        episode_blocks.append(
            "\n".join(
                [
                    f"### Episode {idx}/{total}  anchor≈{anchor:.2f}s  "
                    f"circle≈{t_on:.2f}s–{t_off:.2f}s  (pose snapshots={ev.get('frames_used')})",
                    f"- category: {vis_out.category}",
                    f"- sentiment: {vis_out.sentiment}",
                    f"- coaching: {vis_out.coaching_note}",
                ]
            )
        )

        ts = round(anchor, 2)
        while ts in used_ts:
            ts = round(ts + 0.05, 2)
        used_ts.add(ts)
        moments.append(
            ReviewMoment(
                timestamp_sec=ts,
                category=vis_out.category,
                sentiment=vis_out.sentiment,
                coaching_note=vis_out.coaching_note,
            )
        )

    moments.sort(key=lambda m: m.timestamp_sec)
    segments_markdown = "\n\n".join(episode_blocks)

    if on_progress:
        on_progress(
            {
                "phase": "pose_overall_synthesis",
                "segment_total": total,
                "segment_current": total,
                "progress_detail": "Synthesizing overall assessment from pose-highlight episodes…",
            }
        )

    merged_assets = _merge_frame_assets_by_path(all_window_assets)
    max_sb_frames = int((os.getenv("VIDEO_CIRCLE_OVERALL_MAX_FRAMES_FOR_STORYBOARD") or "48").strip() or "48")
    storyboard_source = _subsample_frame_assets(merged_assets, max_sb_frames)
    storyboards = create_storyboards(storyboard_source, storyboards_dir)

    _raise_if_cancelled(cancel_check)
    prompt_text = (BASE_DIR / "video_feedback_agent_system_prompt.md").read_text(encoding="utf-8")
    overall, overall_dbg = synthesize_overall_from_circle_segments(
        prompt_tone=prompt_text,
        sport=sport,
        player_focus=player_focus,
        duration_sec=duration_sec,
        analysis_scope=analysis_scope,
        coaching_focus=coaching_focus,
        segments_markdown=segments_markdown,
        storyboard_paths=storyboards,
        player_memory_context=player_memory_context,
        shared_context=shared_context,
    )

    review_payload = VideoFeedbackReview(
        video_summary=VideoSummary(
            sport=sport,
            player_focus=player_focus,
            duration_sec=duration_sec,
            analysis_scope=analysis_scope,
        ),
        overall_assessment=overall,
        moments=moments,
    )
    allowed_timestamps = [round(float(m.timestamp_sec), 2) for m in moments]

    segment_meta = [
        {
            "index": ev["event_index"],
            "t_on": ev["start_timestamp_sec"],
            "t_off": ev["end_timestamp_sec"],
            "anchor_sec": ev.get("anchor_timestamp_sec"),
            "pose_frames_used": ev.get("frames_used"),
            "pose_summary_status": ev.get("summary_status"),
        }
        for ev in events
    ]

    llm_debug: dict[str, Any] = {
        "analysis_kind": "pose-json-episodes",
        "pose_segment_vision": vision_debug,
        "pose_segment_overall": overall_dbg,
        "pose_feedback": feedback,
    }

    review = _to_review_document(
        review_id=review_id,
        video_url=video_url,
        duration_sec=duration_sec,
        review_payload=review_payload,
        analysis_mode="pose-json-episodes",
        allowed_timestamps=allowed_timestamps,
    )
    review["pose_segments"] = segment_meta
    _attach_pose_events_to_markers(review, events)

    video_pre: dict[str, Any] = {
        "pose_highlight_episodes": len(events),
        "frames_in_episode_windows": len(merged_assets),
        "frames_sent_to_storyboard": len(storyboard_source),
        "storyboard_pages": len(storyboards),
        "pose_kb": str(kb_file),
    }

    review["generation_debug"] = {
        "analysis_kind": "pose-json-episodes",
        "openai": llm_debug,
        "shared_context_sheet": shared_context_sheet_debug,
        "player_memory_vector_retrieval": player_memory_retrieval_debug,
        "video_preprocess": video_pre,
    }
    review["video_context"] = {
        "description": (
            "YOLO pose JSON: one marker per red-circle span; per-episode vision agent "
            "(circle-segment flow) with body-pose metrics/findings as supporting context."
        ),
        "combined_highlight_text": segments_markdown[:60_000],
        "preprocess": video_pre,
    }
    save_json(base_dir / "review.json", review)
    return review


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
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
    player_first_name: str | None = None,
    player_last_name: str | None = None,
) -> dict:
    base_dir = DATA_DIR / "reviews" / review_id
    frames_dir = base_dir / "frames"
    storyboards_dir = base_dir / "storyboards"
    focused_dir = base_dir / "focused_frames"

    if _truthy_env("VIDEO_CIRCLE_SEGMENT_MODE", "true"):
        seg_review = _try_circle_segment_episode_review(
            review_id=review_id,
            video_url=video_url,
            base_dir=base_dir,
            storyboards_dir=storyboards_dir,
            sport=sport,
            player_focus=player_focus,
            analysis_scope=analysis_scope,
            coaching_focus=coaching_focus,
            player_memory_context=player_memory_context,
            shared_context=shared_context,
            player_memory_retrieval_debug=player_memory_retrieval_debug,
            shared_context_sheet_debug=shared_context_sheet_debug,
            on_progress=on_progress,
            cancel_check=cancel_check,
            player_first_name=player_first_name,
            player_last_name=player_last_name,
        )
        if seg_review is not None:
            return seg_review

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
    _raise_if_cancelled(cancel_check)
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

    _raise_if_cancelled(cancel_check)
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
        "tactical_pipeline_spec": "agents/feedback/video_utils.py (HSV highlight probe)",
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
