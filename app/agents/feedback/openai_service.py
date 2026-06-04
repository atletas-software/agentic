from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Any, List, Literal, Optional

from openai import OpenAI
from pydantic import BaseModel, Field

from agents.feedback.models import (
    Category,
    OverallAssessment,
    Sentiment,
    VideoFeedbackReview,
)
from agents.feedback.video_utils import FrameAsset


class PlayerLocalization(BaseModel):
    found: bool
    confidence: Literal["low", "medium", "high"]
    center_x: float
    center_y: float
    radius: float
    note: str


class CircleSegmentVisionOutput(BaseModel):
    """Structured coaching for one highlight visibility window (pre, during circle, post)."""

    category: Category
    sentiment: Sentiment
    coaching_note: str = Field(max_length=2500)


class ManualMomentFeedback(BaseModel):
    action_type: Literal[
        "shot",
        "pass",
        "tackle",
        "run",
        "positioning_without_ball",
        "header",
        "other",
    ]
    category: Literal[
        "scanning",
        "body_shape",
        "positioning",
        "movement",
        "first_touch",
        "passing",
        "pressing",
        "transition",
        "communication",
        "duel",
        "decision_making",
        "finishing",
        "defending",
    ]
    sentiment: Literal["positive", "corrective", "mixed"]
    coaching_note: str


def _encode_image(path: Path) -> str:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise RuntimeError(f"Cannot read frame image for vision (missing or unreadable): {path}") from exc
    return base64.b64encode(raw).decode("utf-8")


_DEBUG_STORE_MAX = 48_000


def _truncate_for_debug_text(text: str, max_chars: int = _DEBUG_STORE_MAX) -> str:
    """Keep review.json bounded; full prompts may include long shared_context / memory."""
    text = text or ""
    if len(text) <= max_chars:
        return text
    head = max_chars - 100
    return text[:head] + f"\n\n... [truncated for storage — total length {len(text)} characters]\n"


def _sample_window_frames(grp: list[FrameAsset], max_n: int) -> list[FrameAsset]:
    if max_n <= 0 or not grp:
        return []
    if len(grp) <= max_n:
        return grp
    n = len(grp)
    idxs = [round(i * (n - 1) / (max_n - 1)) for i in range(max_n)]
    return [grp[int(i)] for i in idxs]


def summarize_highlight_windows_for_feedback(
    *,
    windows: list[tuple[float, list[FrameAsset]]],
    player_focus: str,
    sport: str,
    window_sec: float,
    max_windows: int = 8,
    max_images_per_window: int = 5,
    max_images_total: int = 24,
) -> tuple[str, dict[str, Any]]:
    """
    One vision call: chronological stills around each highlight (±window_sec) with labels.
    Returns combined markdown-style text for the main feedback model + debug metadata.
    """
    debug: dict[str, Any] = {
        "outcome": "skipped",
        "window_sec": window_sec,
        "max_windows_config": max_windows,
        "max_images_total_config": max_images_total,
    }
    if not windows:
        debug["reason"] = "no_highlight_windows"
        return "", debug
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        debug["reason"] = "OPENAI_API_KEY_missing"
        return "", debug

    model = (os.getenv("VIDEO_HIGHLIGHT_SUMMARY_MODEL") or "gpt-4o-mini").strip()
    client = OpenAI(api_key=api_key)

    trimmed = windows[:max_windows]
    to_process: list[tuple[float, list[FrameAsset], list[FrameAsset]]] = []
    img_budget = 0
    for T, grp in trimmed:
        sampled = _sample_window_frames(grp, max_images_per_window)
        if not sampled:
            continue
        if img_budget + len(sampled) > max_images_total:
            sampled = sampled[: max(0, max_images_total - img_budget)]
        if not sampled:
            continue
        to_process.append((T, sampled, grp))
        img_budget += len(sampled)
        if img_budget >= max_images_total:
            break

    meta_windows: list[dict[str, Any]] = []
    parts: list[dict[str, Any]] = [
        {
            "type": "text",
            "text": (
                f"Sport: {sport or 'Soccer'}. Player focus: {player_focus or 'the circled athlete'}.\n"
                f"Each block is labeled with highlight time T (seconds). Images are chronological stills from "
                f"about T−{window_sec:g}s through T+{window_sec:g}s (red circle marks the highlight frame when present).\n"
                "For EVERY block, output markdown:\n"
                "### Highlight T=<seconds>s\n"
                "- Pre (before the moment): positioning / pressure / options (1–2 bullets)\n"
                "- Event: what happens at the highlight (1–2 bullets)\n"
                "- Post: immediate consequence / next read (1–2 bullets)\n"
                "Be specific and tactical; no generic filler. If a frame is unclear, say so briefly.\n"
                "Separate each highlight block with a line containing only ---"
            ),
        }
    ]
    for T, sampled, full_grp in to_process:
        meta_windows.append(
            {
                "anchor_sec": T,
                "frame_seconds": [round(f.timestamp_sec, 2) for f in full_grp],
                "frames_sent_to_captioner": len(sampled),
            }
        )
        parts.append({"type": "text", "text": f"--- Highlight T={T:.2f}s ({len(sampled)} frame(s)) ---"})
        for f in sampled:
            parts.append(
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{_encode_image(f.image_path)}"},
                }
            )

    img_count = sum(len(s) for _, s, _ in to_process)
    debug["windows_captioned"] = len(to_process)
    debug["images_sent_to_captioner"] = img_count
    debug["model"] = model

    if not to_process:
        debug["outcome"] = "skipped"
        debug["reason"] = "no_images_after_sampling"
        return "", debug

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "You write concise elite football tactical summaries from still frames only.",
                },
                {"role": "user", "content": parts},
            ],
            max_tokens=int(os.getenv("VIDEO_HIGHLIGHT_SUMMARY_MAX_TOKENS", "2500")),
        )
        text = (resp.choices[0].message.content or "").strip()
    except Exception as exc:  # noqa: BLE001
        debug["outcome"] = "error"
        debug["error"] = str(exc)
        return "", debug

    debug["outcome"] = "success" if text else "empty"
    debug["chars_returned"] = len(text)
    debug["windows_detail"] = meta_windows
    return text, debug


def vision_analyze_circle_segment(
    *,
    frame_paths: list[Path],
    t_lo: float,
    t_on: float,
    t_off: float,
    t_hi: float,
    sport: str,
    player_focus: str,
    segment_index: int,
    segment_total: int,
    pose_context: str | None = None,
) -> tuple[CircleSegmentVisionOutput, dict[str, Any]]:
    """Single vision parse for one episode: stills cover pre-circle, visible span, and post-circle."""
    debug: dict[str, Any] = {
        "segment_index": segment_index,
        "segment_total": segment_total,
        "t_lo": t_lo,
        "t_on": t_on,
        "t_off": t_off,
        "t_hi": t_hi,
        "frame_count": len(frame_paths),
        "outcome": "pending",
    }
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        debug["outcome"] = "error"
        debug["error"] = "OPENAI_API_KEY_missing"
        raise RuntimeError("OPENAI_API_KEY is missing. Add it to your environment or .env file.")
    if not frame_paths:
        debug["outcome"] = "error"
        debug["error"] = "no_frames"
        raise RuntimeError("vision_analyze_circle_segment requires at least one frame.")

    model = (os.getenv("VIDEO_CIRCLE_SEGMENT_VISION_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4.1-mini").strip()
    client = OpenAI(api_key=api_key)

    user_lines = [
        "You analyze still frames from one continuous time window of a soccer performance clip.",
        f"Sport: {sport or 'Soccer'}. Player focus: {player_focus or 'the athlete circled in red when visible'}.",
        f"Episode {segment_index} of {segment_total}: red highlight circle visibility is approximately {t_on:.2f}s–{t_off:.2f}s.",
        f"Frames span {t_lo:.2f}s through {t_hi:.2f}s (includes ~{t_lo:.2f}s–{t_on:.2f}s before the circle reliably appears, the span while it is on, and ~{t_off:.2f}s–{t_hi:.2f}s after it disappears).",
        "Images are chronological. The red circle may only appear on some middle frames.",
        "",
        "Tasks:",
        "1) Judge positioning, movement, and decisions for the circled/target player across the whole window (before, during, after the highlight).",
        "2) Choose one primary category and sentiment.",
        "3) Write coaching_note as 2–5 tight sentences: what to keep doing, what to adjust, and the next read — grounded only in what the stills support.",
        "If the target player is unclear, stay conservative and avoid invented actions.",
    ]
    pose_block = (pose_context or "").strip()
    if pose_block:
        user_lines.extend(
            [
                "",
                "YOLOv8 body-pose analysis for this highlight (shoulders through ankles only; use as supporting signal):",
                pose_block,
                "Blend visible action in the frames with posture findings; do not quote raw angle lists in coaching_note.",
            ]
        )
    user_text = "\n".join(user_lines)
    content: list[dict[str, Any]] = [{"type": "input_text", "text": user_text}]
    for path in frame_paths:
        content.append(
            {
                "type": "input_image",
                "image_url": f"data:image/jpeg;base64,{_encode_image(path)}",
            }
        )

    try:
        response = client.responses.parse(
            model=model,
            input=[{"role": "user", "content": content}],
            text_format=CircleSegmentVisionOutput,
        )
        parsed = response.output_parsed
    except Exception as exc:  # noqa: BLE001
        debug["outcome"] = "error"
        debug["error"] = str(exc)
        raise
    if parsed is None:
        debug["outcome"] = "error"
        debug["error"] = "empty_parse"
        raise RuntimeError("Segment vision returned no parsed payload.")

    debug["outcome"] = "success"
    debug["model"] = model
    return parsed, debug


def synthesize_overall_from_circle_segments(
    *,
    prompt_tone: str,
    sport: str,
    player_focus: str,
    duration_sec: float,
    analysis_scope: str,
    coaching_focus: str,
    segments_markdown: str,
    storyboard_paths: list[Path],
    player_memory_context: Optional[str] = None,
    shared_context: Optional[str] = None,
) -> tuple[OverallAssessment, dict[str, Any]]:
    """Second pass: overall strengths / improvements / next_focus from episode notes + optional storyboards."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing. Add it to your environment or .env file.")

    model = (os.getenv("VIDEO_CIRCLE_OVERALL_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4.1-mini").strip()
    client = OpenAI(api_key=api_key)

    system = (
        "You are an elite youth soccer performance analyst. You synthesize episode-level coaching notes into "
        "one overall assessment (strengths, improvements, next_focus). "
        "Ground the synthesis in the structured episode text and any storyboard pages; do not contradict them. "
        "Tone from the reference prompt:\n"
        + (prompt_tone or "")[:6000]
    )

    prefix_lines = [
        "Synthesize overall assessment from highlight-circle episodes (each episode analyzed separately below).",
        f"Sport: {sport or 'Soccer'}",
        f"Player focus: {player_focus or 'the athlete'}",
        f"Video duration (seconds): {duration_sec:.2f}",
        f"Analysis scope: {analysis_scope or 'Full clip.'}",
        f"Coaching focus requested: {coaching_focus or 'Balanced technical and tactical feedback.'}",
        "",
        "--- EPISODES (structured; one red-circle visibility span per block) ---",
        segments_markdown[:80_000],
    ]
    user_text_prefix = "\n".join(prefix_lines)

    mem = (player_memory_context or "").strip()
    org = (shared_context or "").strip()
    suffix_parts: list[str] = []
    if mem:
        suffix_parts.extend(
            [
                "",
                "--- PLAYER MEMORY (continuity for this player; do not contradict episode evidence) ---",
                mem[:120_000],
            ]
        )
    if org:
        suffix_parts.extend(
            [
                "",
                "--- SHARED CONTEXT (club rubric / vocabulary) ---",
                org[:120_000],
            ]
        )
    user_text_suffix = "\n".join(suffix_parts) if suffix_parts else ""

    content: list[dict[str, Any]] = [{"type": "input_text", "text": user_text_prefix}]
    for path in storyboard_paths:
        content.append(
            {
                "type": "input_image",
                "image_url": f"data:image/jpeg;base64,{_encode_image(path)}",
            }
        )
    if user_text_suffix:
        content.append({"type": "input_text", "text": user_text_suffix})

    response = client.responses.parse(
        model=model,
        input=[
            {"role": "system", "content": system},
            {"role": "user", "content": content},
        ],
        text_format=OverallAssessment,
    )
    parsed = response.output_parsed
    if parsed is None:
        raise RuntimeError("Overall synthesis returned no parsed payload.")

    bridge = (
        f"\n\n--- [{len(storyboard_paths)} storyboard image(s) between text blocks] ---\n\n"
        if storyboard_paths
        else ""
    )
    llm_debug: dict[str, Any] = {
        "model": model,
        "pass": "circle_segment_overall",
        "system_message": _truncate_for_debug_text(system),
        "user_message_text": _truncate_for_debug_text(user_text_prefix + bridge + user_text_suffix),
        "storyboard_image_count": len(storyboard_paths),
    }
    return parsed, llm_debug


def analyze_storyboards(
    *,
    prompt_text: str,
    sport: str,
    player_focus: str,
    duration_sec: float,
    analysis_scope: str,
    coaching_focus: str,
    storyboard_paths: list[Path],
    analysis_mode: str,
    allowed_timestamps: Optional[List[float]] = None,
    player_memory_context: Optional[str] = None,
    shared_context: Optional[str] = None,
    highlight_window_narrative: Optional[str] = None,
) -> tuple[VideoFeedbackReview, dict[str, Any]]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing. Add it to your environment or .env file.")

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    client = OpenAI(api_key=api_key)

    hw = (highlight_window_narrative or "").strip()
    prefix_lines: list[str] = [
        "Analyze this soccer player review video from storyboard images.",
        f"Sport: {sport}",
        f"Player focus: {player_focus or 'Unknown player'}",
        f"Video duration in seconds: {duration_sec:.2f}",
        f"Analysis scope: {analysis_scope or 'Review the full visible video.'}",
        f"Coaching focus: {coaching_focus or 'Balanced technical and tactical feedback.'}",
        f"Analysis mode: {analysis_mode}",
        f"Allowed feedback timestamps in seconds: {_format_allowed_timestamps(allowed_timestamps)}",
    ]
    if hw:
        prefix_lines.extend(
            [
                "",
                "--- HIGHLIGHT WINDOW NARRATIVES (separate vision pass on ±2s stills per red-circle detection; tactical text to combine with storyboards) ---",
                hw[:25_000],
            ]
        )
    prefix_lines.extend(
        [
            "",
            "Important limits:",
            "- You are seeing storyboard frames, not raw motion video.",
            "- If the player is circled in red, treat the circled player as the only target of the feedback.",
            "- Only create feedback at timestamps listed in Allowed feedback timestamps.",
            "- Do not invent feedback moments between allowed timestamps.",
            "- If no red circle is visible in a frame, do not build player-specific claims from that frame unless clearly justified.",
            "- Only make claims supported by visible evidence.",
            "- If motion detail is uncertain, say less and keep the note conservative.",
            "- Use the timestamp labels visible in the frames to anchor your feedback moments.",
            "- Return JSON only through the structured schema.",
            "",
            "Reasoning order: (1) Interpret storyboard images together with HIGHLIGHT WINDOW NARRATIVES (if any).",
            "(2) Then apply PLAYER MEMORY (if present) for this athlete's continuity.",
            "(3) Then align language and priorities with SHARED CONTEXT rubric (if present).",
        ]
    )
    user_text_prefix = "\n".join(prefix_lines)

    mem = (player_memory_context or "").strip()
    org = (shared_context or "").strip()
    post_blocks: list[str] = [
        "Additional context blocks below — use them only after you have grounded judgments in the storyboard images.",
    ]
    if mem:
        post_blocks.extend(
            [
                "",
                "--- PLAYER MEMORY (this player only; continuity — do not contradict clear video evidence) ---",
                mem,
            ]
        )
    if org:
        post_blocks.extend(
            [
                "",
                "--- SHARED CONTEXT (organization-wide rubric / vocabulary for all players) ---",
                org,
            ]
        )
    user_text_suffix = "\n".join(post_blocks) if (mem or org) else ""

    content: list[dict[str, Any]] = [{"type": "input_text", "text": user_text_prefix}]
    for path in storyboard_paths:
        content.append(
            {
                "type": "input_image",
                "image_url": f"data:image/jpeg;base64,{_encode_image(path)}",
            }
        )
    if user_text_suffix:
        content.append({"type": "input_text", "text": user_text_suffix})

    response = client.responses.parse(
        model=model,
        input=[
            {"role": "system", "content": prompt_text},
            {"role": "user", "content": content},
        ],
        text_format=VideoFeedbackReview,
    )
    parsed = response.output_parsed
    if parsed is None:
        raise RuntimeError("The model did not return a parsed review payload.")
    bridge = f"\n\n--- [{len(storyboard_paths)} storyboard image(s) sent to the model between the two text blocks] ---\n\n"
    user_text_for_debug = user_text_prefix + bridge + user_text_suffix
    llm_debug: dict[str, Any] = {
        "model": model,
        "system_prompt_file": "video_feedback_agent_system_prompt.md",
        "system_message": _truncate_for_debug_text(prompt_text),
        "user_message_text": _truncate_for_debug_text(user_text_for_debug),
        "user_message_note": (
            "User message is split: (1) instructions + optional highlight-window narrative text, then "
            f"{len(storyboard_paths)} input_image storyboard(s), then (2) PLAYER MEMORY and/or SHARED CONTEXT text."
        ),
        "storyboard_image_count": len(storyboard_paths),
    }
    return parsed, llm_debug


def _format_allowed_timestamps(allowed_timestamps: Optional[List[float]]) -> str:
    if not allowed_timestamps:
        return "none provided"
    return ", ".join(f"{timestamp:.2f}" for timestamp in allowed_timestamps)


class TextCoachingStructured(BaseModel):
    """Structured text-only coaching (no video frames)."""

    coach_letter: str = Field(
        description=(
            "2–4 short paragraphs as a supportive youth coach: acknowledge effort, name 1–2 bright spots, "
            "then prioritize what to sharpen next with clear, encouraging language. Address the player (or parent) directly."
        )
    )
    strengths: List[str] = Field(
        default_factory=list,
        description="3–6 short bullet phrases grounded in the supplied context.",
    )
    improvements: List[str] = Field(
        default_factory=list,
        description="3–7 concrete development areas; use action verbs (e.g. scan before receiving, open body shape).",
    )
    next_focus: List[str] = Field(
        description="1–3 priority themes for upcoming training sessions.",
        min_length=1,
        max_length=3,
    )


def generate_text_coaching_review(
    *,
    sport: str,
    player_focus: str,
    analysis_scope: str,
    coaching_focus: str,
    video_link_for_reference: str = "",
    player_memory_context: Optional[str] = None,
    shared_context: Optional[str] = None,
) -> tuple[TextCoachingStructured, dict[str, Any]]:
    """
    Coach-style written feedback using sheet/game scope + optional vector memory + optional org rubric.
    No video or ffprobe — safe for Trace/Hudl page URLs that are not direct media files.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing. Add it to your environment or .env file.")

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    client = OpenAI(api_key=api_key)

    system = (
        "You are an experienced youth soccer coach writing feedback for a player and their family. "
        "You do NOT have video — only written context. Never claim you watched a video or saw specific clips. "
        "Ground every statement in the provided context blocks; if context is thin, say so briefly and stay general but still useful. "
        "Tone: professional, warm, specific, and developmental (what to enhance next and how). "
        "Distinguish: (1) player-specific notes from PLAYER MEMORY vs (2) organization-wide standards from SHARED CONTEXT — "
        "apply the rubric without treating it as private facts about this athlete unless memory and context agree."
    )

    parts: list[str] = [
        "Write text-only coaching feedback using the following inputs.",
        f"Sport: {sport or 'Soccer'}",
        f"Player / role focus: {player_focus or 'the athlete'}",
        f"Game / session / sheet context: {analysis_scope or '(none supplied)'}",
        f"Coaching priorities requested: {coaching_focus or 'balanced technical and tactical growth'}",
    ]
    link = (video_link_for_reference or "").strip()
    if link:
        parts.append(
            "Optional viewing link (may be a team app page, not a direct video file — do not assume you viewed it): "
            + link
        )
    parts.append(
        "Reasoning order: (1) Ground in game/session context above. "
        "(2) Then apply PLAYER MEMORY (if present). "
        "(3) Then align with SHARED CONTEXT rubric (if present)."
    )
    mem = (player_memory_context or "").strip()
    if mem:
        parts.extend(
            [
                "",
                "--- PLAYER MEMORY (retrieved notes about this player only; treat as higher-truth for personalization) ---",
                mem[:120_000],
            ]
        )
    org = (shared_context or "").strip()
    if org:
        parts.extend(
            [
                "",
                "--- SHARED CONTEXT (club/program standards for all players; use as rubric) ---",
                org[:120_000],
            ]
        )
    user_text = "\n".join(parts)

    response = client.responses.parse(
        model=model,
        input=[
            {"role": "system", "content": system},
            {"role": "user", "content": [{"type": "input_text", "text": user_text}]},
        ],
        text_format=TextCoachingStructured,
    )
    parsed = response.output_parsed
    if parsed is None:
        raise RuntimeError("The model did not return parsed text coaching.")
    llm_debug: dict[str, Any] = {
        "model": model,
        "system_message": _truncate_for_debug_text(system),
        "user_message_text": _truncate_for_debug_text(user_text),
    }
    return parsed, llm_debug


def locate_player_in_frame(
    *,
    image_path: Path,
    player_focus: str,
    marker_label: str,
    coaching_note: str,
) -> PlayerLocalization:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing. Add it to your environment or .env file.")

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    client = OpenAI(api_key=api_key)

    user_text = "\n".join(
        [
            "Find the player being reviewed in this soccer frame.",
            f"Player focus: {player_focus or 'Unknown player'}",
            f"Marker label: {marker_label}",
            f"Marker note: {coaching_note}",
            "",
            "Return the approximate center point and radius of a circle around that player.",
            "Coordinates must be normalized between 0 and 1.",
            "If the player cannot be identified confidently, set found=false and explain why.",
        ]
    )

    response = client.responses.parse(
        model=model,
        input=[
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": user_text},
                    {
                        "type": "input_image",
                        "image_url": f"data:image/jpeg;base64,{_encode_image(image_path)}",
                    },
                ],
            }
        ],
        text_format=PlayerLocalization,
    )
    parsed = response.output_parsed
    if parsed is None:
        raise RuntimeError("The model did not return player localization data.")
    return parsed


def analyze_manual_moment(
    *,
    context_image_paths: List[Path],
    marked_image_path: Path,
    player_focus: str,
    player_name: str,
    sport: str,
    timestamp_sec: float,
    context_start_sec: float,
    feedback_prompt: str = "",
) -> ManualMomentFeedback:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing. Add it to your environment or .env file.")

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    client = OpenAI(api_key=api_key)

    user_text = "\n".join(
        [
            "Provide one concise soccer coaching feedback marker for the manually selected player.",
            f"Sport: {sport or 'Soccer'}",
            f"Player focus: {player_focus or 'selected player'}",
            f"Player name to address: {player_name or player_focus or 'the player'}",
            f"Marker timestamp: {timestamp_sec:.2f} seconds",
            f"Context window: {context_start_sec:.2f} to {timestamp_sec:.2f} seconds",
            f"Coach requested feedback focus: {feedback_prompt or 'Balanced technical and tactical feedback.'}",
            "",
            "First classify the selected player's main action in the last 2 seconds using exactly one action_type:",
            "- shot: the selected player shoots or prepares/follows through on a shot.",
            "- pass: the selected player passes, receives-to-pass, or clearly chooses a passing option.",
            "- tackle: the selected player challenges, tackles, blocks, or tries to win the ball.",
            "- run: the selected player carries the ball, sprints, overlaps, underlaps, or makes a clear run.",
            "- positioning_without_ball: the selected player is mainly shaping, scanning, marking, supporting, opening body angle, or creating space without touching the ball.",
            "- header: the selected player heads the ball or contests an aerial/header action.",
            "- other: use only when none of the above can be supported visually.",
            "",
            "You receive a sequence of images from the final seconds before the paused moment.",
            "The last image is the paused moment with a red bounding box around the selected player.",
            "Earlier images are sampled from the previous 2 seconds and may include a red circle when the selected player could be tracked.",
            "",
            "Analyze the action that just happened across this sequence, not only the frozen final frame.",
            "Only analyze the selected player: the player inside the red bounding box in the final image and the matching red-circled player in earlier images.",
            "Make the coaching_note explicitly match the chosen action_type.",
            "Write the coaching_note as direct advice to the player by name.",
            "Make the feedback directional: include what the player should do next time, using clear action verbs.",
            "Avoid generic praise. Be specific about the visible decision, body shape, movement, timing, pressure, or option.",
            "Keep the note to 1-2 short sentences.",
            "Prioritize the coach requested feedback focus when writing the note.",
            "If no coach focus was provided, choose the most relevant technical/tactical feedback for the detected action_type.",
            "If the requested focus is not visible, say what is visible and keep the advice conservative.",
            "Keep feedback specific to this moment.",
            "If the moment is unclear, say what can be safely observed and give conservative advice.",
        ]
    )

    response = client.responses.parse(
        model=model,
        input=[
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": user_text},
                    *[
                        {
                            "type": "input_image",
                            "image_url": f"data:image/jpeg;base64,{_encode_image(path)}",
                        }
                        for path in context_image_paths
                    ],
                    {
                        "type": "input_image",
                        "image_url": f"data:image/jpeg;base64,{_encode_image(marked_image_path)}",
                    },
                ],
            }
        ],
        text_format=ManualMomentFeedback,
    )
    parsed = response.output_parsed
    if parsed is None:
        raise RuntimeError("The model did not return manual feedback.")
    return parsed
