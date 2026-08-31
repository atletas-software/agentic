from __future__ import annotations

import base64
import os
import re
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


def coaching_wants_per_circle_feedback(coaching_focus: str | None) -> bool:
    """True when the admin prompt asks for feedback at each red-circle highlight."""
    cf = (coaching_focus or "").strip().lower()
    if not cf:
        return False
    patterns = (
        "every time",
        "each time",
        "whenever",
        "each circle",
        "every circle",
        "circled",
        "red circle",
        "highlight",
        "per circle",
        "when player is circled",
        "when the player is circled",
    )
    return any(p in cf for p in patterns)


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
    pitch_location: str = Field(
        max_length=1200,
        description=(
            "Where the circled player is on the pitch from the wide camera view: half (own/opponent), "
            "channel (left/center/right), distance from halfway line, vertical third, and a coach label "
            "e.g. 'left defensive midfield support'. No body mechanics."
        ),
    )
    coaching_note: str = Field(
        max_length=2500,
        description=(
            "Coach-to-player note: tactical/technical soccer only — game situations (e.g. 3v2 overload, "
            "1v1, third-man runs), support angles, passing/receiving decisions, pressing, dribble skills. "
            "Reference pitch_location when useful. Never describe how the body looks."
        ),
    )


class _CoachingNoteRewrite(BaseModel):
    coaching_note: str = Field(max_length=2500)


class OpenAIPickedMoment(BaseModel):
    timestamp_sec: float = Field(ge=0)
    importance: Literal["high", "medium", "low"] = "medium"
    action: str = Field(default="", max_length=240)
    why: str = Field(default="", max_length=800)


class OpenAIMomentPickOutput(BaseModel):
    moments: list[OpenAIPickedMoment] = Field(default_factory=list)


_IMPORTANCE_RANK = {"high": 0, "medium": 1, "low": 2}


def merge_openai_picked_moments(
    moments: list[OpenAIPickedMoment] | list[dict[str, Any]],
    *,
    min_gap_sec: float = 3.0,
    max_moments: int = 10,
) -> list[OpenAIPickedMoment]:
    """Keep the most important moments, dropping near-duplicates."""
    parsed: list[OpenAIPickedMoment] = []
    for item in moments:
        if isinstance(item, OpenAIPickedMoment):
            parsed.append(item)
            continue
        try:
            parsed.append(OpenAIPickedMoment.model_validate(item))
        except Exception:  # noqa: BLE001
            continue
    parsed.sort(
        key=lambda m: (_IMPORTANCE_RANK.get(m.importance, 9), m.timestamp_sec),
    )
    kept: list[OpenAIPickedMoment] = []
    for moment in parsed:
        if any(abs(moment.timestamp_sec - other.timestamp_sec) < min_gap_sec for other in kept):
            continue
        kept.append(moment)
        if len(kept) >= max(1, max_moments):
            break
    kept.sort(key=lambda m: m.timestamp_sec)
    return kept


def select_coaching_moments_from_frames(
    *,
    frames: list[FrameAsset],
    player_focus: str,
    sport: str,
    duration_sec: float,
    coaching_focus: str | None = None,
    player_memory_context: str | None = None,
    shared_context: str | None = None,
    batch_size: int = 16,
    min_gap_sec: float = 3.0,
    max_moments: int = 10,
) -> tuple[list[OpenAIPickedMoment], dict[str, Any]]:
    """Pass 1: OpenAI vision picks coaching-worthy timestamps from sampled stills."""
    circle_mode = coaching_wants_per_circle_feedback(coaching_focus)
    if circle_mode:
        min_gap_sec = min(min_gap_sec, float((os.getenv("VIDEO_OPENAI_CIRCLE_MIN_GAP_SEC") or "1.0").strip() or "1.0"))
        max_moments = max(max_moments, int((os.getenv("VIDEO_OPENAI_CIRCLE_MAX_MOMENTS") or "25").strip() or "25"))

    debug: dict[str, Any] = {
        "outcome": "pending",
        "frames_offered": len(frames),
        "batches": 0,
        "raw_moment_count": 0,
        "circle_mode": circle_mode,
    }
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        debug["outcome"] = "error"
        debug["error"] = "OPENAI_API_KEY_missing"
        raise RuntimeError("OPENAI_API_KEY is missing. Add it to your environment or .env file.")
    if not frames:
        debug["outcome"] = "skipped"
        debug["reason"] = "no_frames"
        return [], debug

    model = (
        os.getenv("VIDEO_OPENAI_MOMENT_MODEL")
        or os.getenv("VIDEO_CIRCLE_SEGMENT_VISION_MODEL")
        or os.getenv("OPENAI_MODEL")
        or "gpt-4.1-mini"
    ).strip()
    client = OpenAI(api_key=api_key)
    chunk = max(4, int(batch_size or 16))
    raw: list[OpenAIPickedMoment] = []
    batch_debug: list[dict[str, Any]] = []

    for start in range(0, len(frames), chunk):
        batch = frames[start : start + chunk]
        t0 = batch[0].timestamp_sec
        t1 = batch[-1].timestamp_sec
        user_parts: list[dict[str, Any]] = [
            {
                "type": "text",
                "text": (
                    f"Sport: {sport or 'Soccer'}. Player to coach: {player_focus or 'the focal athlete'}.\n"
                    f"Video duration: {duration_sec:.1f}s. These stills cover about {t0:.1f}s–{t1:.1f}s.\n"
                    + (
                        "Each image is labeled with its timestamp. Pick EVERY timestamp where the named player "
                        "is highlighted with a red circle overlay. Return one moment per distinct red-circle "
                        "highlight (do not skip circles). timestamp_sec must match a labeled frame time (or very close).\n"
                        "If no red circle highlights the focal player in this batch, return an empty moments list."
                        if circle_mode
                        else (
                            "Each image is labeled with its timestamp. Pick the most useful coaching moments "
                            "for THIS player (on or off the ball): 1v1s, receives, scans, press, support angles, "
                            "decisions, finishing, defensive actions.\n"
                            "Do not pick moments just because a red circle is present. Ignore other players unless "
                            "the focal player is involved.\n"
                            "Return 0–5 moments. timestamp_sec must match a labeled frame time (or very close). "
                            "If the focal player is not visible in this batch, return an empty moments list."
                        )
                    )
                ),
            }
        ]
        cf = (coaching_focus or "").strip()
        if cf:
            user_parts.append(
                {
                    "type": "text",
                    "text": (
                        "--- ADMIN SESSION BRIEF (MANDATORY when selecting moments) ---\n" + cf[:1200]
                        if circle_mode
                        else f"Session direction from the coach: {cf[:800]}"
                    ),
                }
            )
        mem = (player_memory_context or "").strip()
        if mem:
            user_parts.append(
                {
                    "type": "text",
                    "text": (
                        "--- PLAYER MEMORY (style/themes only, do not invent events) ---\n" + mem[:4000]
                    ),
                }
            )
        org = (shared_context or "").strip()
        if org:
            user_parts.append(
                {
                    "type": "text",
                    "text": (
                        "--- SHARED CLUB RUBRIC (vocabulary and standards) ---\n" + org[:4000]
                    ),
                }
            )
        for frame in batch:
            user_parts.append({"type": "text", "text": f"Frame t={frame.timestamp_sec:.2f}s"})
            user_parts.append(
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{_encode_image(frame.image_path)}"},
                }
            )
        batch_meta: dict[str, Any] = {
            "t_lo": t0,
            "t_hi": t1,
            "frame_count": len(batch),
            "outcome": "pending",
        }
        try:
            response = client.responses.parse(
                model=model,
                input=[
                    {
                        "role": "system",
                        "content": (
                            "You are a youth soccer coach selecting clip timestamps for later tactical feedback. "
                            + (
                                "When the admin brief asks for feedback at each red-circle highlight, return one "
                                "moment per distinct circle — do not merge or skip visible circles."
                                if circle_mode
                                else "Only choose moments that will teach the named player something specific."
                            )
                        ),
                    },
                    {"role": "user", "content": user_parts},
                ],
                text_format=OpenAIMomentPickOutput,
            )
            parsed = response.output_parsed
            found = list(parsed.moments) if parsed else []
            raw.extend(found)
            batch_meta["outcome"] = "success"
            batch_meta["moment_count"] = len(found)
        except Exception as exc:  # noqa: BLE001
            batch_meta["outcome"] = "error"
            batch_meta["error"] = str(exc)[:500]
        batch_debug.append(batch_meta)

    merged = merge_openai_picked_moments(raw, min_gap_sec=min_gap_sec, max_moments=max_moments)
    debug.update(
        {
            "outcome": "success" if merged else "empty",
            "model": model,
            "batches": len(batch_debug),
            "raw_moment_count": len(raw),
            "kept_moment_count": len(merged),
            "batch_debug": batch_debug,
            "kept_timestamps": [round(m.timestamp_sec, 2) for m in merged],
        }
    )
    return merged, debug


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


def _pose_context_enabled_for_vision() -> bool:
    """YOLO pose is used to find highlight moments; biomechanical context in vision is opt-in."""
    return (os.getenv("FEEDBACK_INCLUDE_POSE_CONTEXT_IN_VISION") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


_COACHING_STYLE_GOOD_EXAMPLES = """
GOOD coaching_note style (match this voice — tactical soccer, like prior coach annotations):
- "Well done applying the 3rd man principle to progress the attack. As you can see, the on-ball player receives the ball in front of him."
- "3 v 2, attacking advantage on the wing. Try to support the on-ball player and outplay the overload."
- "Well done supporting the striker from underneath as an attacking midfielder. The skill you used there — roll, step-over, accelerate with the other foot — is also great in a 1 v 1."

BAD coaching_note style (NEVER write like this):
- "His body shape is mostly upright and ready… shoulder dip may reduce balance… squaring the shoulders toward play…"
- Any mention of upright torso, straight knees, knee flexion, shoulder position/dip, squaring shoulders, athletic posture, explosiveness from stance, or physiotherapy cues.
""".strip()

_BIOMECHANICAL_COACHING_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"\bshoulder",
        r"\btorso\b",
        r"\bknees?\b",
        r"\bupright\b",
        r"\bposture\b",
        r"\bflexion\b",
        r"\bsquaring\b",
        r"\bperipheral vision\b",
        r"\bathletic (?:stance|posture|shape)\b",
        r"\bbody shape is\b",
        r"\b(?:dip|lean|tilt).{0,40}\b(?:shoulder|torso|balance)\b",
        r"\b(?:shoulder|torso|hip).{0,40}\b(?:balance|vision|scanning)\b",
        r"\bready,? supporting effective scanning\b",
        r"\bexplosiveness\b",
    )
)


def _coaching_note_has_biomechanical_language(text: str) -> bool:
    return any(p.search(text or "") for p in _BIOMECHANICAL_COACHING_PATTERNS)


def _rewrite_coaching_note_tactical(
    client: OpenAI,
    model: str,
    *,
    note: str,
    player_focus: str,
    player_memory_context: str | None,
) -> str:
    """Strip anatomy/posture phrasing; keep tactical soccer coaching only."""
    mem = (player_memory_context or "").strip()
    mem_block = mem[:4000] if mem else "(none)"
    prompt = "\n".join(
        [
            "Rewrite the coaching note below for a youth soccer player.",
            "Keep the same overall tactical point (positioning, decisions, communication, overload, etc.) "
            "but remove ALL description of how the body looks or moves mechanically.",
            "Do NOT mention: shoulders, torso, knees, upright, posture, balance (physical), squaring, peripheral vision, athletic stance.",
            "Write 1–3 sentences like a coach annotating match video — same style as PLAYER MEMORY examples.",
            f"Player: {player_focus or 'the player'}",
            "",
            "--- PLAYER MEMORY style reference ---",
            mem_block,
            "",
            "--- NOTE TO REWRITE ---",
            note,
        ]
    )
    response = client.responses.parse(
        model=model,
        input=[
            {"role": "system", "content": _soccer_coaching_vision_system()},
            {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
        ],
        text_format=_CoachingNoteRewrite,
    )
    parsed = response.output_parsed
    if parsed is None or not (parsed.coaching_note or "").strip():
        return note
    rewritten = parsed.coaching_note.strip()
    if _coaching_note_has_biomechanical_language(rewritten):
        return note
    return rewritten


def _soccer_coaching_vision_system() -> str:
    return (
        "You are an experienced youth soccer coach reviewing still frames from a match clip. "
        "Your output must read like existing coach annotations in PLAYER MEMORY — not like a physiotherapist or pose-analysis tool.\n\n"
        + _COACHING_STYLE_GOOD_EXAMPLES
        + "\n\n"
        "First establish pitch_location from wide field images (half, channel, zone, coach label). "
        "Then coach tactically: game picture (overload, 3v2, third-man), role, on-ball/off-ball actions, "
        "support angles, pressing, line organization, and decision quality.\n"
        "Category 'body_shape' means open to receive / half-turn to play forward — NEVER describe shoulders, torso, or stance.\n"
        "Forbidden in coaching_note: shoulder(s), torso, knee(s), upright, posture, flexion, squaring, peripheral vision, "
        "athletic stance, body shape is [upright/ready], physical balance, explosiveness from stance, degrees, physiotherapy.\n"
        "Tactical positioning (goal-side, line height, spacing, show inside) is GOOD. Describing how the player's body looks is BAD.\n"
        "If frames only show a player standing, comment on defensive line, marking, cover, or when to step — not anatomy.\n"
        "Prefer coaching that changes future behavior over narrating what already happened."
    )


def _subsample_image_paths(paths: list[Path], max_n: int) -> list[Path]:
    if max_n <= 0 or not paths:
        return []
    if len(paths) <= max_n:
        return list(paths)
    n = len(paths)
    idxs = [round(i * (n - 1) / (max_n - 1)) for i in range(max_n)]
    return [paths[int(i)] for i in idxs]


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
    player_crop_paths: list[Path] | None = None,
    coaching_focus: str | None = None,
    player_memory_context: str | None = None,
    shared_context: str | None = None,
    finder: str = "circle",
    moment_action: str | None = None,
    moment_why: str | None = None,
) -> tuple[CircleSegmentVisionOutput, dict[str, Any]]:
    """Single vision parse for one episode window (circled highlight or OpenAI-picked action)."""
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
    mem_limit = int((os.getenv("FEEDBACK_SEGMENT_MEMORY_MAX_CHARS") or "12000").strip() or "12000")
    org_limit = int((os.getenv("FEEDBACK_SEGMENT_SHARED_MAX_CHARS") or "12000").strip() or "12000")

    if (finder or "circle").strip().lower() in {"openai", "gpt", "vision"}:
        user_lines = [
            "Analyze still frames from one coaching window of a soccer clip (no red-circle overlay is required).",
            f"Sport: {sport or 'Soccer'}. Player focus: {player_focus or 'the named athlete'}.",
            f"Episode {segment_index} of {segment_total}: action window {t_on:.2f}s–{t_off:.2f}s.",
            f"Frames span {t_lo:.2f}s through {t_hi:.2f}s (a little before, the action, and a little after).",
            "Images are chronological. Coach the named player only.",
        ]
        if (moment_action or "").strip():
            user_lines.append(f"Suggested action label from the moment picker: {moment_action.strip()}")
        if (moment_why or "").strip():
            user_lines.append(f"Why this window was picked: {moment_why.strip()}")
    else:
        user_lines = [
            "Analyze still frames from one continuous time window of a soccer performance clip.",
            f"Sport: {sport or 'Soccer'}. Player focus: {player_focus or 'the athlete circled in red when visible'}.",
            f"Episode {segment_index} of {segment_total}: red highlight circle visibility is approximately {t_on:.2f}s–{t_off:.2f}s.",
            f"Frames span {t_lo:.2f}s through {t_hi:.2f}s (includes ~{t_lo:.2f}s–{t_on:.2f}s before the circle reliably appears, the span while it is on, and ~{t_off:.2f}s–{t_hi:.2f}s after it disappears).",
            "Images are chronological. The red circle may only appear on some middle frames.",
        ]
    cf = (coaching_focus or "").strip()
    mem = (player_memory_context or "").strip()
    org = (shared_context or "").strip()
    if mem:
        user_lines.extend(
            [
                "",
                "--- PLAYER MEMORY (PRIMARY STYLE REFERENCE — write coaching_note like these prior annotations) ---",
                mem[:mem_limit],
            ]
        )
    if cf:
        user_lines.extend(
            [
                "",
                "--- ADMIN SESSION BRIEF (role, position, session focus) ---",
                cf[:3000],
            ]
        )
        if coaching_wants_per_circle_feedback(cf):
            user_lines.extend(
                [
                    "",
                    "--- COACHING DIRECTIVE (MANDATORY) ---",
                    "The admin requires one distinct feedback point for EACH separate red-circle highlight. "
                    "This episode window should produce coaching for the circled player at this highlight only — "
                    "do not skip or merge with other highlights.",
                ]
            )
    if org:
        user_lines.extend(
            [
                "",
                "--- SHARED CLUB RUBRIC (vocabulary and standards) ---",
                org[:org_limit],
            ]
        )
    has_crops = bool(player_crop_paths)
    user_lines.extend(
        [
            "",
            "Tasks:",
            "1) From the WIDE field images, set pitch_location: where is the circled player on the pitch?",
            "   Include: camera left/right half, own vs opponent half relative to halfway line, channel (left/center/right),",
            "   vertical third (defensive/middle/attacking), and a coach zone label (e.g. left DM, right wing-back support).",
            "2) Describe the game moment: attack/defend/transition, ball relative to player, on-ball vs off-ball role.",
            "3) Name tactical ideas when visible: overload, 3v2, third-man, 1v1, press trigger, line height, support angle.",
            "4) Choose one primary category and sentiment.",
            "5) Write coaching_note as 1–3 sentences matching PLAYER MEMORY — tactical coaching only, like annotating match video.",
            "Do NOT describe body mechanics (shoulders, torso, knees, posture, upright stance).",
            "Ground only in visible evidence. If unclear, conservative tactical observation only.",
        ]
    )
    if has_crops:
        user_lines.extend(
            [
                "",
                "Image order: WIDE FIELD frames first (pitch context), then PLAYER CROP frames (enlarged circled player from YOLO bbox).",
                "Use wide frames for pitch_location; use crops to confirm which player and their action.",
            ]
        )
    pose_block = (pose_context or "").strip()
    if pose_block and _pose_context_enabled_for_vision():
        user_lines.extend(
            [
                "",
                "Optional low-confidence body-pose hints (do NOT quote angles or anatomy in coaching_note; "
                "only use to infer readiness or balance if frames are ambiguous):",
                pose_block,
            ]
        )
    user_text = "\n".join(user_lines)
    max_wide = int((os.getenv("VIDEO_HIGHLIGHT_VISION_MAX_WIDE_FRAMES") or "4").strip() or "4")
    max_crops = int((os.getenv("VIDEO_HIGHLIGHT_VISION_MAX_CROP_FRAMES") or "2").strip() or "2")
    wide_paths = _subsample_image_paths(frame_paths, max_wide)
    crop_paths = _subsample_image_paths(list(player_crop_paths or []), max_crops)

    content: list[dict[str, Any]] = [{"type": "input_text", "text": user_text}]
    if wide_paths:
        content.append(
            {
                "type": "input_text",
                "text": f"--- WIDE FIELD VIEW ({len(wide_paths)} chronological still(s); use for pitch_location) ---",
            }
        )
        for path in wide_paths:
            content.append(
                {
                    "type": "input_image",
                    "image_url": f"data:image/jpeg;base64,{_encode_image(path)}",
                }
            )
    if crop_paths:
        content.append(
            {
                "type": "input_text",
                "text": (
                    f"--- PLAYER CROP ({len(crop_paths)} still(s); YOLO highlight bbox — circled player enlarged) ---"
                ),
            }
        )
        for path in crop_paths:
            content.append(
                {
                    "type": "input_image",
                    "image_url": f"data:image/jpeg;base64,{_encode_image(path)}",
                }
            )
    if not wide_paths and not crop_paths:
        debug["outcome"] = "error"
        debug["error"] = "no_frames"
        raise RuntimeError("vision_analyze_circle_segment requires at least one frame.")

    system = _soccer_coaching_vision_system()
    try:
        response = client.responses.parse(
            model=model,
            input=[
                {"role": "system", "content": system},
                {"role": "user", "content": content},
            ],
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

    if _coaching_note_has_biomechanical_language(parsed.coaching_note):
        debug["biomechanical_language_detected"] = True
        rewritten = _rewrite_coaching_note_tactical(
            client,
            model,
            note=parsed.coaching_note,
            player_focus=player_focus,
            player_memory_context=player_memory_context,
        )
        if rewritten != parsed.coaching_note:
            parsed = parsed.model_copy(update={"coaching_note": rewritten})
            debug["coaching_note_rewritten"] = True

    debug["outcome"] = "success"
    debug["model"] = model
    debug["wide_frame_count"] = len(wide_paths)
    debug["crop_frame_count"] = len(crop_paths)
    debug["system_message"] = _truncate_for_debug_text(system)
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
        "Write like PLAYER MEMORY coach annotations: tactical themes (support, overloads, third-man, 1v1 skills, "
        "pressing, positioning to the ball). Never biomechanical or posture language.\n\n"
        + _COACHING_STYLE_GOOD_EXAMPLES
        + "\n\nTone from the reference prompt:\n"
        + (prompt_tone or "")[:6000]
    )

    prefix_lines = [
        "Synthesize overall assessment from highlight-circle episodes (each episode analyzed separately below).",
        f"Sport: {sport or 'Soccer'}",
        f"Player focus: {player_focus or 'the athlete'}",
        f"Video duration (seconds): {duration_sec:.2f}",
        f"Analysis scope: {analysis_scope or 'Full clip.'}",
        f"Admin session brief (role/position/focus): {coaching_focus or 'Balanced technical and tactical feedback.'}",
        "Judge episodes against the admin brief and shared rubric when supported by evidence; do not invent observations.",
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


def analyze_video_direct_openai(
    *,
    video_url: str,
    sport: str,
    player_focus: str,
    duration_sec: float,
    analysis_scope: str,
    coaching_focus: str,
    player_memory_context: str | None = None,
    shared_context: str | None = None,
    base_dir: Path | None = None,
) -> tuple[VideoFeedbackReview, dict[str, Any]]:
    """
    Send match video directly to OpenAI vision (file upload when small enough, else dense frame grid).
    Skips YOLO / local circle detection — the model chooses coaching moments from the full clip.
    """
    from agents.feedback.highlight.cache import get_local_video
    from agents.feedback.video_utils import extract_uniform_frames_in_range, probe_duration

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing. Add it to your environment or .env file.")

    model = (
        os.getenv("VIDEO_OPENAI_DIRECT_MODEL")
        or os.getenv("OPENAI_MODEL")
        or "gpt-4.1-mini"
    ).strip()
    client = OpenAI(api_key=api_key)
    debug: dict[str, Any] = {"mode": "direct_openai_video", "model": model, "outcome": "pending"}

    duration = duration_sec if duration_sec > 0 else probe_duration(video_url)
    cached = get_local_video(video_url)
    local_path = cached.cache_path if cached.cache_path and cached.cache_path.exists() else None
    if local_path is None and not video_url.startswith(("http://", "https://")):
        candidate = Path(video_url.replace("file://", ""))
        if candidate.exists():
            local_path = candidate

    max_mb = float((os.getenv("VIDEO_OPENAI_DIRECT_MAX_MB") or "25").strip() or "25")
    use_file_upload = False
    uploaded_file_id: str | None = None
    if local_path is not None:
        size_mb = local_path.stat().st_size / (1024 * 1024)
        debug["local_video_mb"] = round(size_mb, 2)
        if size_mb <= max_mb:
            try:
                with local_path.open("rb") as handle:
                    uploaded = client.files.create(file=handle, purpose="vision")
                uploaded_file_id = uploaded.id
                use_file_upload = True
                debug["input_mode"] = "openai_file_upload"
                debug["file_id"] = uploaded_file_id
            except Exception as exc:  # noqa: BLE001
                debug["file_upload_error"] = str(exc)[:500]
                use_file_upload = False

    mem = (player_memory_context or "").strip()
    org = (shared_context or "").strip()
    cf = (coaching_focus or "").strip()
    circle_mode = coaching_wants_per_circle_feedback(coaching_focus)

    prompt_lines = [
        "You are a youth soccer video coach. Analyze this match clip and produce structured coaching feedback.",
        f"Sport: {sport or 'Soccer'}",
        f"Player focus: {player_focus or 'the named athlete'}",
        f"Video duration (seconds): {duration:.1f}",
        f"Analysis scope: {analysis_scope or 'Full visible clip.'}",
    ]
    if cf:
        prompt_lines.extend(["", "--- ADMIN SESSION BRIEF ---", cf[:3000]])
    if circle_mode:
        prompt_lines.extend(
            [
                "",
                "Create one coaching moment for EACH time the named player is highlighted with a red circle overlay.",
                "Do not merge separate circle highlights into a single moment.",
            ]
        )
    else:
        prompt_lines.append(
            "Create coaching moments for the named player at tactically meaningful points (on and off the ball)."
        )
    if mem:
        prompt_lines.extend(
            [
                "",
                "--- PLAYER MEMORY (style reference — match this coaching voice) ---",
                mem[:12000],
            ]
        )
    if org:
        prompt_lines.extend(
            [
                "",
                "--- SHARED CLUB RUBRIC ---",
                org[:12000],
            ]
        )
    prompt_lines.extend(
        [
            "",
            "Return JSON with video_summary, overall_assessment, and moments (timestamp_sec, category, sentiment, coaching_note).",
            "Ground every note in visible evidence. Tactical coaching only — no body-mechanics anatomy.",
        ]
    )
    user_text = "\n".join(prompt_lines)

    content: list[dict[str, Any]] = [{"type": "input_text", "text": user_text}]
    if use_file_upload and uploaded_file_id:
        content.append({"type": "input_file", "file_id": uploaded_file_id})
    else:
        debug["input_mode"] = "dense_frame_grid"
        max_frames = int((os.getenv("VIDEO_OPENAI_DIRECT_MAX_FRAMES") or "36").strip() or "36")
        frame_dir = (base_dir or Path("/tmp")) / "direct_openai_frames"
        frame_dir.mkdir(parents=True, exist_ok=True)
        frames = extract_uniform_frames_in_range(
            video_url,
            0.0,
            duration,
            frame_dir,
            "direct",
            max_frames=max_frames,
            frame_width=int((os.getenv("VIDEO_OPENAI_DIRECT_FRAME_WIDTH") or "720").strip() or "720"),
            video_duration_sec=duration,
        )
        debug["frame_count"] = len(frames)
        for frame in frames:
            content.append({"type": "input_text", "text": f"Frame t={frame.timestamp_sec:.2f}s"})
            content.append(
                {
                    "type": "input_image",
                    "image_url": f"data:image/jpeg;base64,{_encode_image(frame.image_path)}",
                }
            )
        if not frames:
            raise RuntimeError("Could not extract frames for direct OpenAI video analysis.")

    system = _soccer_coaching_vision_system()
    response = client.responses.parse(
        model=model,
        input=[
            {"role": "system", "content": system},
            {"role": "user", "content": content},
        ],
        text_format=VideoFeedbackReview,
    )
    parsed = response.output_parsed
    if parsed is None:
        debug["outcome"] = "error"
        raise RuntimeError("Direct OpenAI video analysis returned no parsed payload.")

    debug["outcome"] = "success"
    debug["moment_count"] = len(parsed.moments or [])
    debug["system_message"] = _truncate_for_debug_text(system)
    debug["user_message_text"] = _truncate_for_debug_text(user_text)
    return parsed, debug
