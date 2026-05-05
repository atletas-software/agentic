from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import List, Literal, Optional

from openai import OpenAI
from pydantic import BaseModel

from agents.feedback.models import VideoFeedbackReview


class PlayerLocalization(BaseModel):
    found: bool
    confidence: Literal["low", "medium", "high"]
    center_x: float
    center_y: float
    radius: float
    note: str


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
    return base64.b64encode(path.read_bytes()).decode("utf-8")


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
) -> VideoFeedbackReview:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing. Add it to your environment or .env file.")

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    client = OpenAI(api_key=api_key)

    user_text = "\n".join(
        [
            "Analyze this soccer player review video from storyboard images.",
            f"Sport: {sport}",
            f"Player focus: {player_focus or 'Unknown player'}",
            f"Video duration in seconds: {duration_sec:.2f}",
            f"Analysis scope: {analysis_scope or 'Review the full visible video.'}",
            f"Coaching focus: {coaching_focus or 'Balanced technical and tactical feedback.'}",
            f"Analysis mode: {analysis_mode}",
            f"Allowed feedback timestamps in seconds: {_format_allowed_timestamps(allowed_timestamps)}",
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
        ]
    )

    content = [{"type": "input_text", "text": user_text}]
    for path in storyboard_paths:
        content.append(
            {
                "type": "input_image",
                "image_url": f"data:image/jpeg;base64,{_encode_image(path)}",
            }
        )

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
    return parsed


def _format_allowed_timestamps(allowed_timestamps: Optional[List[float]]) -> str:
    if not allowed_timestamps:
        return "none provided"
    return ", ".join(f"{timestamp:.2f}" for timestamp in allowed_timestamps)


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
