"""
Roadmap for an advanced soccer tactical pipeline (highlight → track → coach).

Today, the feedback agent uses ffmpeg + sparse red-circle detection + storyboards; see
`video_utils.py` and `review_agent.build_review`. This module documents the intended modular
stages for a future CV/ML stack (GPU tracking, overlays, async jobs) without importing heavy deps.

Planned stages (align with product spec):
  1. detection       — first frame with highlight circle; timestamp T
  2. windowing       — analysis_start/end around T (e.g. T±2s); extract frames
  3. association     — player nearest circle center; identity across frames
  4. tracking        — trajectory, accel, ball/teammate/opponent relations
  5. tactics         — positioning, lanes, pressure, transitions (pre/event/post)
  6. visualization   — annotated clip, trails / heatmap (optional)
  7. feedback        — structured JSON + coaching copy + confidence scores

Integrate behind a single entrypoint when implemented, e.g. `run_tactical_pipeline(...) -> TacticalArtifacts`,
then pass condensed summaries + clip URLs into the LLM *after* raw video frames, keeping
player memory and shared rubric as separate text blocks as today.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


PIPELINE_SPEC_VERSION = "0.1-roadmap-only"


@dataclass(frozen=True)
class TacticalPipelinePlaceholder:
    """Returned until a real CV pipeline is wired."""

    version: str = PIPELINE_SPEC_VERSION
    note: str = "Stub — use video_utils + storyboards for production today."


def describe_pipeline_stages() -> list[dict[str, Any]]:
    """Human-readable stage list for logs or admin UI."""
    return [
        {"id": "detection", "summary": "Locate first highlight circle; record T."},
        {"id": "windowing", "summary": "Sample frames in [T-2s, T+2s] (configurable)."},
        {"id": "association", "summary": "Bind circle to player identity."},
        {"id": "tracking", "summary": "Multi-frame motion + ball/team context."},
        {"id": "tactics", "summary": "Pre/event/post tactical interpretation."},
        {"id": "visualization", "summary": "Overlays / annotated export (optional)."},
        {"id": "feedback", "summary": "Coaching JSON + narrative + confidence."},
    ]
