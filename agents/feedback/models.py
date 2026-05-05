from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, Field


Category = Literal[
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

Sentiment = Literal["positive", "corrective", "mixed"]


class ReferenceClip(BaseModel):
    title: str
    teaching_point: str
    url: Optional[str] = None


class DiagramRequest(BaseModel):
    title: str
    prompt: str


class FreezeFrameRequest(BaseModel):
    title: str
    reason: str


class ReviewMoment(BaseModel):
    timestamp_sec: float
    category: Category
    sentiment: Sentiment = "mixed"
    coaching_note: str
    reference_clip: Optional[ReferenceClip] = None
    diagram_request: Optional[DiagramRequest] = None
    freeze_frame_request: Optional[FreezeFrameRequest] = None


class VideoSummary(BaseModel):
    sport: str
    player_focus: str
    duration_sec: float
    analysis_scope: str


class OverallAssessment(BaseModel):
    strengths: List[str] = Field(default_factory=list)
    improvements: List[str] = Field(default_factory=list)
    next_focus: List[str] = Field(default_factory=list)


class VideoFeedbackReview(BaseModel):
    video_summary: VideoSummary
    overall_assessment: OverallAssessment
    moments: List[ReviewMoment]
