from __future__ import annotations

import re
from typing import Final

# Allowed chunk_type values (Pinecone metadata filter / UX).
CHUNK_TYPES: Final[tuple[str, ...]] = (
    "overall_summary",
    "technical_skills",
    "dribbling",
    "passing",
    "ball_control",
    "decision_making",
    "defending",
    "physicality",
    "speed",
    "mentality",
    "leadership",
    "fitness",
    "positioning",
    "movement",
    "coach_feedback",
    "video_analysis",
    "development_areas",
    "strengths",
    "weaknesses",
)

_KEYWORD_CHUNK: Final[list[tuple[str, tuple[str, ...]]]] = [
    ("passing", ("pass", "passing", "distribution")),
    ("dribbling", ("dribble", "dribbling", "1v1", "take on")),
    ("ball_control", ("first touch", "touch", "control", "ball control")),
    ("decision_making", ("decision", "awareness", "reading the game")),
    ("defending", ("defend", "defending", "tackle", "press")),
    ("physicality", ("physical", "strength", "duel", "aerial")),
    ("speed", ("speed", "pace", "acceleration")),
    ("mentality", ("mentality", "composure", "confidence", "attitude")),
    ("leadership", ("leadership", "captain", "communication")),
    ("fitness", ("fitness", "endurance", "conditioning")),
    ("positioning", ("positioning", "shape", "spacing")),
    ("movement", ("movement", "runs", "off the ball")),
    ("development_areas", ("improve", "development", "needs to", "work on")),
    ("strengths", ("strength", "strong", "excel")),
    ("weaknesses", ("weak", "struggle", "vulnerable")),
    ("coach_feedback", ("coach", "feedback", "session")),
    ("video_analysis", ("video", "clip", "footage")),
]

_STOP: Final[set[str]] = {
    "that",
    "this",
    "with",
    "from",
    "have",
    "been",
    "were",
    "their",
    "they",
    "them",
    "player",
    "shows",
    "good",
    "very",
    "also",
    "into",
    "when",
    "what",
    "will",
    "would",
    "could",
    "should",
    "about",
    "after",
    "before",
    "during",
    "while",
    "where",
    "which",
    "there",
    "these",
    "those",
}


def infer_chunk_type(chunk_text: str) -> str:
    t = (chunk_text or "").lower()
    if not t.strip():
        return "overall_summary"
    for ctype, words in _KEYWORD_CHUNK:
        if any(w in t for w in words):
            return ctype if ctype in CHUNK_TYPES else "technical_skills"
    if "summary" in t[:200] or t.startswith("player:"):
        return "overall_summary"
    return "technical_skills"


def extract_tags(chunk_text: str, *, max_tags: int = 8) -> list[str]:
    raw = re.findall(r"[a-z][a-z\-]{2,}", (chunk_text or "").lower())
    out: list[str] = []
    seen: set[str] = set()
    for w in raw:
        if w in _STOP or w in seen:
            continue
        seen.add(w)
        out.append(w)
        if len(out) >= max_tags:
            break
    return out


def format_embedding_input(*, summary: str, chunk_text: str, max_chars: int = 12000) -> str:
    """What gets embedded: cleaned summary + chunk (never raw HTML/JSON here)."""
    s = (summary or "").strip()
    c = (chunk_text or "").strip()
    if s and c:
        body = f"{s}\n\n{c}".strip()
    else:
        body = s or c
    if len(body) > max_chars:
        return body[:max_chars]
    return body
