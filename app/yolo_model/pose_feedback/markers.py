"""Convert pose-feedback events into review markers (one per red-circle highlight)."""

from __future__ import annotations

from typing import Any


def _marker_label(category: str, sentiment: str) -> str:
    prefix = {
        "positive": "Positive",
        "corrective": "Improve",
        "mixed": "Review",
    }.get(sentiment, "Review")
    return f"{prefix}: {category.replace('_', ' ').title()}"


def _sentiment_from_status(summary_status: str, findings: list[dict[str, Any]]) -> str:
    if summary_status == "warn":
        return "corrective"
    if summary_status == "ok":
        return "positive"
    warns = sum(1 for f in findings if f.get("status") == "warn")
    oks = sum(1 for f in findings if f.get("status") == "ok")
    if warns and oks:
        return "mixed"
    if warns:
        return "corrective"
    if oks:
        return "positive"
    return "mixed"


def events_to_review_markers(
    events: list[dict[str, Any]],
    *,
    duration_sec: float,
    sport: str = "Soccer",
) -> list[dict[str, Any]]:
    """Map pose-feedback events to markers compatible with agents/feedback reviews."""
    markers: list[dict[str, Any]] = []
    used_ts: set[float] = set()

    for ev in events:
        ts = round(float(ev.get("anchor_timestamp_sec", 0.0)), 2)
        ts = max(0.0, min(ts, max(duration_sec - 0.25, 0.0) if duration_sec else ts))
        while ts in used_ts:
            ts = round(ts + 0.05, 2)
        used_ts.add(ts)

        findings = ev.get("findings") or []
        summary_status = str(ev.get("summary_status") or "no_data")
        category = str(ev.get("category") or "body_shape")
        sentiment = str(ev.get("sentiment") or _sentiment_from_status(summary_status, findings))

        markers.append(
            {
                "id": len(markers) + 1,
                "timestamp_sec": ts,
                "category": category,
                "sentiment": sentiment,
                "label": _marker_label(category, sentiment),
                "coaching_note": (ev.get("coaching_note") or "").strip()
                or "Pose data recorded; no coaching note generated.",
                "reference_clip": None,
                "diagram_request": None,
                "freeze_frame_request": {
                    "title": f"Highlight @ {ts:.1f}s",
                    "reason": "Red-circle highlight with YOLOv8 pose snapshot.",
                },
                "pose_marker": True,
                "pose_event": {
                    "event_index": ev.get("event_index"),
                    "start_frame": ev.get("start_frame"),
                    "end_frame": ev.get("end_frame"),
                    "start_timestamp_sec": ev.get("start_timestamp_sec"),
                    "end_timestamp_sec": ev.get("end_timestamp_sec"),
                    "frames_used": ev.get("frames_used"),
                    "summary_status": summary_status,
                    "metrics": ev.get("metrics"),
                    "findings": findings,
                    "highlight_bbox": ev.get("highlight_bbox"),
                    "pose_quality": ev.get("pose_quality"),
                    "pose_visibility_mean": ev.get("pose_visibility_mean"),
                    "track_ids": ev.get("track_ids"),
                },
                "sport": sport,
            }
        )

    return markers
