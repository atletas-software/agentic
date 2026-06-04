"""Derive posture metrics from COCO-17 pose JSON and evaluate against a YAML KB."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Iterable

SCHEMA_VERSION = 2

# COCO body keypoints only (ids 5–16). Face landmarks may exist in pose JSON but are
# never used for posture metrics or agent feedback context.
BODY_KEYPOINT_INDEX: dict[str, int] = {
    "left_shoulder": 5,
    "right_shoulder": 6,
    "left_elbow": 7,
    "right_elbow": 8,
    "left_wrist": 9,
    "right_wrist": 10,
    "left_hip": 11,
    "right_hip": 12,
    "left_knee": 13,
    "right_knee": 14,
    "left_ankle": 15,
    "right_ankle": 16,
}


def load_posture_kb(kb_path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("PyYAML is required. Install with: pip install pyyaml") from exc
    return yaml.safe_load(kb_path.read_text(encoding="utf-8"))


def _kp(
    landmarks: list[dict[str, Any]],
    name: str,
    min_visibility: float = 0.20,
) -> tuple[float, float] | None:
    idx = BODY_KEYPOINT_INDEX[name]
    for lm in landmarks:
        if lm.get("id") != idx:
            continue
        if lm.get("visibility", 0.0) < min_visibility:
            return None
        return (float(lm["x"]), float(lm["y"]))
    return None


def _angle_deg(p1, p2, p3) -> float | None:
    if p1 is None or p2 is None or p3 is None:
        return None
    v1x, v1y = p1[0] - p2[0], p1[1] - p2[1]
    v2x, v2y = p3[0] - p2[0], p3[1] - p2[1]
    n1 = math.hypot(v1x, v1y)
    n2 = math.hypot(v2x, v2y)
    if n1 == 0 or n2 == 0:
        return None
    cos_a = max(-1.0, min(1.0, (v1x * v2x + v1y * v2y) / (n1 * n2)))
    return math.degrees(math.acos(cos_a))


def _torso_tilt_from_vertical(hip_low, shoulder_high) -> float | None:
    if hip_low is None or shoulder_high is None:
        return None
    dx = abs(shoulder_high[0] - hip_low[0])
    dy = abs(hip_low[1] - shoulder_high[1])
    if dx == 0 and dy == 0:
        return None
    return math.degrees(math.atan2(dx, dy))


def _shoulder_tilt_deg(left_shoulder, right_shoulder) -> float | None:
    if left_shoulder is None or right_shoulder is None:
        return None
    dx = right_shoulder[0] - left_shoulder[0]
    dy = right_shoulder[1] - left_shoulder[1]
    if dx == 0 and dy == 0:
        return None
    return math.degrees(math.atan2(dy, dx))


def compute_frame_metrics(landmarks: list[dict[str, Any]]) -> dict[str, float | None]:
    ls = _kp(landmarks, "left_shoulder")
    rs = _kp(landmarks, "right_shoulder")
    lh = _kp(landmarks, "left_hip")
    rh = _kp(landmarks, "right_hip")
    lk = _kp(landmarks, "left_knee")
    rk = _kp(landmarks, "right_knee")
    la = _kp(landmarks, "left_ankle")
    ra = _kp(landmarks, "right_ankle")
    le = _kp(landmarks, "left_elbow")
    re = _kp(landmarks, "right_elbow")
    lw = _kp(landmarks, "left_wrist")
    rw = _kp(landmarks, "right_wrist")

    knee_left = _angle_deg(lh, lk, la)
    knee_right = _angle_deg(rh, rk, ra)

    metrics: dict[str, float | None] = {
        "knee_flex_left": knee_left,
        "knee_flex_right": knee_right,
        "hip_flex_left": _angle_deg(ls, lh, lk),
        "hip_flex_right": _angle_deg(rs, rh, rk),
        "elbow_flex_left": _angle_deg(ls, le, lw),
        "elbow_flex_right": _angle_deg(rs, re, rw),
        "shoulder_tilt_deg": _shoulder_tilt_deg(ls, rs),
    }

    if knee_left is not None and knee_right is not None:
        metrics["knee_flex_diff"] = abs(knee_left - knee_right)
    else:
        metrics["knee_flex_diff"] = None

    if lh is not None and rh is not None and ls is not None and rs is not None:
        hip_mid = ((lh[0] + rh[0]) / 2.0, (lh[1] + rh[1]) / 2.0)
        sh_mid = ((ls[0] + rs[0]) / 2.0, (ls[1] + rs[1]) / 2.0)
        metrics["torso_tilt_from_vertical"] = _torso_tilt_from_vertical(hip_mid, sh_mid)
    else:
        metrics["torso_tilt_from_vertical"] = None

    return metrics


def aggregate(per_frame: list[dict[str, float | None]], mode: str = "mean") -> dict[str, float | None]:
    names: set[str] = set()
    for m in per_frame:
        names.update(m.keys())

    out: dict[str, float | None] = {}
    for name in names:
        values = [m[name] for m in per_frame if m.get(name) is not None]
        if not values:
            out[name] = None
            continue
        if mode == "median":
            sv = sorted(values)
            out[name] = sv[len(sv) // 2]
        elif mode == "min":
            out[name] = min(values)
        elif mode == "max":
            out[name] = max(values)
        elif mode == "range":
            out[name] = max(values) - min(values)
        else:
            out[name] = sum(values) / len(values)

    return out


def evaluate_rule(value: float | None, rule: dict[str, Any]) -> tuple[str, str]:
    metric = rule.get("metric", "?")

    if value is None:
        return (
            "no_data",
            f"No data available for '{metric}'. Keypoints may have been low-visibility.",
        )

    target_min = rule.get("target_min")
    target_max = rule.get("target_max")

    if target_min is not None and value < target_min:
        return (
            "warn",
            rule.get(
                "warn_below",
                f"{metric}={value:.1f} below target_min={target_min}.",
            ),
        )

    if target_max is not None and value > target_max:
        return (
            "warn",
            rule.get(
                "warn_above",
                f"{metric}={value:.1f} above target_max={target_max}.",
            ),
        )

    return (
        "ok",
        rule.get("ok", f"{metric}={value:.1f} within target range."),
    )


def group_events(
    detected_frames: list[dict[str, Any]],
    *,
    max_frame_gap: int | None = None,
    fps: float | None = None,
) -> list[dict[str, Any]]:
    """Group consecutive detected frames into one red-circle highlight event.

    ``track_id`` is not used for grouping (ByteTrack IDs can restart mid-span).
    """
    if not detected_frames:
        return []

    if max_frame_gap is None:
        if fps and fps > 0:
            max_frame_gap = max(1, int(round(fps * 0.5)))
        else:
            max_frame_gap = 15

    events: list[dict[str, Any]] = []
    cur: dict[str, Any] | None = None

    for f in detected_frames:
        gap = f["frame_index"] - cur["end_frame"] if cur is not None else 0
        if cur is None or gap > max_frame_gap:
            if cur is not None:
                events.append(cur)
            cur = {
                "event_index": len(events) + 1,
                "track_ids": [],
                "start_frame": f["frame_index"],
                "end_frame": f["frame_index"],
                "start_timestamp_sec": f["timestamp_sec"],
                "end_timestamp_sec": f["timestamp_sec"],
                "frames": [f],
            }
        else:
            cur["end_frame"] = f["frame_index"]
            cur["end_timestamp_sec"] = f["timestamp_sec"]
            cur["frames"].append(f)

        tid = f.get("track_id")
        if tid is not None and tid not in cur["track_ids"]:
            cur["track_ids"].append(tid)

    if cur is not None:
        events.append(cur)

    for i, ev in enumerate(events, start=1):
        ev["event_index"] = i
        mid = ev["frames"][len(ev["frames"]) // 2]
        ev["anchor_timestamp_sec"] = round(
            (ev["start_timestamp_sec"] + ev["end_timestamp_sec"]) / 2.0,
            3,
        )
        ev["highlight_bbox"] = mid.get("highlight_bbox")
        ev["pose_quality"] = mid.get("pose_quality")
        ev["pose_visibility_mean"] = mid.get("pose_visibility_mean")

    return events


def evaluate_event(event: dict[str, Any], kb: dict[str, Any]) -> dict[str, Any]:
    per_frame: list[dict[str, float | None]] = []
    for frame in event["frames"]:
        if frame.get("keypoints") is None:
            continue
        per_frame.append(compute_frame_metrics(frame["keypoints"]))

    if not per_frame:
        return {
            "frames_used": 0,
            "metrics": {},
            "findings": [],
            "summary_status": "no_data",
        }

    aggregated_mean = aggregate(per_frame, mode="mean")

    findings: list[dict[str, Any]] = []
    for rule in kb.get("rules", []):
        agg_mode = rule.get("aggregate", "mean")
        if agg_mode == "mean":
            value = aggregated_mean.get(rule["metric"])
        else:
            value = aggregate(per_frame, mode=agg_mode).get(rule["metric"])

        status, message = evaluate_rule(value, rule)
        findings.append(
            {
                "rule_id": rule.get("id"),
                "metric": rule["metric"],
                "aggregate": agg_mode,
                "value": round(value, 2) if value is not None else None,
                "target_min": rule.get("target_min"),
                "target_max": rule.get("target_max"),
                "status": status,
                "message": message,
            }
        )

    return {
        "frames_used": len(per_frame),
        "metrics": {
            k: round(v, 2) if v is not None else None
            for k, v in aggregated_mean.items()
        },
        "findings": findings,
        "summary_status": _summarize_status(findings),
    }


def _summarize_status(findings: Iterable[dict[str, Any]]) -> str:
    statuses = [f["status"] for f in findings]
    if "warn" in statuses:
        return "warn"
    if "ok" in statuses:
        return "ok"
    return "no_data"


def rule_based_coaching_note(
    findings: list[dict[str, Any]],
    kb: dict[str, Any],
    *,
    summary_status: str,
) -> str:
    defaults = kb.get("narrative_defaults") or {}
    warns = [f["message"] for f in findings if f.get("status") == "warn"]
    if warns:
        return " ".join(warns[:3])
    if summary_status == "ok":
        return str(defaults.get("all_ok", "Posture looks solid across this highlight."))
    return str(defaults.get("no_findings", "Not enough pose data for this highlight."))


def generate_feedback_payload(
    pose_data: dict[str, Any],
    kb: dict[str, Any],
    *,
    pose_json_path: str | None = None,
    kb_path: str | None = None,
) -> dict[str, Any]:
    """Full feedback document from pose JSON (schema v3 from detect_pose_video_yolov8)."""
    frames = pose_data.get("pose_results", [])
    detected = [f for f in frames if f.get("detected")]
    fps = float(pose_data.get("fps") or 30.0)
    events = group_events(detected, fps=fps)

    event_outputs: list[dict[str, Any]] = []
    for ev in events:
        result = evaluate_event(ev, kb)
        note = rule_based_coaching_note(
            result["findings"],
            kb,
            summary_status=result["summary_status"],
        )
        event_outputs.append(
            {
                "event_index": ev["event_index"],
                "track_ids": ev.get("track_ids") or [],
                "start_frame": ev["start_frame"],
                "end_frame": ev["end_frame"],
                "start_timestamp_sec": ev["start_timestamp_sec"],
                "end_timestamp_sec": ev["end_timestamp_sec"],
                "anchor_timestamp_sec": ev["anchor_timestamp_sec"],
                "highlight_bbox": ev.get("highlight_bbox"),
                "pose_quality": ev.get("pose_quality"),
                "pose_visibility_mean": ev.get("pose_visibility_mean"),
                "frames_used": result["frames_used"],
                "summary_status": result["summary_status"],
                "metrics": result["metrics"],
                "findings": result["findings"],
                "coaching_note": note,
            }
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "video": pose_data.get("video"),
        "fps": fps,
        "frame_width": pose_data.get("frame_width"),
        "frame_height": pose_data.get("frame_height"),
        "pose_json": pose_json_path,
        "kb": kb_path,
        "kb_description": kb.get("description"),
        "frames_total": len(frames),
        "frames_detected": len(detected),
        "event_count": len(event_outputs),
        "events": event_outputs,
    }


def load_pose_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def format_pose_context_for_agent(event: dict[str, Any]) -> str:
    """Text block for the feedback agent: posture metrics + KB findings (no raw keypoints)."""
    metrics = event.get("metrics") or {}
    metrics_line = ", ".join(f"{k}={v}°" for k, v in metrics.items() if v is not None)
    lines = [
        f"Pose snapshots: {event.get('frames_used', 0)} frames, quality={event.get('pose_quality')}, "
        f"visibility_mean={event.get('pose_visibility_mean')}.",
    ]
    if metrics_line:
        lines.append(f"Aggregated 2D angles (image plane): {metrics_line}.")
    findings = event.get("findings") or []
    if findings:
        lines.append("Posture rule checks:")
        for f in findings:
            lines.append(f"  - [{f.get('status')}] {f.get('message')}")
    else:
        lines.append("Posture rule checks: insufficient body keypoint visibility.")
    return "\n".join(lines)
