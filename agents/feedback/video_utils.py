from __future__ import annotations

import math
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont


@dataclass
class FrameAsset:
    timestamp_sec: float
    image_path: Path
    circle_found: bool = False
    circle_center_x: float = 0.0
    circle_center_y: float = 0.0
    circle_radius: float = 0.0


def require_binary(name: str) -> None:
    if shutil.which(name) is None:
        raise RuntimeError(
            f"Required binary '{name}' was not found. Install it first, for example with 'brew install ffmpeg'."
        )


def probe_duration(video_url: str) -> float:
    require_binary("ffprobe")
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        video_url,
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return float(result.stdout.strip())


def extract_frames(
    video_url: str,
    output_dir: Path,
    max_frames: int = 18,
    frame_width: int = 960,
) -> tuple[float, list[FrameAsset]]:
    require_binary("ffmpeg")
    duration = probe_duration(video_url)
    output_dir.mkdir(parents=True, exist_ok=True)

    if duration <= 0:
        raise RuntimeError("Video duration is invalid.")

    count = min(max_frames, max(8, math.ceil(duration / 12)))
    gap = duration / (count + 1)

    assets: list[FrameAsset] = []
    for index in range(count):
        timestamp = round((index + 1) * gap, 2)
        image_path = output_dir / f"frame_{index + 1:02d}.jpg"
        cmd = [
            "ffmpeg",
            "-y",
            "-ss",
            str(timestamp),
            "-i",
            video_url,
            "-frames:v",
            "1",
            "-vf",
            f"scale={frame_width}:-1",
            str(image_path),
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        _stamp_timestamp(image_path, timestamp)
        assets.append(FrameAsset(timestamp_sec=timestamp, image_path=image_path))

    return duration, assets


def create_storyboards(frame_assets: list[FrameAsset], output_dir: Path) -> list[Path]:
    if not frame_assets:
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    pages: list[Path] = []
    chunk_size = 6
    rows = 3
    cols = 2

    for chunk_index in range(0, len(frame_assets), chunk_size):
        chunk = frame_assets[chunk_index : chunk_index + chunk_size]
        images = [Image.open(item.image_path).convert("RGB") for item in chunk]
        thumb_w = 540
        thumb_h = 304
        canvas = Image.new("RGB", (cols * thumb_w, rows * thumb_h), color=(12, 18, 28))
        for idx, image in enumerate(images):
            row = idx // cols
            col = idx % cols
            fitted = image.copy()
            fitted.thumbnail((thumb_w - 10, thumb_h - 10))
            x = col * thumb_w + (thumb_w - fitted.width) // 2
            y = row * thumb_h + (thumb_h - fitted.height) // 2
            canvas.paste(fitted, (x, y))
        page_path = output_dir / f"storyboard_{len(pages) + 1:02d}.jpg"
        canvas.save(page_path, quality=92)
        pages.append(page_path)

    return pages


def enrich_highlight_temporal_context(
    video_url: str,
    frame_assets: list[FrameAsset],
    duration_sec: float,
    output_dir: Path,
    *,
    window_sec: float = 2.0,
    step_sec: float = 1.0,
    max_frames: int = 48,
) -> list[FrameAsset]:
    """
    Around each frame with a detected highlight circle, sample additional full-frame
    timestamps from T-window .. T+window (step_sec), so the model sees pre/post context.

    Disabled when window_sec <= 0. Caps total frames by dropping non-circle extras first.
    """
    if window_sec <= 0 or duration_sec <= 0 or not frame_assets:
        return frame_assets

    output_dir.mkdir(parents=True, exist_ok=True)
    eps = 0.14
    out: list[FrameAsset] = list(frame_assets)

    def has_near(ts: float, pool: list[FrameAsset]) -> bool:
        return any(abs(a.timestamp_sec - ts) < eps for a in pool)

    steps = int(round((2 * window_sec) / max(step_sec, 0.25))) + 1
    offsets: list[float] = []
    for i in range(steps):
        off = round(-window_sec + i * step_sec, 2)
        if abs(off) <= 1e-6:
            continue
        offsets.append(off)

    for anchor in frame_assets:
        if not anchor.circle_found:
            continue
        t0 = float(anchor.timestamp_sec)
        for off in offsets:
            t = round(max(0.0, min(duration_sec - 0.05, t0 + off)), 2)
            if has_near(t, out):
                continue
            safe = f"{t:.2f}".replace(".", "_")
            path = output_dir / f"context_{safe}.jpg"
            extract_frame_at_timestamp(video_url, t, path, frame_width=960)
            _stamp_timestamp(path, t)
            out.append(FrameAsset(timestamp_sec=t, image_path=path, circle_found=False))

    out.sort(key=lambda a: a.timestamp_sec)
    if len(out) <= max_frames:
        return out

    circles = [a for a in out if a.circle_found]
    others = [a for a in out if not a.circle_found]
    budget = max(0, max_frames - len(circles))
    if budget >= len(others):
        return circles + others
    step = max(1, len(others) // budget)
    picked = [others[i] for i in range(0, len(others), step)][:budget]
    merged = circles + picked
    merged.sort(key=lambda a: a.timestamp_sec)
    return merged


def group_frames_by_highlight_anchor(
    frames: list[FrameAsset],
    *,
    window_sec: float,
) -> list[tuple[float, list[FrameAsset]]]:
    """
    Group stills per detected highlight (circle_found): all frames with timestamps in
    [T - window_sec, T + window_sec]. Anchors are unique circle timestamps (sorted).
    """
    if window_sec <= 0 or not frames:
        return []
    anchors = sorted({round(f.timestamp_sec, 2) for f in frames if f.circle_found})
    if not anchors:
        return []
    eps = 0.25
    groups: list[tuple[float, list[FrameAsset]]] = []
    for T in anchors:
        lo, hi = T - window_sec - eps, T + window_sec + eps
        grp = [f for f in frames if lo <= f.timestamp_sec <= hi]
        grp.sort(key=lambda x: x.timestamp_sec)
        seen: set[Path] = set()
        uniq: list[FrameAsset] = []
        for f in grp:
            if f.image_path in seen:
                continue
            seen.add(f.image_path)
            uniq.append(f)
        if uniq:
            groups.append((T, uniq))
    return groups


def extract_circled_frames(
    video_url: str,
    output_dir: Path,
    *,
    scan_interval_sec: float = 30.0,
    max_frames: int = 18,
    max_scan_frames: int = 36,
    frame_width: int = 854,
) -> tuple[float, list[FrameAsset]]:
    require_binary("ffmpeg")
    duration = probe_duration(video_url)
    output_dir.mkdir(parents=True, exist_ok=True)

    count = min(max_scan_frames, max(8, math.ceil(duration / scan_interval_sec)))
    timestamps = [round((index + 1) * duration / (count + 1), 2) for index in range(count)]

    detected: list[FrameAsset] = []
    for idx, timestamp in enumerate(timestamps, start=1):
        image_path = output_dir / f"sample_{idx:05d}.jpg"
        extract_frame_at_timestamp(video_url, timestamp, image_path, frame_width=frame_width)
        circle = detect_red_circle(image_path)
        if not circle["found"]:
            continue
        _stamp_timestamp(image_path, timestamp)
        detected.append(
            FrameAsset(
                timestamp_sec=timestamp,
                image_path=image_path,
                circle_found=True,
                circle_center_x=float(circle["center_x"]),
                circle_center_y=float(circle["center_y"]),
                circle_radius=float(circle["radius"]),
            )
        )

    if len(detected) <= max_frames:
        return duration, detected

    step = (len(detected) - 1) / max(max_frames - 1, 1)
    selected = [detected[round(i * step)] for i in range(max_frames)]
    return duration, selected


def create_focus_crops(frame_assets: list[FrameAsset], output_dir: Path) -> list[FrameAsset]:
    output_dir.mkdir(parents=True, exist_ok=True)
    focused: list[FrameAsset] = []

    for item in frame_assets:
        if not item.circle_found:
            focused.append(item)
            continue

        image = Image.open(item.image_path).convert("RGB")
        width, height = image.size
        cx = int(item.circle_center_x * width)
        cy = int(item.circle_center_y * height)
        radius_px = int(max(item.circle_radius * min(width, height), 28))

        crop_half_w = max(radius_px * 3, 180)
        crop_half_h = max(radius_px * 3, 180)
        left = max(0, cx - crop_half_w)
        top = max(0, cy - crop_half_h)
        right = min(width, cx + crop_half_w)
        bottom = min(height, cy + crop_half_h)

        crop = image.crop((left, top, right, bottom))
        draw = ImageDraw.Draw(crop)
        local_cx = cx - left
        local_cy = cy - top
        draw.ellipse(
            (
                local_cx - radius_px,
                local_cy - radius_px,
                local_cx + radius_px,
                local_cy + radius_px,
            ),
            outline=(255, 70, 70),
            width=6,
        )
        draw.ellipse((local_cx - 4, local_cy - 4, local_cx + 4, local_cy + 4), fill=(255, 70, 70))
        focused_path = output_dir / item.image_path.name.replace("sample_", "focus_")
        crop.save(focused_path, quality=94)
        _stamp_timestamp(focused_path, item.timestamp_sec)
        focused.append(
            FrameAsset(
                timestamp_sec=item.timestamp_sec,
                image_path=focused_path,
                circle_found=item.circle_found,
                circle_center_x=item.circle_center_x,
                circle_center_y=item.circle_center_y,
                circle_radius=item.circle_radius,
            )
        )

    return focused


def extract_frame_at_timestamp(
    video_url: str,
    timestamp_sec: float,
    output_path: Path,
    frame_width: int = 1280,
) -> Path:
    require_binary("ffmpeg")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-y",
        "-ss",
        str(timestamp_sec),
        "-i",
        video_url,
        "-frames:v",
        "1",
        "-vf",
        f"scale={frame_width}:-1",
        str(output_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    return output_path


def draw_player_circle(
    *,
    image_path: Path,
    output_path: Path,
    center_x: float,
    center_y: float,
    radius: float,
    label: str,
    note: str,
    found: bool,
) -> Path:
    image = Image.open(image_path).convert("RGB")
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()
    width, height = image.size

    cx = max(0, min(int(center_x * width), width - 1))
    cy = max(0, min(int(center_y * height), height - 1))
    px_radius = max(24, int(radius * min(width, height)))

    if found:
        draw.ellipse(
            (cx - px_radius, cy - px_radius, cx + px_radius, cy + px_radius),
            outline=(255, 80, 80),
            width=6,
        )
        draw.ellipse(
            (cx - 5, cy - 5, cx + 5, cy + 5),
            fill=(255, 80, 80),
        )

    banner = (16, 16, width - 16, 64)
    draw.rounded_rectangle(banner, radius=10, fill=(0, 0, 0))
    header = label if found else f"{label} - player not confidently found"
    draw.text((28, 26), header[:120], fill=(255, 255, 255), font=font)
    draw.text((28, 44), note[:150], fill=(210, 220, 230), font=font)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(output_path, quality=92)
    return output_path


def draw_bbox_overlay(
    *,
    image_path: Path,
    output_path: Path,
    bbox: dict,
    label: str,
) -> Path:
    image = Image.open(image_path).convert("RGB")
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()
    width, height = image.size

    x = float(bbox["x"]) * width
    y = float(bbox["y"]) * height
    w = float(bbox["width"]) * width
    h = float(bbox["height"]) * height
    left = max(0, int(x))
    top = max(0, int(y))
    right = min(width, int(x + w))
    bottom = min(height, int(y + h))

    draw.rectangle((left, top, right, bottom), outline=(255, 70, 70), width=6)
    label_text = label[:80]
    label_box = (left, max(0, top - 30), min(width, left + 360), top)
    draw.rectangle(label_box, fill=(0, 0, 0))
    draw.text((left + 8, max(0, top - 22)), label_text, fill=(255, 255, 255), font=font)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    image.save(output_path, quality=94)
    return output_path


def crop_reference_patch(
    *,
    image_path: Path,
    bbox: dict,
    output_path: Path,
    padding_ratio: float = 0.15,
) -> Path:
    image = Image.open(image_path).convert("RGB")
    width, height = image.size
    x = float(bbox["x"])
    y = float(bbox["y"])
    w = float(bbox["width"])
    h = float(bbox["height"])

    pad_x = w * padding_ratio
    pad_y = h * padding_ratio

    left = max(0, int((x - pad_x) * width))
    top = max(0, int((y - pad_y) * height))
    right = min(width, int((x + w + pad_x) * width))
    bottom = min(height, int((y + h + pad_y) * height))

    patch = image.crop((left, top, right, bottom))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    patch.save(output_path, quality=95)
    return output_path


def locate_patch_in_frame(
    *,
    frame_path: Path,
    patch_path: Path,
) -> dict:
    frame = cv2.imread(str(frame_path))
    patch = cv2.imread(str(patch_path))
    if frame is None or patch is None:
        raise RuntimeError("Unable to load images for player localization.")

    frame_h, frame_w = frame.shape[:2]
    patch_h, patch_w = patch.shape[:2]

    scales = [0.7, 0.85, 1.0, 1.15, 1.3]
    best = None

    for scale in scales:
        scaled_w = max(16, int(patch_w * scale))
        scaled_h = max(16, int(patch_h * scale))
        if scaled_w >= frame_w or scaled_h >= frame_h:
            continue

        scaled_patch = cv2.resize(patch, (scaled_w, scaled_h), interpolation=cv2.INTER_LINEAR)
        result = cv2.matchTemplate(frame, scaled_patch, cv2.TM_CCOEFF_NORMED)
        _, max_val, _, max_loc = cv2.minMaxLoc(result)

        candidate = {
            "score": float(max_val),
            "left": int(max_loc[0]),
            "top": int(max_loc[1]),
            "width": int(scaled_w),
            "height": int(scaled_h),
        }
        if best is None or candidate["score"] > best["score"]:
            best = candidate

    if best is None:
        return {
            "found": False,
            "confidence": "low",
            "center_x": 0.5,
            "center_y": 0.5,
            "radius": 0.08,
            "score": 0.0,
            "note": "No usable template match was found in this frame.",
        }

    center_x = (best["left"] + best["width"] / 2.0) / frame_w
    center_y = (best["top"] + best["height"] / 2.0) / frame_h
    radius = max(best["width"], best["height"]) / min(frame_w, frame_h) / 2.0
    score = best["score"]
    if score >= 0.78:
        confidence = "high"
    elif score >= 0.65:
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "found": score >= 0.58,
        "confidence": confidence,
        "center_x": round(float(center_x), 4),
        "center_y": round(float(center_y), 4),
        "radius": round(float(max(radius, 0.04)), 4),
        "score": round(float(score), 4),
        "note": f"Reference-based match score {score:.2f}.",
    }


def detect_red_circle(image_path: Path) -> dict:
    image = cv2.imread(str(image_path))
    if image is None:
        raise RuntimeError(f"Unable to read frame image: {image_path}")

    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    lower1 = np.array([0, 90, 80], dtype=np.uint8)
    upper1 = np.array([12, 255, 255], dtype=np.uint8)
    lower2 = np.array([165, 90, 80], dtype=np.uint8)
    upper2 = np.array([180, 255, 255], dtype=np.uint8)
    mask = cv2.inRange(hsv, lower1, upper1) | cv2.inRange(hsv, lower2, upper2)
    kernel = np.ones((3, 3), np.uint8)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel)
    mask = cv2.morphologyEx(mask, cv2.MORPH_DILATE, kernel)

    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return {"found": False}

    height, width = image.shape[:2]
    best = None
    for contour in contours:
        area = cv2.contourArea(contour)
        if area < 80:
            continue
        perimeter = cv2.arcLength(contour, True)
        if perimeter <= 0:
            continue
        circularity = 4 * math.pi * area / (perimeter * perimeter)
        (cx, cy), radius = cv2.minEnclosingCircle(contour)
        if radius < 10:
            continue
        score = circularity * min(area / 400.0, 3.0)
        candidate = {
            "score": score,
            "center_x": float(cx / width),
            "center_y": float(cy / height),
            "radius": float(radius / min(width, height)),
        }
        if best is None or candidate["score"] > best["score"]:
            best = candidate

    if best is None:
        return {"found": False}

    return {
        "found": True,
        "center_x": round(best["center_x"], 4),
        "center_y": round(best["center_y"], 4),
        "radius": round(max(best["radius"], 0.03), 4),
        "score": round(best["score"], 4),
    }


def _stamp_timestamp(image_path: Path, timestamp: float) -> None:
    image = Image.open(image_path).convert("RGB")
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()
    label = _format_time(timestamp)
    box = (12, 12, 90, 36)
    draw.rounded_rectangle(box, radius=8, fill=(0, 0, 0))
    draw.text((20, 20), label, fill=(255, 255, 255), font=font)
    image.save(image_path, quality=92)


def _format_time(seconds: float) -> str:
    total = int(seconds)
    minutes, secs = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:01d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"
