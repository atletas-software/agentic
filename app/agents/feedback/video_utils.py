from __future__ import annotations

import math
import os
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

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


def _ffprobe_timeout_sec() -> float | None:
    raw = (os.getenv("VIDEO_FFPROBE_TIMEOUT_SEC") or "90").strip().lower()
    if raw in {"", "0", "none", "off"}:
        return None
    return float(raw)


def _ffmpeg_extract_timeout_sec() -> float | None:
    raw = (os.getenv("VIDEO_FFMPEG_EXTRACT_TIMEOUT_SEC") or "180").strip().lower()
    if raw in {"", "0", "none", "off"}:
        return None
    return float(raw)


def _stderr_tail(proc: subprocess.CompletedProcess) -> str:
    raw = proc.stderr
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace").strip()[-6000:]
    return str(raw).strip()[-6000:]


def _ffmpeg_http_global_options(video_url: str) -> list[str]:
    """Reconnect / read timeouts help CloudFront and other HTTP(S) sources during seeks."""
    if not video_url.startswith(("http://", "https://")):
        return []
    rw_us = int((os.getenv("VIDEO_FFMPEG_RW_TIMEOUT_US") or "25000000").strip() or "25000000")
    opts = [
        "-reconnect",
        "1",
        "-reconnect_streamed",
        "1",
        "-reconnect_delay_max",
        "5",
        "-rw_timeout",
        str(rw_us),
    ]
    ua = (os.getenv("VIDEO_FFMPEG_USER_AGENT") or "").strip()
    if ua:
        opts.extend(["-user_agent", ua])
    return opts


def _jpeg_output_usable(path: Path) -> bool:
    """ffmpeg occasionally exits 0 without a real frame (e.g. seek past decodable content)."""
    try:
        if not path.is_file():
            return False
        if path.stat().st_size < 512:
            return False
        with path.open("rb") as fh:
            return fh.read(3) == b"\xff\xd8\xff"
    except OSError:
        return False


def _run_subprocess(
    cmd: list[str],
    *,
    timeout: float | None,
    text: bool,
    what: str,
) -> subprocess.CompletedProcess:
    try:
        proc = subprocess.run(cmd, check=False, capture_output=True, text=text, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        hint = (
            "If this URL is a CloudFront signed link, it may be locked to a specific source IP or may have expired. "
            "Try ffprobe from the same host, or increase VIDEO_FFPROBE_TIMEOUT_SEC / VIDEO_FFMPEG_EXTRACT_TIMEOUT_SEC."
        )
        raise RuntimeError(f"{what} timed out after {timeout}s. {hint}") from exc
    if proc.returncode != 0:
        tail = _stderr_tail(proc)
        raise RuntimeError(
            f"{what} failed (exit {proc.returncode}). ffmpeg/ffprobe stderr (tail): {tail or '(empty)'}"
        )
    return proc


def probe_duration(video_url: str) -> float:
    require_binary("ffprobe")
    prefix: list[str] = []
    if video_url.startswith(("http://", "https://")):
        rw_us = int((os.getenv("VIDEO_FFPROBE_RW_TIMEOUT_US") or "30000000").strip() or "30000000")
        prefix = ["-rw_timeout", str(rw_us)]
    cmd = [
        "ffprobe",
        *prefix,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        video_url,
    ]
    result = _run_subprocess(
        cmd,
        timeout=_ffprobe_timeout_sec(),
        text=True,
        what="ffprobe (read duration)",
    )
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
        extract_frame_at_timestamp(video_url, timestamp, image_path, frame_width=frame_width)
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


def _ffmpeg_jpeg_video_filter(frame_width: int) -> str:
    """
    Scale then convert to full-range 4:2:0 for MJPEG output.

    TV-range H.264 (yuv420p + limited) often fails the default MJPEG encoder with
    'Non full-range YUV is non-standard' / EINVAL (-22) on ffmpeg 6+.
    """
    return f"scale={frame_width}:-1:flags=bicubic,format=yuvj420p"


def _ffmpeg_frame_extract_cmd(
    video_url: str,
    timestamp_sec: float,
    output_path: Path,
    frame_width: int,
    *,
    seek_before_input: bool,
) -> list[str]:
    http_opts = _ffmpeg_http_global_options(video_url)
    q = (os.getenv("VIDEO_FFMPEG_JPEG_Q") or "3").strip() or "3"
    vf = _ffmpeg_jpeg_video_filter(frame_width)
    tail = [
        "-vf",
        vf,
        "-frames:v",
        "1",
        "-q:v",
        q,
        "-strict",
        "-2",
        str(output_path),
    ]
    if seek_before_input:
        return ["ffmpeg", "-y", *http_opts, "-ss", str(timestamp_sec), "-i", video_url, *tail]
    return ["ffmpeg", "-y", *http_opts, "-i", video_url, "-ss", str(timestamp_sec), *tail]


def extract_frame_at_timestamp(
    video_url: str,
    timestamp_sec: float,
    output_path: Path,
    frame_width: int = 1280,
) -> Path:
    """
    Extract one JPEG. Tries fast seek (-ss before -i), then accurate seek (-ss after -i) on failure —
    CloudFront / HTTP sources often need the slower path after mid-file seeks (ffmpeg exit 234, etc.).
    Pixel pipeline uses full-range YUV (yuvj420p) so MJPEG does not fail on TV-range H.264 (exit -22).
    """
    require_binary("ffmpeg")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    timeout = _ffmpeg_extract_timeout_sec()
    last_err: RuntimeError | None = None
    for seek_first in (True, False):
        cmd = _ffmpeg_frame_extract_cmd(
            video_url,
            timestamp_sec,
            output_path,
            frame_width,
            seek_before_input=seek_first,
        )
        mode = "fast_seek" if seek_first else "decode_seek"
        try:
            _run_subprocess(
                cmd,
                timeout=timeout,
                text=False,
                what=f"ffmpeg extract frame at {timestamp_sec}s ({mode})",
            )
            if _jpeg_output_usable(output_path):
                return output_path
            last_err = RuntimeError(
                f"ffmpeg exited 0 but JPEG output missing or unusable at {timestamp_sec}s ({mode}): {output_path}"
            )
        except RuntimeError as exc:
            last_err = exc
        if output_path.exists():
            try:
                output_path.unlink()
            except OSError:
                pass
    assert last_err is not None
    raise last_err


def probe_circle_timeline(
    video_url: str,
    duration_sec: float,
    output_dir: Path,
    *,
    interval_sec: float,
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
    player_first_name: Optional[str] = None,
    player_last_name: Optional[str] = None,
) -> list[tuple[float, bool]]:
    """Sample the video at fixed time steps; mark each as on/off via detect_highlight_overlay.

    Detects Veo-style red ring (with arrow notches) OR white pointer overlay; optional OCR
    cross-checks the player's first/last name to suppress false positives when available.
    """
    require_binary("ffmpeg")
    output_dir.mkdir(parents=True, exist_ok=True)
    if duration_sec <= 0:
        return []
    interval = max(0.15, float(interval_sec))
    max_probes = int(os.getenv("VIDEO_CIRCLE_MAX_PROBES", "2000"))
    est = int(math.ceil(duration_sec / interval)) + 1
    if est > max_probes:
        interval = duration_sec / max(max_probes - 1, 1)
    probe_estimate = int(math.ceil(duration_sec / interval)) + 1
    require_name_match = (os.getenv("VIDEO_HIGHLIGHT_REQUIRE_NAME_MATCH") or "false").strip().lower() in {"1", "true", "yes", "on"}
    out: list[tuple[float, bool]] = []
    t = 0.0
    idx = 0
    while t <= duration_sec + 1e-6:
        idx += 1
        if cancel_check is not None and (idx % 8 == 1 or idx <= 2) and cancel_check():
            raise RuntimeError("Review cancelled by user")
        ts = round(min(max(t, 0.0), max(0.0, duration_sec - 0.25)), 3)
        path = output_dir / f"probe_{idx:05d}.jpg"
        if on_progress and (idx == 1 or idx % 25 == 0 or idx >= probe_estimate):
            on_progress(
                {
                    "phase": "circle_timeline_probe",
                    "probe_current": idx,
                    "probe_estimate": probe_estimate,
                    "progress_detail": (
                        f"Scanning for highlight overlay: time sample {idx} / ~{probe_estimate} "
                        f"(this can take several minutes on long videos)"
                    ),
                }
            )
        # Near EOF, ffmpeg often exits 0 with no JPEG (or seek fails). Skip that sample
        # instead of failing the whole multi-minute probe pass.
        try:
            extract_frame_at_timestamp(video_url, ts, path, frame_width=640)
        except RuntimeError as exc:
            if path.exists():
                try:
                    path.unlink()
                except OSError:
                    pass
            # Treat as no-overlay sample so segments still build from successful probes.
            out.append((ts, False))
            t += interval
            if on_progress and (idx == 1 or idx % 50 == 0 or idx >= probe_estimate):
                on_progress(
                    {
                        "phase": "circle_timeline_probe",
                        "probe_current": idx,
                        "probe_estimate": probe_estimate,
                        "progress_detail": (
                            f"Skipped probe {idx} at {ts}s (frame extract failed): {str(exc)[:120]}"
                        ),
                    }
                )
            continue
        if not _jpeg_output_usable(path):
            out.append((ts, False))
            t += interval
            continue
        overlay = detect_highlight_overlay(
            path,
            player_first_name=player_first_name,
            player_last_name=player_last_name,
        )
        is_on = bool(overlay.get("found"))
        if is_on and require_name_match and (player_first_name or player_last_name):
            is_on = bool(overlay.get("name_confirmed"))
        out.append((ts, is_on))
        t += interval
    if on_progress:
        on_progress(
            {
                "phase": "circle_timeline_probe_done",
                "probe_current": idx,
                "probe_estimate": probe_estimate,
                "progress_detail": f"Finished timeline probe ({idx} samples).",
            }
        )
    return out


def circle_visibility_segments_from_probes(
    probes: list[tuple[float, bool]],
) -> list[tuple[float, float, float, int]]:
    """
    Contiguous True runs → (t_on, t_off, anchor_mid, probe_count).
    t_on / t_off use probe timestamps (approximate visibility span).
    """
    if not probes:
        return []
    segments: list[tuple[float, float, float, int]] = []
    i = 0
    while i < len(probes):
        if not probes[i][1]:
            i += 1
            continue
        start = i
        while i < len(probes) and probes[i][1]:
            i += 1
        run = probes[start:i]
        t_on = run[0][0]
        t_off = run[-1][0]
        anchor = round((t_on + t_off) / 2.0, 2)
        segments.append((t_on, t_off, anchor, len(run)))
    return segments


def extract_uniform_frames_in_range(
    video_url: str,
    t_lo: float,
    t_hi: float,
    output_dir: Path,
    file_prefix: str,
    *,
    max_frames: int = 14,
    frame_width: int = 720,
    video_duration_sec: float | None = None,
) -> list[FrameAsset]:
    """Uniformly sample stills in [t_lo, t_hi] inclusive (for segment analysis)."""
    require_binary("ffmpeg")
    output_dir.mkdir(parents=True, exist_ok=True)
    t_lo = float(max(0.0, t_lo))
    t_hi = float(max(t_lo + 0.05, t_hi))
    t_cap = float("inf")
    if video_duration_sec is not None and video_duration_sec > 0.12:
        t_cap = max(0.0, float(video_duration_sec) - 0.08)
    t_hi = min(t_hi, t_cap)
    t_lo = min(t_lo, t_hi)
    if max_frames <= 1:
        times = [(t_lo + t_hi) / 2.0]
    else:
        times = [t_lo + (t_hi - t_lo) * (k / (max_frames - 1)) for k in range(max_frames)]
    assets: list[FrameAsset] = []
    for j, raw_t in enumerate(times):
        ts = round(min(max(raw_t, 0.0), t_hi, t_cap), 3)
        safe = f"{ts:.2f}".replace(".", "_")
        p = output_dir / f"{file_prefix}_{j:02d}_{safe}.jpg"
        extract_frame_at_timestamp(video_url, ts, p, frame_width=frame_width)
        if not _jpeg_output_usable(p):
            raise RuntimeError(f"Frame extract produced no usable JPEG at t={ts}s path={p}")
        _stamp_timestamp(p, ts)
        if not _jpeg_output_usable(p):
            raise RuntimeError(f"Timestamp stamp produced no usable JPEG at t={ts}s path={p}")
        overlay = detect_highlight_overlay(p)
        assets.append(
            FrameAsset(
                timestamp_sec=ts,
                image_path=p,
                circle_found=bool(overlay.get("found")),
                circle_center_x=float(overlay.get("center_x", 0.0)),
                circle_center_y=float(overlay.get("center_y", 0.0)),
                circle_radius=float(overlay.get("radius", 0.0)),
            )
        )
    return assets


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


def _detect_red_ring_image(image: "np.ndarray") -> dict:
    """Robust to ring overlays with arrow notches (Hough on closed red mask + contour fallback)."""
    if image is None:
        return {"found": False}
    height, width = image.shape[:2]
    min_dim = max(min(height, width), 1)
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    lower1 = np.array([0, 80, 70], dtype=np.uint8)
    upper1 = np.array([12, 255, 255], dtype=np.uint8)
    lower2 = np.array([165, 80, 70], dtype=np.uint8)
    upper2 = np.array([180, 255, 255], dtype=np.uint8)
    mask = cv2.inRange(hsv, lower1, upper1) | cv2.inRange(hsv, lower2, upper2)
    if cv2.countNonZero(mask) < 60:
        return {"found": False}
    k_close = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (9, 9))
    closed = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, k_close)
    blurred = cv2.GaussianBlur(closed, (7, 7), 0)
    circles = cv2.HoughCircles(
        blurred,
        cv2.HOUGH_GRADIENT,
        dp=1.2,
        minDist=int(max(min_dim * 0.10, 20)),
        param1=80,
        param2=18,
        minRadius=int(max(min_dim * 0.025, 8)),
        maxRadius=int(max(min_dim * 0.18, 20)),
    )
    if circles is not None and len(circles) > 0:
        cx, cy, r = circles[0][0]
        return {
            "found": True,
            "method": "hough_red_ring",
            "center_x": round(float(cx) / width, 4),
            "center_y": round(float(cy) / height, 4),
            "radius": round(float(r) / min_dim, 4),
        }
    contours, _ = cv2.findContours(closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    best: dict[str, Any] | None = None
    for contour in contours:
        area = cv2.contourArea(contour)
        if area < 80:
            continue
        perimeter = cv2.arcLength(contour, True)
        if perimeter <= 0:
            continue
        circularity = 4 * math.pi * area / (perimeter * perimeter)
        (cx, cy), radius = cv2.minEnclosingCircle(contour)
        if radius < 8:
            continue
        if circularity < 0.30:
            continue
        score = circularity * min(area / 400.0, 3.0)
        candidate = {
            "score": score,
            "center_x": float(cx) / width,
            "center_y": float(cy) / height,
            "radius": float(radius) / min_dim,
        }
        if best is None or candidate["score"] > best["score"]:
            best = candidate
    if best is None:
        return {"found": False}
    return {
        "found": True,
        "method": "contour_red",
        "center_x": round(best["center_x"], 4),
        "center_y": round(best["center_y"], 4),
        "radius": round(max(best["radius"], 0.03), 4),
        "score": round(best["score"], 4),
    }


def _detect_white_pointer_image(image: "np.ndarray") -> dict:
    """Tall, narrow bright/white near-vertical stroke (Veo / athlete-focus pointer)."""
    if image is None:
        return {"found": False}
    height, width = image.shape[:2]
    min_dim = max(min(height, width), 1)
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    mask = cv2.inRange(
        hsv,
        np.array([0, 0, 215], dtype=np.uint8),
        np.array([180, 45, 255], dtype=np.uint8),
    )
    if cv2.countNonZero(mask) < 80:
        return {"found": False}
    k_vert = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 21))
    mask_v = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, k_vert)
    contours, _ = cv2.findContours(mask_v, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    best: dict[str, Any] | None = None
    for c in contours:
        x, y, cw, ch = cv2.boundingRect(c)
        if ch < height * 0.10:
            continue
        if cw > width * 0.04:
            continue
        aspect = ch / max(cw, 1)
        if aspect < 6:
            continue
        roi = mask[y : y + ch, x : x + cw]
        density = cv2.countNonZero(roi) / max(cw * ch, 1)
        if density < 0.45:
            continue
        score = float(density) * float(aspect)
        cand = {
            "score": score,
            "center_x": (x + cw / 2.0) / width,
            "center_y": (y + ch / 2.0) / height,
            "radius": max(ch, cw) / min_dim / 2.0,
            "bbox": (int(x), int(y), int(cw), int(ch)),
        }
        if best is None or cand["score"] > best["score"]:
            best = cand
    if best is None:
        return {"found": False}
    return {
        "found": True,
        "method": "white_pointer",
        "center_x": round(best["center_x"], 4),
        "center_y": round(best["center_y"], 4),
        "radius": round(max(best["radius"], 0.03), 4),
        "score": round(best["score"], 4),
        "bbox": best["bbox"],
    }


_TESS_AVAILABLE_CACHE: Optional[bool] = None


def _tesseract_available() -> bool:
    """Cached check: pytesseract import + system 'tesseract' binary on PATH (env opt-out honored)."""
    global _TESS_AVAILABLE_CACHE
    if _TESS_AVAILABLE_CACHE is not None:
        return _TESS_AVAILABLE_CACHE
    mode = (os.getenv("VIDEO_HIGHLIGHT_DETECTOR_OCR") or "auto").strip().lower()
    if mode in {"off", "false", "0", "no"}:
        _TESS_AVAILABLE_CACHE = False
        return False
    try:
        import pytesseract  # noqa: F401
    except ImportError:
        _TESS_AVAILABLE_CACHE = False
        return False
    if shutil.which("tesseract") is None:
        _TESS_AVAILABLE_CACHE = False
        return False
    _TESS_AVAILABLE_CACHE = True
    return True


def _ocr_text_in_roi(image: "np.ndarray", bbox: tuple[int, int, int, int]) -> str:
    if not _tesseract_available():
        return ""
    try:
        import pytesseract
    except ImportError:
        return ""
    height, width = image.shape[:2]
    x, y, cw, ch = bbox
    pad_x = max(int(width * 0.10), 80)
    pad_y = max(int(ch * 0.20), 20)
    x0 = max(0, x - pad_x)
    y0 = max(0, y - pad_y)
    x1 = min(width, x + cw + pad_x)
    y1 = min(height, y + ch + pad_y)
    roi = image[y0:y1, x0:x1]
    if roi.size == 0:
        return ""
    try:
        gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
        _, th = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY)
        text = pytesseract.image_to_string(th, config="--psm 6")
    except Exception:  # noqa: BLE001
        return ""
    return (text or "").strip()


def _bbox_from_circle(result: dict, image: "np.ndarray") -> tuple[int, int, int, int]:
    height, width = image.shape[:2]
    min_dim = max(min(height, width), 1)
    cx = float(result.get("center_x") or 0.5)
    cy = float(result.get("center_y") or 0.5)
    rr = float(result.get("radius") or 0.05)
    px = int(cx * width)
    py = int(cy * height)
    pr = max(int(rr * min_dim), 12)
    return (max(0, px - pr), max(0, py - pr), 2 * pr, 2 * pr)


def detect_highlight_overlay(
    image_path: Path,
    *,
    player_first_name: Optional[str] = None,
    player_last_name: Optional[str] = None,
) -> dict:
    """Combined detector for Veo-style red ring (with notches) + white pointer overlay.

    Returns {"found": bool, "method": str, "center_x", "center_y", "radius", optional "ocr_text",
    "name_confirmed"}. OR'd across enabled detectors; OCR is best-effort and only adds confirmation.
    """
    image = cv2.imread(str(image_path))
    if image is None:
        return {"found": False, "method": "none"}
    enable_red = (os.getenv("VIDEO_HIGHLIGHT_RED_RING") or "true").strip().lower() not in {"0", "false", "off", "no"}
    enable_white = (os.getenv("VIDEO_HIGHLIGHT_WHITE_POINTER") or "true").strip().lower() not in {"0", "false", "off", "no"}

    result: dict[str, Any] = {"found": False, "method": "none"}
    if enable_red:
        r = _detect_red_ring_image(image)
        if r.get("found"):
            result = {**r}
    if not result.get("found") and enable_white:
        p = _detect_white_pointer_image(image)
        if p.get("found"):
            result = {**p}

    if result.get("found") and (player_first_name or player_last_name):
        bbox = result.get("bbox") or _bbox_from_circle(result, image)
        text = _ocr_text_in_roi(image, bbox)
        if text:
            result["ocr_text"] = text[:120]
            tl = text.lower()
            for name in (player_first_name, player_last_name):
                clean = (name or "").strip().lower()
                if clean and len(clean) >= 3 and clean in tl:
                    result["name_confirmed"] = name
                    break
    return result


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
