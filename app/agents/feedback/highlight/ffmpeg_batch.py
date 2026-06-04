"""Batched ffmpeg sampling — many JPEGs in one process.

The legacy pipeline in ``video_utils.extract_frame_at_timestamp`` does one
``ffmpeg`` invocation *per* timestamp. That is fine for a handful of frames,
but a 10-minute video at a 0.25s probe interval needs 2400 seeks. On an HTTPS /
CDN source that takes many minutes.

This module exposes two helpers that emit the timestamps we want via the
``fps`` video filter, so we get N JPEGs from one ``ffmpeg`` process:

- ``sample_range_uniform`` — uniformly spaced frames between t_lo and t_hi.
- ``sample_video_uniform`` — chunked uniform sampling across the full video.

Both return ``SampledFrame`` records with the **estimated** timestamp for each
JPEG (derived from the chunk start and the configured fps). That timestamp is
accurate to within ~1 / fps seconds, which is fine for both the YOLO probing
stage and the per-event extraction stage.

If a ``fps``-style call fails (some exotic CDN streams cannot be range-read
this way), the helpers fall back to per-timestamp seeks via the existing
``extract_frame_at_timestamp`` so the caller always gets *some* output.
"""

from __future__ import annotations

import logging
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

from agents.feedback.video_utils import (
    _ffmpeg_http_global_options,  # noqa: PLC2701 — internal but stable and shared
    _ffmpeg_jpeg_video_filter,  # noqa: PLC2701
    _jpeg_output_usable,  # noqa: PLC2701
    extract_frame_at_timestamp,
    require_binary,
)

LOG = logging.getLogger("highlight.ffmpeg_batch")


@dataclass
class SampledFrame:
    timestamp_sec: float
    image_path: Path


def _ffmpeg_extract_timeout_sec() -> Optional[float]:
    raw = (os.getenv("VIDEO_FFMPEG_EXTRACT_TIMEOUT_SEC") or "180").strip().lower()
    if raw in {"", "0", "none", "off"}:
        return None
    try:
        return float(raw)
    except ValueError:
        return 180.0


def _run(cmd: list[str], *, what: str, timeout: Optional[float]) -> subprocess.CompletedProcess:
    LOG.debug("$ %s", " ".join(cmd))
    try:
        proc = subprocess.run(cmd, check=False, capture_output=True, text=False, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"{what} timed out after {timeout}s.") from exc
    if proc.returncode != 0:
        tail = (proc.stderr or b"").decode("utf-8", errors="replace").strip()[-4000:]
        raise RuntimeError(f"{what} failed (exit {proc.returncode}). stderr tail: {tail or '(empty)'}")
    return proc


def _chunk_command(
    *,
    video_url: str,
    chunk_start: float,
    chunk_duration: float,
    fps: float,
    frame_width: int,
    output_pattern: str,
) -> list[str]:
    """Build an ffmpeg command that writes one JPEG every (1/fps) seconds inside a chunk."""
    http_opts = _ffmpeg_http_global_options(video_url)
    q = (os.getenv("VIDEO_FFMPEG_JPEG_Q") or "3").strip() or "3"
    # Reuse the scale + yuvj420p filter from video_utils, then chain the fps filter.
    base_vf = _ffmpeg_jpeg_video_filter(frame_width)
    vf = f"fps={fps:g},{base_vf}"
    return [
        "ffmpeg",
        "-y",
        *http_opts,
        "-ss",
        f"{chunk_start:.3f}",
        "-i",
        video_url,
        "-t",
        f"{chunk_duration:.3f}",
        "-vf",
        vf,
        "-q:v",
        q,
        "-strict",
        "-2",
        "-start_number",
        "0",
        output_pattern,
    ]


def _per_frame_fallback(
    *,
    video_url: str,
    timestamps: Sequence[float],
    output_dir: Path,
    prefix: str,
    frame_width: int,
) -> list[SampledFrame]:
    """Used when the batched fps approach failed for a chunk. Slower but safe."""
    out: list[SampledFrame] = []
    for idx, ts in enumerate(timestamps):
        safe = f"{ts:.3f}".replace(".", "_")
        path = output_dir / f"{prefix}_fb_{idx:05d}_{safe}.jpg"
        try:
            extract_frame_at_timestamp(video_url, ts, path, frame_width=frame_width)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("per-frame fallback failed at t=%.3fs: %s", ts, exc)
            continue
        if _jpeg_output_usable(path):
            out.append(SampledFrame(timestamp_sec=round(ts, 3), image_path=path))
    return out


def sample_range_uniform(
    *,
    video_url: str,
    t_lo: float,
    t_hi: float,
    output_dir: Path,
    prefix: str,
    fps: float,
    frame_width: int = 640,
) -> list[SampledFrame]:
    """Sample uniformly spaced JPEGs in [t_lo, t_hi] (inclusive of t_lo)."""
    require_binary("ffmpeg")
    if t_hi <= t_lo:
        return []
    fps = max(0.1, float(fps))
    duration = float(t_hi - t_lo)
    output_dir.mkdir(parents=True, exist_ok=True)

    pattern = str(output_dir / f"{prefix}_%05d.jpg")
    cmd = _chunk_command(
        video_url=video_url,
        chunk_start=float(t_lo),
        chunk_duration=duration,
        fps=fps,
        frame_width=frame_width,
        output_pattern=pattern,
    )

    try:
        _run(cmd, what=f"ffmpeg batch sample [{t_lo:.2f},{t_hi:.2f}]", timeout=_ffmpeg_extract_timeout_sec())
        outputs = sorted(output_dir.glob(f"{prefix}_*.jpg"))
        if not outputs:
            raise RuntimeError("ffmpeg produced no JPEGs")
        period = 1.0 / fps
        usable: list[SampledFrame] = []
        for idx, path in enumerate(outputs):
            if not _jpeg_output_usable(path):
                continue
            ts = round(float(t_lo) + idx * period, 3)
            usable.append(SampledFrame(timestamp_sec=ts, image_path=path))
        if usable:
            return usable
        raise RuntimeError("no usable JPEGs from batch")
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Batched ffmpeg failed (%s); falling back to per-frame seek.", exc)
        # Best-effort: clean partials before trying per-frame
        for path in output_dir.glob(f"{prefix}_*.jpg"):
            try:
                path.unlink()
            except OSError:
                pass
        n = max(1, int(math.floor(duration * fps)) + 1)
        timestamps = [round(float(t_lo) + i / fps, 3) for i in range(n)]
        return _per_frame_fallback(
            video_url=video_url,
            timestamps=timestamps,
            output_dir=output_dir,
            prefix=prefix,
            frame_width=frame_width,
        )


def sample_video_uniform(
    *,
    video_url: str,
    duration_sec: float,
    output_dir: Path,
    prefix: str,
    fps: float,
    frame_width: int = 640,
    chunk_sec: float = 60.0,
) -> list[SampledFrame]:
    """Sample uniformly across [0, duration_sec] in chunks of ``chunk_sec``."""
    require_binary("ffmpeg")
    output_dir.mkdir(parents=True, exist_ok=True)
    if duration_sec <= 0:
        return []
    fps = max(0.1, float(fps))
    chunk_sec = max(1.0, float(chunk_sec))

    out: list[SampledFrame] = []
    t = 0.0
    chunk_idx = 0
    while t < duration_sec - 1e-3:
        chunk_idx += 1
        chunk_end = min(duration_sec, t + chunk_sec)
        chunk_prefix = f"{prefix}_c{chunk_idx:04d}"
        chunk_frames = sample_range_uniform(
            video_url=video_url,
            t_lo=t,
            t_hi=chunk_end,
            output_dir=output_dir,
            prefix=chunk_prefix,
            fps=fps,
            frame_width=frame_width,
        )
        out.extend(chunk_frames)
        t = chunk_end
    return out


def iter_timestamps(start: float, end: float, fps: float) -> Iterable[float]:
    """Helper: yield the timestamps that ``sample_range_uniform(start, end, fps)`` would emit."""
    period = 1.0 / max(0.1, fps)
    n = max(1, int(math.floor((end - start) * fps)) + 1)
    for i in range(n):
        yield round(start + i * period, 3)
