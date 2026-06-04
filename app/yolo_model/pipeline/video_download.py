"""Download remote match videos to a local path for offline YOLO inference."""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

LOG = logging.getLogger("yolo.pipeline.video")


def _cache_root() -> Path:
    override = (os.getenv("POSE_PIPELINE_CACHE_DIR") or os.getenv("VIDEO_HIGHLIGHT_CACHE_DIR") or "").strip()
    if override:
        root = Path(override)
    else:
        root = Path(os.getenv("POSE_PIPELINE_WORKDIR", "/tmp/pose_pipeline")) / "video_cache"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _is_remote(url: str) -> bool:
    return urlparse(url).scheme in {"http", "https"}


def _local_path(url: str) -> Path | None:
    if url.startswith("file://"):
        return Path(url[7:])
    p = Path(url)
    if p.is_file():
        return p
    return None


def ensure_local_video(video_url: str, *, job_key: str) -> Path:
    """Return a local filesystem path for ``video_url`` (download if HTTP/S)."""
    url = (video_url or "").strip()
    if not url:
        raise ValueError("video_url is empty")

    local = _local_path(url)
    if local is not None:
        if not local.is_file():
            raise FileNotFoundError(f"Local video not found: {local}")
        return local.resolve()

    if not _is_remote(url):
        raise ValueError(f"Unsupported video URL scheme: {url}")

    dest = _cache_root() / f"{job_key}_{hashlib.sha1(url.encode()).hexdigest()[:12]}.mp4"
    if dest.is_file() and dest.stat().st_size > 0:
        LOG.info("Using cached video: %s", dest)
        return dest

    LOG.info("Downloading video to %s", dest)
    tmp = dest.with_suffix(".part")
    req = urllib.request.Request(
        url,
        headers={"User-Agent": (os.getenv("VIDEO_FFMPEG_USER_AGENT") or "Mozilla/5.0").strip()},
    )
    with urllib.request.urlopen(req, timeout=600) as resp, tmp.open("wb") as out:
        shutil.copyfileobj(resp, out, length=1024 * 1024)
    if tmp.stat().st_size == 0:
        tmp.unlink(missing_ok=True)
        raise RuntimeError("Downloaded video file is empty")
    tmp.replace(dest)
    return dest
