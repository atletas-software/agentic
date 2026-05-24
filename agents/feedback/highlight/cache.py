"""Local video cache for HTTPS sources.

The probe stage extracts hundreds of frames; doing per-seek HTTP roundtrips
against a CDN is both slow (tens of seconds per seek on bad networks) and
unreliable (CloudFront / signed URLs). Downloading the video once and reading
from a local file fixes both.

Caching is purely opportunistic:

- If the source URL is local (``file://`` or a real path), we return it as-is.
- If caching is disabled via ``VIDEO_HIGHLIGHT_VIDEO_CACHE=off``, we return the
  original URL and let ffmpeg stream as it does today.
- Otherwise we attempt to download; if the download fails for any reason, we
  fall back to the original URL.

The cache directory is bounded by ``VIDEO_HIGHLIGHT_CACHE_MAX_GB`` (default 10).
When over budget, the least-recently-used files are pruned.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from agents.feedback.storage import DATA_DIR

LOG = logging.getLogger("highlight.cache")


@dataclass
class CachedVideo:
    """A local-file handle for ffmpeg/ffprobe to use.

    ``ffmpeg_input`` is what callers should pass to ffmpeg. If caching failed
    or was disabled this is just the original URL; otherwise it's the absolute
    local path.
    """

    ffmpeg_input: str
    cached: bool
    cache_path: Optional[Path] = None
    bytes_downloaded: int = 0


def _cache_root() -> Path:
    override = (os.getenv("VIDEO_HIGHLIGHT_CACHE_DIR") or "").strip()
    root = Path(override) if override else DATA_DIR / "highlight_cache"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _enabled() -> bool:
    raw = (os.getenv("VIDEO_HIGHLIGHT_VIDEO_CACHE") or "on").strip().lower()
    return raw not in {"0", "off", "false", "no", "disable", "disabled"}


def _max_bytes() -> int:
    gb = float((os.getenv("VIDEO_HIGHLIGHT_CACHE_MAX_GB") or "10").strip() or "10")
    return int(max(1, gb) * 1024 * 1024 * 1024)


def _hash_url(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def _is_remote(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"}


def _normalize_local(url: str) -> str:
    if url.startswith("file://"):
        return urlparse(url).path
    return url


def _prune_lru(root: Path, max_bytes: int) -> None:
    files = [p for p in root.iterdir() if p.is_file()]
    total = sum(p.stat().st_size for p in files)
    if total <= max_bytes:
        return
    files.sort(key=lambda p: p.stat().st_atime)
    for p in files:
        if total <= max_bytes:
            break
        try:
            size = p.stat().st_size
            p.unlink()
            total -= size
            LOG.info("Pruned %s (%d bytes) from highlight cache", p.name, size)
        except OSError as exc:
            LOG.warning("Could not prune %s: %s", p, exc)


def _download(url: str, dest: Path, *, timeout: float = 600.0) -> int:
    tmp = dest.with_suffix(dest.suffix + ".part")
    user_agent = (os.getenv("VIDEO_FFMPEG_USER_AGENT") or "Mozilla/5.0 (compatible; FeedbackAgent/1.0)").strip()
    req = urllib.request.Request(url, headers={"User-Agent": user_agent or "FeedbackAgent/1.0"})
    started = time.time()
    bytes_written = 0
    with urllib.request.urlopen(req, timeout=timeout) as resp, tmp.open("wb") as fh:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            fh.write(chunk)
            bytes_written += len(chunk)
            if time.time() - started > timeout:
                raise TimeoutError(f"Download exceeded {timeout}s ({bytes_written} bytes)")
    tmp.replace(dest)
    return bytes_written


def get_local_video(url: str) -> CachedVideo:
    """Return a local-file handle for the given URL, caching if remote and enabled."""
    if not url:
        return CachedVideo(ffmpeg_input=url, cached=False)

    if not _is_remote(url):
        return CachedVideo(ffmpeg_input=_normalize_local(url), cached=False)

    if not _enabled():
        LOG.debug("Highlight video cache disabled; streaming %s directly", url)
        return CachedVideo(ffmpeg_input=url, cached=False)

    root = _cache_root()
    digest = _hash_url(url)
    suffix = Path(urlparse(url).path).suffix or ".mp4"
    dest = root / f"{digest}{suffix}"

    if dest.is_file() and dest.stat().st_size > 0:
        os.utime(dest, None)
        return CachedVideo(ffmpeg_input=str(dest), cached=True, cache_path=dest, bytes_downloaded=0)

    try:
        LOG.info("Caching remote video locally: %s -> %s", url, dest)
        bytes_written = _download(url, dest)
        _prune_lru(root, _max_bytes())
        return CachedVideo(ffmpeg_input=str(dest), cached=True, cache_path=dest, bytes_downloaded=bytes_written)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Local video cache failed for %s: %s — falling back to streaming.", url, exc)
        return CachedVideo(ffmpeg_input=url, cached=False)


def clear_cache() -> None:
    root = _cache_root()
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
