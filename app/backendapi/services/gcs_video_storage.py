"""Upload Agent Lab match videos to GCS and return an HTTPS URL ffmpeg can fetch."""

from __future__ import annotations

import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import quote

from backendapi.core.logger import error as log_error
from backendapi.core.logger import info as log_info

ALLOWED_VIDEO_EXTENSIONS = frozenset({".mp4", ".mov", ".m4v", ".webm", ".mkv", ".mpeg", ".mpg", ".avi"})
ALLOWED_VIDEO_CONTENT_TYPES = frozenset(
    {
        "video/mp4",
        "video/quicktime",
        "video/webm",
        "video/x-matroska",
        "video/mpeg",
        "video/x-msvideo",
        "application/octet-stream",
    }
)

_STORAGE_CLIENT = None


class GcsVideoStorageError(RuntimeError):
    """Raised when GCS is not configured or an upload fails."""


def gcs_feedback_video_bucket() -> str:
    return (os.getenv("GCS_FEEDBACK_VIDEO_BUCKET") or "").strip()


def gcs_feedback_video_prefix() -> str:
    raw = (os.getenv("GCS_FEEDBACK_VIDEO_PREFIX") or "feedback-videos").strip().strip("/")
    return raw or "feedback-videos"


def gcs_feedback_video_max_bytes() -> int:
    raw = (os.getenv("GCS_FEEDBACK_VIDEO_MAX_BYTES") or str(500 * 1024 * 1024)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise GcsVideoStorageError("GCS_FEEDBACK_VIDEO_MAX_BYTES must be an integer.") from exc
    return max(1, value)


def gcs_signed_url_ttl_seconds() -> int:
    raw = (os.getenv("GCS_FEEDBACK_VIDEO_SIGNED_URL_TTL_SECONDS") or str(7 * 24 * 3600)).strip()
    try:
        value = int(raw)
    except ValueError:
        value = 7 * 24 * 3600
    return max(300, value)


def sanitize_filename(name: str | None) -> str:
    base = Path(name or "video.mp4").name
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._")
    return (cleaned or "video.mp4")[:180]


def sanitize_player_slug(name: str | None, player_key: str | None = None) -> str:
    """GCS-safe player folder/file label, e.g. Danny_Papez-22292."""
    raw_name = re.sub(r"\s+", " ", (name or "").strip())
    name_part = re.sub(r"[^A-Za-z0-9._-]+", "_", raw_name).strip("._")[:80]
    key_part = re.sub(r"[^A-Za-z0-9._-]+", "_", (player_key or "").strip()).strip("._")[:40]
    if name_part and key_part:
        return f"{name_part}-{key_part}"
    return name_part or key_part or "unknown-player"


def video_extension(filename: str, content_type: str | None = None) -> str:
    ext = Path(sanitize_filename(filename)).suffix.lower()
    if ext in ALLOWED_VIDEO_EXTENSIONS:
        return ext
    ctype = (content_type or "").split(";", 1)[0].strip().lower()
    mapping = {
        "video/mp4": ".mp4",
        "video/quicktime": ".mov",
        "video/webm": ".webm",
        "video/x-matroska": ".mkv",
        "video/mpeg": ".mpeg",
        "video/x-msvideo": ".avi",
    }
    return mapping.get(ctype, "")


def is_allowed_video(*, filename: str, content_type: str | None) -> bool:
    if video_extension(filename, content_type):
        return True
    ctype = (content_type or "").split(";", 1)[0].strip().lower()
    return ctype in ALLOWED_VIDEO_CONTENT_TYPES and ctype != "application/octet-stream"


def build_object_name(
    filename: str,
    content_type: str | None = None,
    *,
    player_name: str | None = None,
    player_key: str | None = None,
) -> str:
    """Player-first layout for easy GCS filtering: feedback-videos/{Player_Name-id}/YYYY/MM/..."""
    safe = sanitize_filename(filename)
    ext = video_extension(filename, content_type) or Path(safe).suffix.lower() or ".mp4"
    stem = Path(safe).stem or "video"
    player_slug = sanitize_player_slug(player_name, player_key)
    now = datetime.now(UTC)
    unique = uuid.uuid4().hex[:8]
    return (
        f"{gcs_feedback_video_prefix()}/{player_slug}/{now:%Y/%m}/"
        f"{player_slug}-{stem}-{unique}{ext}"
    )


def is_managed_gcs_video_url(url: str) -> bool:
    """True when URL already points at our feedback-videos bucket (public or signed)."""
    bucket = gcs_feedback_video_bucket()
    if not bucket or not (url or "").strip():
        return False
    u = url.strip().lower()
    b = bucket.lower()
    return (
        f"storage.googleapis.com/{b}/" in u
        or f"storage.cloud.google.com/{b}/" in u
        or f"/{b}/o/" in u  # JSON API / signed forms
        or u.startswith(f"gs://{b}/")
    )


def _prepare_adc() -> None:
    """Use VM/workload ADC. Sheets JSON in GOOGLE_APPLICATION_CREDENTIALS is not for Storage."""
    cred_path = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if cred_path:
        log_info(
            "gcs_video_unset_app_credentials",
            path_preview=cred_path[:120],
            hint="GCS uploads use VM ADC; Sheets use DESTINATION_GOOGLE_CREDENTIALS_FILE.",
        )
    os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)


def _storage_client():
    global _STORAGE_CLIENT
    if _STORAGE_CLIENT is not None:
        return _STORAGE_CLIENT
    try:
        from google.cloud import storage
    except ImportError as exc:
        raise GcsVideoStorageError(
            "google-cloud-storage is not installed. Add it to requirements and rebuild the API image."
        ) from exc
    _prepare_adc()
    project = (os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or "").strip() or None
    _STORAGE_CLIENT = storage.Client(project=project)
    return _STORAGE_CLIENT


def _public_https_url(bucket: str, object_name: str) -> str:
    return f"https://storage.googleapis.com/{quote(bucket, safe='')}/{quote(object_name, safe='/')}"


def _signed_get_url(blob: Any, ttl_seconds: int) -> str:
    expiration = timedelta(seconds=ttl_seconds)
    try:
        return blob.generate_signed_url(version="v4", expiration=expiration, method="GET")
    except Exception as direct_err:  # noqa: BLE001
        log_info("gcs_video_direct_sign_failed", error=str(direct_err)[:300])

    from google.auth.transport import requests as google_auth_requests

    credentials = blob.client._credentials
    request = google_auth_requests.Request()
    credentials.refresh(request)
    sa_email = getattr(credentials, "service_account_email", None) or ""
    if not sa_email:
        raise GcsVideoStorageError(
            "Could not sign a GCS download URL. Grant the VM service account "
            "roles/iam.serviceAccountTokenCreator (signBlob) or set GCS_FEEDBACK_VIDEO_PUBLIC=true "
            "on a bucket that allows public object reads."
        )
    return blob.generate_signed_url(
        version="v4",
        expiration=expiration,
        method="GET",
        service_account_email=sa_email,
        access_token=credentials.token,
    )


def upload_feedback_video(
    fileobj: BinaryIO,
    *,
    filename: str,
    content_type: str | None,
    size_bytes: int | None = None,
    player_name: str | None = None,
    player_key: str | None = None,
) -> dict[str, Any]:
    bucket_name = gcs_feedback_video_bucket()
    if not bucket_name:
        raise GcsVideoStorageError(
            "GCS_FEEDBACK_VIDEO_BUCKET is not set. Create a bucket and set it in app/backendapi/.env."
        )
    if not is_allowed_video(filename=filename, content_type=content_type):
        raise GcsVideoStorageError(
            "Unsupported video type. Use mp4, mov, m4v, webm, mkv, mpeg, or avi."
        )
    max_bytes = gcs_feedback_video_max_bytes()
    if size_bytes is not None and size_bytes > max_bytes:
        raise GcsVideoStorageError(f"Video exceeds max size of {max_bytes} bytes.")

    object_name = build_object_name(
        filename,
        content_type,
        player_name=player_name,
        player_key=player_key,
    )
    ctype = (content_type or "").split(";", 1)[0].strip() or "video/mp4"
    make_public = (os.getenv("GCS_FEEDBACK_VIDEO_PUBLIC") or "").strip().lower() in {"1", "true", "yes"}
    player_slug = sanitize_player_slug(player_name, player_key)

    try:
        client = _storage_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.cache_control = "private, max-age=0"
        meta = {
            "player_name": (player_name or "").strip()[:180],
            "player_key": (player_key or "").strip()[:80],
            "player_slug": player_slug,
            "original_filename": sanitize_filename(filename),
        }
        blob.metadata = {k: v for k, v in meta.items() if v}
        blob.upload_from_file(fileobj, content_type=ctype, rewind=True)
        if make_public:
            try:
                blob.make_public()
            except Exception as pub_err:  # noqa: BLE001
                log_info("gcs_video_make_public_skipped", error=str(pub_err)[:300])
            video_url = _public_https_url(bucket_name, object_name)
        else:
            video_url = _signed_get_url(blob, gcs_signed_url_ttl_seconds())
    except GcsVideoStorageError:
        raise
    except Exception as exc:  # noqa: BLE001
        log_error("gcs_video_upload_failed", error=str(exc), bucket=bucket_name)
        raise GcsVideoStorageError(f"Failed to upload video to GCS: {exc}") from exc

    log_info(
        "gcs_video_uploaded",
        bucket=bucket_name,
        object_name=object_name,
        size_bytes=size_bytes,
        public=make_public,
        player_slug=player_slug,
    )
    return {
        "video_url": video_url,
        "gs_uri": f"gs://{bucket_name}/{object_name}",
        "bucket": bucket_name,
        "object_name": object_name,
        "original_filename": sanitize_filename(filename),
        "player_name": (player_name or "").strip(),
        "player_key": (player_key or "").strip(),
        "content_type": ctype,
        "size_bytes": size_bytes,
    }


def ingest_remote_video_to_gcs(
    remote_url: str,
    *,
    player_name: str | None = None,
    player_key: str | None = None,
    filename_hint: str | None = None,
) -> dict[str, Any]:
    """
    Download a remote video URL and store it under the selected player's GCS folder.
    Skips download when the URL already points at our feedback-videos bucket.
    """
    url = (remote_url or "").strip()
    if not url:
        raise GcsVideoStorageError("video_url is empty.")
    if is_managed_gcs_video_url(url):
        return {
            "video_url": url,
            "ingested": False,
            "skipped_reason": "already_managed_gcs_url",
            "player_name": (player_name or "").strip(),
            "player_key": (player_key or "").strip(),
        }

    import tempfile
    from urllib.parse import unquote, urlparse

    import httpx

    max_bytes = gcs_feedback_video_max_bytes()
    parsed = urlparse(url)
    path_name = Path(unquote(parsed.path or "")).name or "remote-video.mp4"
    filename = sanitize_filename(filename_hint or path_name)
    if not video_extension(filename, None):
        filename = f"{Path(filename).stem or 'remote-video'}.mp4"

    timeout = httpx.Timeout(30.0, read=600.0, write=60.0, connect=30.0)
    tmp_path: str | None = None
    written = 0
    content_type = "video/mp4"
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=timeout) as resp:
            if resp.status_code >= 400:
                raise GcsVideoStorageError(
                    f"Could not download video URL (HTTP {resp.status_code}). "
                    "Signed/CDN links often expire or are IP-locked — upload the file instead."
                )
            ctype = (resp.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
            if ctype.startswith("video/") or ctype == "application/octet-stream":
                content_type = ctype or content_type
            cl = resp.headers.get("content-length")
            if cl and cl.isdigit() and int(cl) > max_bytes:
                raise GcsVideoStorageError(f"Remote video exceeds max size of {max_bytes} bytes.")

            suffix = video_extension(filename, content_type) or ".mp4"
            with tempfile.NamedTemporaryFile(prefix="ingest-video-", suffix=suffix, delete=False) as tmp:
                tmp_path = tmp.name
                for chunk in resp.iter_bytes(1024 * 1024):
                    if not chunk:
                        continue
                    written += len(chunk)
                    if written > max_bytes:
                        raise GcsVideoStorageError(f"Remote video exceeds max size of {max_bytes} bytes.")
                    tmp.write(chunk)
        if written <= 0:
            raise GcsVideoStorageError("Remote video URL returned an empty body.")
        assert tmp_path is not None
        with open(tmp_path, "rb") as fh:
            result = upload_feedback_video(
                fh,
                filename=filename,
                content_type=content_type,
                size_bytes=written,
                player_name=player_name,
                player_key=player_key,
            )
        result["ingested"] = True
        result["source_url"] = url[:500]
        return result
    except GcsVideoStorageError:
        raise
    except httpx.HTTPError as exc:
        raise GcsVideoStorageError(
            f"Failed to download video URL: {exc}. "
            "If this is a CloudFront/signed link, upload the file in Agent Lab instead."
        ) from exc
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

