from io import BytesIO

import pytest

from backendapi.services.gcs_video_storage import (
    GcsVideoStorageError,
    is_allowed_video,
    sanitize_filename,
    upload_feedback_video,
    video_extension,
)


def test_sanitize_filename_strips_paths_and_unsafe_chars():
    assert sanitize_filename("../../evil name.MP4") == "evil_name.MP4"
    assert sanitize_filename("") == "video.mp4"


def test_video_extension_from_name_and_content_type():
    assert video_extension("clip.MOV", "video/quicktime") == ".mov"
    assert video_extension("clip", "video/mp4") == ".mp4"
    assert video_extension("clip.bin", "text/plain") == ""


def test_is_allowed_video():
    assert is_allowed_video(filename="game.mp4", content_type="video/mp4")
    assert not is_allowed_video(filename="notes.pdf", content_type="application/pdf")


def test_upload_requires_bucket(monkeypatch):
    monkeypatch.delenv("GCS_FEEDBACK_VIDEO_BUCKET", raising=False)
    with pytest.raises(GcsVideoStorageError, match="GCS_FEEDBACK_VIDEO_BUCKET"):
        upload_feedback_video(BytesIO(b"abc"), filename="game.mp4", content_type="video/mp4", size_bytes=3)


def test_upload_rejects_unsupported_type(monkeypatch):
    monkeypatch.setenv("GCS_FEEDBACK_VIDEO_BUCKET", "test-bucket")
    with pytest.raises(GcsVideoStorageError, match="Unsupported video type"):
        upload_feedback_video(BytesIO(b"abc"), filename="notes.pdf", content_type="application/pdf", size_bytes=3)


def test_upload_rejects_oversize(monkeypatch):
    monkeypatch.setenv("GCS_FEEDBACK_VIDEO_BUCKET", "test-bucket")
    monkeypatch.setenv("GCS_FEEDBACK_VIDEO_MAX_BYTES", "10")
    with pytest.raises(GcsVideoStorageError, match="exceeds max size"):
        upload_feedback_video(BytesIO(b"abcdefghijk"), filename="game.mp4", content_type="video/mp4", size_bytes=11)
