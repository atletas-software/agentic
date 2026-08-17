from backendapi.services.feedback_public_url import (
    feedback_public_base_url,
    feedback_public_review_url,
    feedback_public_watch_url,
)


def test_prefers_https_frontend_origin(monkeypatch):
    monkeypatch.setenv(
        "FRONTEND_BASE_URL",
        "http://localhost:3000,http://34.181.228.190:3000,https://connect.athlete-focus.com",
    )
    monkeypatch.setenv("FEEDBACK_PUBLIC_BASE_URL", "http://34.181.228.190:8000")
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://feedback.athlete-focus.com")
    assert feedback_public_base_url() == "https://connect.athlete-focus.com"
    assert feedback_public_review_url("abc") == "https://connect.athlete-focus.com/review/abc"
    assert feedback_public_watch_url("abc") == "https://connect.athlete-focus.com/jobs/abc"


def test_uses_public_cors_origin(monkeypatch):
    monkeypatch.setenv(
        "FRONTEND_BASE_URL",
        "http://localhost:3000,http://34.181.228.190:3000",
    )
    monkeypatch.delenv("FEEDBACK_PUBLIC_BASE_URL", raising=False)
    monkeypatch.delenv("PUBLIC_BASE_URL", raising=False)
    assert feedback_public_base_url() == "http://34.181.228.190:3000"


def test_falls_back_to_api_origin(monkeypatch):
    monkeypatch.delenv("FRONTEND_BASE_URL", raising=False)
    monkeypatch.setenv("FEEDBACK_PUBLIC_BASE_URL", "http://34.181.228.190:8000")
    assert feedback_public_base_url() == "http://34.181.228.190:8000"
