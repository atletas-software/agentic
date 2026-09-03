"""Feedback agent registry / plug-in selection."""

from __future__ import annotations

import agents.registry as registry


def setup_function() -> None:
    # Allow tests to re-run registration after clearing.
    registry._REGISTRY.clear()
    registry._ENSURED = False


def test_default_agent_is_v1(monkeypatch) -> None:
    monkeypatch.delenv("FEEDBACK_AGENT_VERSION", raising=False)
    agent = registry.get_feedback_agent()
    assert agent.version == "v1"
    assert "v1" in registry.list_feedback_agents()


def test_env_selects_version(monkeypatch) -> None:
    monkeypatch.setenv("FEEDBACK_AGENT_VERSION", "v1")
    agent = registry.get_feedback_agent()
    assert agent.version == "v1"


def test_unknown_version_raises(monkeypatch) -> None:
    monkeypatch.setenv("FEEDBACK_AGENT_VERSION", "does-not-exist")
    try:
        registry.get_feedback_agent()
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "Unknown FEEDBACK_AGENT_VERSION" in str(exc)


def test_v2_is_registered_as_stub(monkeypatch) -> None:
    monkeypatch.setenv("FEEDBACK_AGENT_VERSION", "v2")
    agent = registry.get_feedback_agent()
    assert agent.version == "v2"
    try:
        agent.run_review("rid", {})
        assert False, "expected NotImplementedError"
    except NotImplementedError:
        pass
