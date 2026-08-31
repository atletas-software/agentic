from agents.feedback.openai_service import coaching_wants_per_circle_feedback


def test_coaching_wants_per_circle_feedback_detects_admin_prompt():
    assert coaching_wants_per_circle_feedback(
        "Create Feedback Points every time Player is circled"
    )
    assert coaching_wants_per_circle_feedback("one note per red circle highlight")
    assert not coaching_wants_per_circle_feedback("Focus on pressing triggers")
    assert not coaching_wants_per_circle_feedback("")
