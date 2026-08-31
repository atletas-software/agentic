from backendapi.services.feedback_review_embed import feedback_review_document_chunks


def test_feedback_chunks_include_video_url_and_marker_metadata():
    review = {
        "video_url": "https://storage.example.com/video.mp4",
        "overall_assessment": {"strengths": ["Good press"], "improvements": [], "next_focus": []},
        "coach_narrative": "Keep working on triggers.",
        "markers": [
            {
                "timestamp_sec": 12.5,
                "category": "defending",
                "coaching_note": "Step when the pass is played.",
            }
        ],
    }
    parts = feedback_review_document_chunks(
        review,
        "rev-1",
        video_url="https://storage.example.com/video.mp4",
        agent_job_id=42,
        player_key="22292",
    )
    assert len(parts) == 3
    overall_text, overall_ref, overall_meta = parts[0]
    assert "Source video: https://storage.example.com/video.mp4" in overall_text
    assert overall_ref == "feedback:rev-1:overall"
    assert overall_meta["video_url"] == "https://storage.example.com/video.mp4"
    assert overall_meta["agent_job_id"] == 42
    assert overall_meta["player_key"] == "22292"

    marker_text, marker_ref, marker_meta = parts[2]
    assert "Source video: https://storage.example.com/video.mp4" in marker_text
    assert marker_ref == "feedback:rev-1:m:0"
    assert marker_meta["timestamp_sec"] == 12.5
    assert marker_meta["category"] == "defending"
    assert marker_meta["chunk_kind"] == "marker"
