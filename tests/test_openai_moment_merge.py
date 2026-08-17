from agents.feedback.openai_service import OpenAIPickedMoment, merge_openai_picked_moments


def test_merge_keeps_high_importance_and_drops_nearby_duplicates():
    merged = merge_openai_picked_moments(
        [
            OpenAIPickedMoment(timestamp_sec=10, importance="low", action="jog"),
            OpenAIPickedMoment(timestamp_sec=11, importance="high", action="1v1"),
            OpenAIPickedMoment(timestamp_sec=40, importance="medium", action="press"),
        ],
        min_gap_sec=3.0,
        max_moments=10,
    )
    assert [round(m.timestamp_sec) for m in merged] == [11, 40]
    assert merged[0].action == "1v1"


def test_merge_caps_max_moments():
    moments = [
        OpenAIPickedMoment(timestamp_sec=float(i * 10), importance="high", action=str(i))
        for i in range(8)
    ]
    merged = merge_openai_picked_moments(moments, min_gap_sec=3.0, max_moments=3)
    assert len(merged) == 3
    assert [round(m.timestamp_sec) for m in merged] == [0, 10, 20]
