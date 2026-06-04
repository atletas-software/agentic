from backendapi.services.player_context_document import document_from_sql_row, merge_personal_documents


def test_document_from_sql_row_includes_all_fields():
    row = {
        "player_user_id": "28",
        "player_name": "Gleb BogdanovDC",
        "club_name": "All-Around Player Soccer Academy",
        "profile_text": "asdfsadfasdf",
        "videos": '[{"summary": "u9 game", "description": null}]',
        "feedback": '{"notes": ["note1"], "video_annotations": ["ann1"]}',
    }
    doc = document_from_sql_row(row)
    assert doc["player_user_id"] == 28
    assert doc["player_name"] == "Gleb BogdanovDC"
    assert doc["club_name"] == "All-Around Player Soccer Academy"
    assert doc["profile_text"] == "asdfsadfasdf"
    assert len(doc["videos"]) == 1
    assert doc["videos"][0]["summary"] == "u9 game"
    assert doc["feedback"]["notes"] == ["note1"]
    assert doc["feedback"]["video_annotations"] == ["ann1"]


def test_merge_overlay_wins():
    sql_doc = document_from_sql_row(
        {
            "player_user_id": "1",
            "player_name": "A",
            "club_name": "C1",
            "profile_text": "old",
            "videos": "[]",
            "feedback": '{"notes": [], "video_annotations": []}',
        }
    )
    merged = merge_personal_documents(sql_doc, {"profile_text": "new", "club_name": "C2"})
    assert merged["profile_text"] == "new"
    assert merged["club_name"] == "C2"


def test_merge_empty_overlay_lists_do_not_wipe_sql():
    sql_doc = document_from_sql_row(
        {
            "player_user_id": "99",
            "player_name": "Blade Gordon",
            "club_name": "AthleteFocus Team",
            "profile_text": "",
            "videos": '[{"summary": "game 1", "description": null}]',
            "feedback": '{"notes": ["coach note"], "video_annotations": ["ann a", "ann b"]}',
        }
    )
    sparse_manual = {
        "player_user_id": 99,
        "player_name": "Blade Gordon",
        "club_name": "AthleteFocus Team",
        "profile_text": "manual profile only",
        "videos": [],
        "feedback": {"notes": [], "video_annotations": []},
    }
    merged = merge_personal_documents(sql_doc, sparse_manual)
    assert merged["profile_text"] == "manual profile only"
    assert len(merged["videos"]) == 1
    assert merged["feedback"]["notes"] == ["coach note"]
    assert len(merged["feedback"]["video_annotations"]) == 2
