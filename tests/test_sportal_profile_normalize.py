from backendapi.api.routes.admin import _normalize_external_profile_row, _sportal_sync_player_id


def test_normalize_external_profile_uses_user_id_as_player_id():
    row = _normalize_external_profile_row(
        {
            "player_id": 18115,
            "profile_id": 21121,
            "first_name": "Yuri",
            "last_name": "Test41",
            "email": "yuri.test41@gmail.com",
        }
    )
    assert row["player_id"] == 18115
    assert row["profile_id"] == 21121
    assert _sportal_sync_player_id(row) == 18115


def test_normalize_external_profile_falls_back_profile_id_to_player_id():
    row = _normalize_external_profile_row(
        {"player_id": 99, "first_name": "A", "last_name": "B", "email": ""}
    )
    assert row["player_id"] == 99
    assert row["profile_id"] == 99
