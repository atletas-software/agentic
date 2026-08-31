from backendapi.services.sql_player_sync import _player_id_from_row_dict


def test_player_id_prefers_reviewee_over_player_user_id():
    rd = {"reviewee_id": "22292", "player_user_id": "99999", "player_id": "1"}
    assert _player_id_from_row_dict(rd) == 22292


def test_player_id_falls_back_to_player_user_id():
    rd = {"player_user_id": "88888", "player_id": "1"}
    assert _player_id_from_row_dict(rd) == 88888
