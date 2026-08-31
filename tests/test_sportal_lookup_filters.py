from backendapi.api.routes.admin import _lookup_sportal_profiles_by_filters


def test_lookup_filters_requires_at_least_one_criterion(monkeypatch):
    class FakeSession:
        pass

    assert _lookup_sportal_profiles_by_filters(FakeSession(), name="", email="", player_id=None) == []
