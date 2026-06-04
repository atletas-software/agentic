from __future__ import annotations

from sqlalchemy import text

from backendapi.services.sql_player_sync import _external_engine


def write_profile_text_to_sportal(*, database_url: str, player_user_id: int, profile_text: str) -> dict[str, object]:
    """
    Persist profile_text to Sportal profile table for the player user_id.
    Other personal-context fields (videos, feedback) are aggregate SQL reads; admin overlays
    are stored in the app DB and merged at vector reindex time.
    """
    eng = _external_engine(database_url)
    if eng is None:
        return {"ok": False, "reason": "missing_sql_database_url"}
    stmt = text(
        """
        UPDATE profile
        SET profile_text = :profile_text, last_updated = UTC_TIMESTAMP()
        WHERE user_id = :player_user_id
        """
    )
    with eng.begin() as conn:
        result = conn.execute(
            stmt,
            {"profile_text": profile_text, "player_user_id": int(player_user_id)},
        )
        updated = int(result.rowcount or 0)
    if updated < 1:
        return {
            "ok": False,
            "reason": "profile_not_updated",
            "detail": f"No profile row updated for user_id={player_user_id}",
        }
    return {"ok": True, "rows_updated": updated}
