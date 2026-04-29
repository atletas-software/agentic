from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from app.models.google_oauth import UserSyncBackoff

QUOTA_HINTS = [
    "quota",
    "rate limit",
    "ratelimit",
    "429",
    "userratelimitexceeded",
    "resource_exhausted",
    "too many requests",
]


def _is_quota_error(message: str | None) -> bool:
    if not message:
        return False
    lower = message.lower()
    return any(hint in lower for hint in QUOTA_HINTS)


def _get_or_create(user_id: str, db: Session) -> UserSyncBackoff:
    row = db.query(UserSyncBackoff).filter(UserSyncBackoff.user_id == user_id).one_or_none()
    if row is None:
        row = UserSyncBackoff(user_id=user_id, consecutive_quota_errors=0, next_allowed_at=None)
        db.add(row)
        db.commit()
        db.refresh(row)
    return row


def is_user_blocked(user_id: str, db: Session, now: datetime | None = None) -> tuple[bool, datetime | None]:
    row = db.query(UserSyncBackoff).filter(UserSyncBackoff.user_id == user_id).one_or_none()
    if row is None or row.next_allowed_at is None:
        return False, None
    current = now or datetime.now(UTC)
    return row.next_allowed_at > current, row.next_allowed_at


def register_sync_result(user_id: str, run_error_message: str | None, db: Session) -> dict[str, str | int | None]:
    row = _get_or_create(user_id=user_id, db=db)
    if _is_quota_error(run_error_message):
        base_minutes = int(os.getenv("SYNC_QUOTA_BACKOFF_BASE_MINUTES", "1"))
        max_minutes = int(os.getenv("SYNC_QUOTA_BACKOFF_MAX_MINUTES", "60"))
        row.consecutive_quota_errors += 1
        wait_minutes = min(base_minutes * (2 ** (row.consecutive_quota_errors - 1)), max_minutes)
        row.next_allowed_at = datetime.now(UTC) + timedelta(minutes=wait_minutes)
    else:
        row.consecutive_quota_errors = 0
        row.next_allowed_at = None
    db.commit()
    db.refresh(row)
    return {
        "consecutive_quota_errors": row.consecutive_quota_errors,
        "next_allowed_at": row.next_allowed_at.isoformat() if row.next_allowed_at else None,
    }
