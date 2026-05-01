from __future__ import annotations

import os
from datetime import UTC, datetime

from fastapi import Cookie, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.auth import AdminSession, AppSession, UserAccount


def get_current_user_context(
    session_id: str | None = Cookie(default=None),
    db: Session = Depends(get_db),
) -> dict[str, str]:
    if not session_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing session.",
        )
    now = datetime.now(UTC)
    session = (
        db.query(AppSession)
        .filter(
            AppSession.session_id == session_id,
            AppSession.is_active.is_(True),
            AppSession.expires_at > now,
        )
        .one_or_none()
    )
    if session is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired session.")
    user = db.get(UserAccount, session.user_id)
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid user session.")
    session.last_seen_at = now
    db.commit()
    return {"user_id": str(user.id), "email": user.email}


def get_current_user_id(context: dict[str, str] = Depends(get_current_user_context)) -> str:
    return context["user_id"]


def get_admin_user_context(context: dict[str, str] = Depends(get_current_user_context)) -> dict[str, str]:
    raw_admins = os.getenv("ADMIN_EMAILS", "")
    allowed = {email.strip().lower() for email in raw_admins.split(",") if email.strip()}
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin panel is not configured. Set ADMIN_EMAILS.",
        )
    if context["email"].lower() not in allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access denied.")
    return context


def get_admin_session_context(
    admin_session_id: str | None = Cookie(default=None),
    db: Session = Depends(get_db),
) -> dict[str, str]:
    if not admin_session_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing admin session.")
    now = datetime.now(UTC)
    session = (
        db.query(AdminSession)
        .filter(
            AdminSession.session_id == admin_session_id,
            AdminSession.is_active.is_(True),
            AdminSession.expires_at > now,
        )
        .one_or_none()
    )
    if session is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired admin session.")
    raw_admins = os.getenv("ADMIN_EMAILS", "")
    allowed = {email.strip().lower() for email in raw_admins.split(",") if email.strip()}
    if session.email.lower() not in allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access denied.")
    session.last_seen_at = now
    db.commit()
    return {"email": session.email}
