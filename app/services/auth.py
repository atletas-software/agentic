from __future__ import annotations

import os
import secrets
from datetime import UTC, datetime, timedelta

from passlib.context import CryptContext
from sqlalchemy.orm import Session

from app.models.auth import AdminSession, AppSession, UserAccount

# Use PBKDF2 to avoid bcrypt backend issues on Python 3.13
# and bcrypt's 72-byte password limitation.
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def upsert_google_user(email: str, db: Session) -> UserAccount:
    normalized_email = email.lower().strip()
    user = db.query(UserAccount).filter(UserAccount.email == normalized_email).one_or_none()
    if user is None:
        # Password auth is deprecated, but the column remains required.
        user = UserAccount(
            email=normalized_email,
            password_hash=hash_password(secrets.token_urlsafe(32)),
            is_active=True,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        return user
    if not user.is_active:
        user.is_active = True
        db.commit()
        db.refresh(user)
    return user


def create_user_session(user_id: int, db: Session) -> AppSession:
    ttl_days = int(os.getenv("SESSION_TTL_DAYS", "30"))
    now = datetime.now(UTC)
    session = AppSession(
        user_id=user_id,
        session_id=secrets.token_urlsafe(48),
        expires_at=now + timedelta(days=ttl_days),
        is_active=True,
        created_at=now,
        last_seen_at=now,
    )
    db.add(session)
    db.commit()
    db.refresh(session)
    return session


def deactivate_session(session_id: str, db: Session) -> None:
    row = db.query(AppSession).filter(AppSession.session_id == session_id, AppSession.is_active.is_(True)).one_or_none()
    if row is None:
        return
    row.is_active = False
    db.commit()


def create_admin_session(email: str, db: Session) -> AdminSession:
    ttl_days = int(os.getenv("ADMIN_SESSION_TTL_DAYS", "7"))
    now = datetime.now(UTC)
    session = AdminSession(
        email=email.lower().strip(),
        session_id=secrets.token_urlsafe(48),
        expires_at=now + timedelta(days=ttl_days),
        is_active=True,
        created_at=now,
        last_seen_at=now,
    )
    db.add(session)
    db.commit()
    db.refresh(session)
    return session


def deactivate_admin_session(session_id: str, db: Session) -> None:
    row = db.query(AdminSession).filter(AdminSession.session_id == session_id, AdminSession.is_active.is_(True)).one_or_none()
    if row is None:
        return
    row.is_active = False
    db.commit()
