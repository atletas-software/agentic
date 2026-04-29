from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from app.models.auth import UserAccount

# Use PBKDF2 to avoid bcrypt backend issues on Python 3.13
# and bcrypt's 72-byte password limitation.
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def authenticate_user(email: str, password: str, db: Session) -> UserAccount | None:
    user = db.query(UserAccount).filter(UserAccount.email == email.lower()).one_or_none()
    if user is None or not user.is_active:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


def create_user(email: str, password: str, db: Session) -> UserAccount:
    user = UserAccount(email=email.lower(), password_hash=hash_password(password), is_active=True)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _jwt_secret() -> str:
    return os.getenv("JWT_SECRET_KEY", "dev-secret-change-me")


def create_access_token(user_id: str, email: str) -> str:
    expiry_minutes = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
    now = datetime.now(UTC)
    payload = {
        "sub": user_id,
        "email": email,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=expiry_minutes)).timestamp()),
    }
    return jwt.encode(payload, _jwt_secret(), algorithm="HS256")


def decode_access_token(token: str) -> dict:
    return jwt.decode(token, _jwt_secret(), algorithms=["HS256"])
