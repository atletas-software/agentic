from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

from fastapi import HTTPException, status
from google_auth_oauthlib.flow import Flow
from sqlalchemy.orm import Session

from app.core.logger import error, info
from app.models.google_oauth import GoogleOAuthState, GoogleOAuthToken
from app.services.auth import upsert_google_user

SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]


def _utc_now_matching(dt: datetime) -> datetime:
    # DB may return naive timestamps depending on backend/driver settings.
    # Compare using matching datetime "awareness" to avoid TypeError.
    now_aware = datetime.now(UTC)
    if dt.tzinfo is None:
        return now_aware.replace(tzinfo=None)
    return now_aware


def _flow(state: str | None = None, code_verifier: str | None = None) -> Flow:
    client_id = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
    redirect_uri = os.getenv("GOOGLE_REDIRECT_URI")
    if not client_id or not client_secret or not redirect_uri:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Google OAuth environment variables are not configured.",
        )
    flow = Flow.from_client_config(
        {
            "web": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        },
        scopes=SCOPES,
        state=state,
        code_verifier=code_verifier,
        autogenerate_code_verifier=code_verifier is None,
    )
    flow.redirect_uri = redirect_uri
    return flow


def build_connect_url(db: Session) -> str:
    state = str(uuid4())
    flow = _flow(state=state)
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    db_state = GoogleOAuthState(
        state=state,
        user_id="PENDING",
        code_verifier=flow.code_verifier,
        expires_at=datetime.now(UTC) + timedelta(minutes=15),
    )
    db.add(db_state)
    db.commit()
    info("google_oauth_connect_url_generated")
    return auth_url


def _fetch_google_user_email(access_token: str) -> str:
    request = Request(
        url="https://www.googleapis.com/oauth2/v2/userinfo",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    try:
        with urlopen(request, timeout=10) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8")
        except Exception:  # noqa: BLE001
            body = "<unavailable>"
        error(
            "google_userinfo_fetch_failed",
            status_code=getattr(exc, "code", None),
            body=body,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unable to resolve Google account profile. Please reconnect and grant profile/email access.",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        error("google_userinfo_fetch_failed", error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unable to resolve Google account profile.",
        ) from exc
    email = str(payload.get("email", "")).strip().lower()
    if not email:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Google account email not available.")
    return email


def exchange_code_for_tokens(code: str, state: str, db: Session) -> tuple[str, str, str]:
    state_row = db.get(GoogleOAuthState, state)
    if state_row is None or state_row.is_used or state_row.expires_at < _utc_now_matching(state_row.expires_at):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid or expired OAuth state.")

    if not state_row.code_verifier:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="OAuth session is incomplete. Please reconnect your Google account.",
        )
    flow = _flow(state=state, code_verifier=state_row.code_verifier)
    try:
        flow.fetch_token(code=code)
    except Exception as exc:  # noqa: BLE001
        error("google_oauth_token_exchange_failed", state=state, error=str(exc))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Failed to exchange OAuth code.") from exc

    creds = flow.credentials
    email = _fetch_google_user_email(creds.token)
    user = upsert_google_user(email=email, db=db)
    user_id = str(user.id)
    existing = db.query(GoogleOAuthToken).filter(GoogleOAuthToken.user_id == user_id).one_or_none()
    scopes = " ".join(creds.scopes or SCOPES)
    if existing is None:
        existing = GoogleOAuthToken(
            user_id=user_id,
            access_token=creds.token,
            refresh_token=creds.refresh_token or "",
            token_uri=creds.token_uri or "https://oauth2.googleapis.com/token",
            scopes=scopes,
            expiry=creds.expiry,
        )
        db.add(existing)
    else:
        existing.access_token = creds.token
        existing.refresh_token = creds.refresh_token or existing.refresh_token
        existing.token_uri = creds.token_uri or existing.token_uri
        existing.scopes = scopes
        existing.expiry = creds.expiry
        existing.updated_at = datetime.now(UTC)

    state_row.is_used = True
    state_row.user_id = user_id
    db.commit()
    info("google_oauth_callback_success", user_id=user_id, email=email)
    return user_id, email, existing.refresh_token
