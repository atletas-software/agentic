from __future__ import annotations

from fastapi import Depends, Header, HTTPException, status

from app.services.auth import decode_access_token


def get_current_user_context(authorization: str | None = Header(default=None)) -> dict[str, str]:
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header.",
        )
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid bearer token format.")
    try:
        payload = decode_access_token(token)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token.") from exc
    user_id = str(payload.get("sub", "")).strip()
    email = str(payload.get("email", "")).strip()
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token missing subject.")
    return {"user_id": user_id, "email": email}


def get_current_user_id(context: dict[str, str] = Depends(get_current_user_context)) -> str:
    return context["user_id"]
