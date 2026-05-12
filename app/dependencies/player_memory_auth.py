from __future__ import annotations

import os

from fastapi import Cookie, Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from app.db import get_db
from app.dependencies.auth import get_admin_session_context
from app.models.auth import UserAccount


def get_player_memory_user_id(
    db: Session = Depends(get_db),
    admin_session_id: str | None = Cookie(default=None),
    authorization: str | None = Header(default=None),
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> str:
    """
    Admin browser uses a system workspace user-id; MCP automation can still target
    specific users via Bearer token + X-User-Id.
    """
    token = (os.getenv("PLAYER_MEMORY_MCP_TOKEN") or "").strip()
    if token and authorization and authorization.startswith("Bearer ") and x_user_id:
        presented = authorization.split(" ", 1)[1].strip()
        if presented != token:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token.")
        try:
            uid = int(x_user_id.strip())
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid X-User-Id.") from exc
        user = db.get(UserAccount, uid)
        if user is None or not user.is_active:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found.")
        return str(user.id)

    get_admin_session_context(admin_session_id=admin_session_id, db=db)
    # System-scoped admin pipeline workspace (single tenant for destination-tab-driven sync).
    return "0"
