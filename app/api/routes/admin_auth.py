from __future__ import annotations

import os

from fastapi import APIRouter, Cookie, Depends, HTTPException, Response, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db import get_db
from app.dependencies.auth import get_admin_session_context
from app.services.auth import create_admin_session, deactivate_admin_session

router = APIRouter(prefix="/admin-auth", tags=["admin-auth"])


class AdminLoginRequest(BaseModel):
    email: str
    password: str


@router.post("/login")
async def admin_login(payload: AdminLoginRequest, response: Response, db: Session = Depends(get_db)) -> dict:
    email = payload.email.lower().strip()
    raw_admins = os.getenv("ADMIN_EMAILS", "")
    allowed = {item.strip().lower() for item in raw_admins.split(",") if item.strip()}
    if email not in allowed:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin credentials.")

    expected_password = os.getenv("ADMIN_PASSWORD", "")
    if not expected_password or payload.password != expected_password:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin credentials.")

    session = create_admin_session(email=email, db=db)
    response.set_cookie(
        key="admin_session_id",
        value=session.session_id,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=max(1, int((session.expires_at - session.created_at).total_seconds())),
    )
    return {"success": True, "email": email}


@router.get("/me")
async def admin_me(context: dict = Depends(get_admin_session_context)) -> dict:
    return {"email": context["email"]}


@router.post("/logout")
async def admin_logout(
    response: Response,
    admin_session_id: str | None = Cookie(default=None),
    db: Session = Depends(get_db),
) -> dict:
    if admin_session_id:
        deactivate_admin_session(session_id=admin_session_id, db=db)
    response.delete_cookie("admin_session_id")
    return {"success": True}
