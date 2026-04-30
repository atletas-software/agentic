from __future__ import annotations

from fastapi import APIRouter, Cookie, Depends, Response
from sqlalchemy.orm import Session

from app.db import get_db
from app.dependencies.auth import get_current_user_context
from app.services.auth import deactivate_session

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/me")
async def me(context: dict = Depends(get_current_user_context)) -> dict:
    return {"user_id": context["user_id"], "email": context["email"]}


@router.post("/logout")
async def logout(
    response: Response,
    session_id: str | None = Cookie(default=None),
    db: Session = Depends(get_db),
) -> dict:
    if session_id:
        deactivate_session(session_id=session_id, db=db)
    response.delete_cookie("session_id")
    response.delete_cookie("session_email")
    return {"success": True}
