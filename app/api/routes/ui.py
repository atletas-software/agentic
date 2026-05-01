from __future__ import annotations

from fastapi import APIRouter, Cookie, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from sqlalchemy.orm import Session

from app.db import get_db
from app.dependencies.auth import get_admin_session_context, get_current_user_context

router = APIRouter()


def _read_static_page(filename: str) -> str:
    with open(f"app/static/{filename}", "r", encoding="utf-8") as fh:
        return fh.read()


@router.get("/", response_class=HTMLResponse, response_model=None)
async def root_ui(
    session_id: str | None = Cookie(default=None),
    db: Session = Depends(get_db),
) -> Response:
    if not session_id:
        return HTMLResponse(content=_read_static_page("google_connect.html"))
    try:
        get_current_user_context(session_id=session_id, db=db)
        return RedirectResponse(url="/app/sheets")
    except HTTPException:
        return HTMLResponse(content=_read_static_page("google_connect.html"))


@router.get("/app", response_class=HTMLResponse, response_model=None)
async def app_ui(
    session_id: str | None = Cookie(default=None),
    db: Session = Depends(get_db),
) -> Response:
    if not session_id:
        return HTMLResponse(content=_read_static_page("google_connect.html"))
    try:
        get_current_user_context(session_id=session_id, db=db)
        return RedirectResponse(url="/app/sheets")
    except HTTPException:
        return HTMLResponse(content=_read_static_page("google_connect.html"))


@router.get("/app/sheets", response_class=HTMLResponse)
async def app_sheets_ui() -> str:
    return _read_static_page("google_sheets.html")


@router.get("/app/sheets/details", response_class=HTMLResponse)
async def app_sheets_details_ui() -> RedirectResponse:
    return RedirectResponse(url="/app/sheets")


@router.get("/app/connect", response_class=HTMLResponse)
async def app_connect_ui() -> str:
    return _read_static_page("google_connect.html")


@router.get("/app/success", response_class=HTMLResponse)
async def app_success_ui() -> str:
    return _read_static_page("google_success.html")


@router.get("/app/settings", response_class=HTMLResponse)
async def app_settings_ui() -> str:
    return _read_static_page("google_settings.html")


@router.get("/admin", response_class=HTMLResponse, response_model=None)
async def admin_ui(
    admin_session_id: str | None = Cookie(default=None),
    db: Session = Depends(get_db),
) -> Response:
    if not admin_session_id:
        return RedirectResponse(url="/admin/login?next=/admin")
    try:
        get_admin_session_context(admin_session_id=admin_session_id, db=db)
    except HTTPException:
        return RedirectResponse(url="/admin/login?next=/admin")
    return HTMLResponse(content=_read_static_page("admin.html"))


@router.get("/admin/login", response_class=HTMLResponse, response_model=None)
async def admin_login_ui(
    next_path: str = Query(default="/admin", alias="next"),
    admin_session_id: str | None = Cookie(default=None),
    db: Session = Depends(get_db),
) -> Response:
    safe_next = next_path if next_path.startswith("/admin") else "/admin"
    if admin_session_id:
        try:
            get_admin_session_context(admin_session_id=admin_session_id, db=db)
            return RedirectResponse(url=safe_next)
        except HTTPException:
            pass
    page = _read_static_page("admin_login.html")
    return HTMLResponse(content=page.replace("__ADMIN_NEXT__", safe_next, 1))


@router.get("/app/login", response_class=HTMLResponse)
async def app_login_ui() -> RedirectResponse:
    return RedirectResponse(url="/app/connect")


@router.get("/app/register", response_class=HTMLResponse)
async def app_register_ui() -> RedirectResponse:
    return RedirectResponse(url="/app/connect")
