from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, RedirectResponse

router = APIRouter()


def _read_static_page(filename: str) -> str:
    with open(f"app/static/{filename}", "r", encoding="utf-8") as fh:
        return fh.read()


@router.get("/", response_class=HTMLResponse)
async def root_ui() -> str:
    return _read_static_page("google_connect.html")


@router.get("/app", response_class=HTMLResponse)
async def app_ui() -> str:
    return _read_static_page("google_connect.html")


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


@router.get("/app/login", response_class=HTMLResponse)
async def app_login_ui() -> RedirectResponse:
    return RedirectResponse(url="/app/connect")


@router.get("/app/register", response_class=HTMLResponse)
async def app_register_ui() -> RedirectResponse:
    return RedirectResponse(url="/app/connect")
