"""Redirect legacy HTML UI paths to the Next.js frontend."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import RedirectResponse

from backendapi.services.frontend_origin import frontend_public_origin

router = APIRouter()


def _frontend_base() -> str:
    return frontend_public_origin()


def _redirect(path: str) -> RedirectResponse:
    base = _frontend_base()
    if not path.startswith("/"):
        path = f"/{path}"
    return RedirectResponse(url=f"{base}{path}", status_code=307)


@router.get("/")
async def root_redirect() -> RedirectResponse:
    return _redirect("/connect")


@router.get("/app")
async def app_redirect() -> RedirectResponse:
    return _redirect("/sheets")


@router.get("/app/sheets")
async def app_sheets_redirect() -> RedirectResponse:
    return _redirect("/sheets")


@router.get("/app/sheets/details")
async def app_sheets_details_redirect() -> RedirectResponse:
    return _redirect("/sheets")


@router.get("/app/connect")
async def app_connect_redirect() -> RedirectResponse:
    return _redirect("/connect")


@router.get("/app/success")
async def app_success_redirect() -> RedirectResponse:
    return _redirect("/success")


@router.get("/app/settings")
async def app_settings_redirect() -> RedirectResponse:
    return _redirect("/settings")


@router.get("/app/agents")
async def app_agents_redirect() -> RedirectResponse:
    return _redirect("/admin/agents-lab")


@router.get("/app/player-memory")
async def app_player_memory_redirect() -> RedirectResponse:
    return _redirect("/admin/player-memory")


@router.get("/app/static/player_memory.html")
async def player_memory_legacy_redirect() -> RedirectResponse:
    return _redirect("/admin/player-memory")


@router.get("/admin")
async def admin_redirect() -> RedirectResponse:
    return _redirect("/admin")


@router.get("/admin/agents-lab")
async def admin_agents_lab_redirect() -> RedirectResponse:
    return _redirect("/admin/agents-lab")


@router.get("/admin/login")
async def admin_login_redirect() -> RedirectResponse:
    return _redirect("/admin/login")


@router.get("/app/login")
async def app_login_redirect() -> RedirectResponse:
    return _redirect("/connect")


@router.get("/app/register")
async def app_register_redirect() -> RedirectResponse:
    return _redirect("/connect")
