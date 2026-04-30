from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query
from fastapi import HTTPException, status
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.db import get_db
from app.dependencies.auth import get_current_user_id
from app.models.google_integration_api import SelectSheetRequest, UserSyncSettingsRequest
from app.models.google_oauth import SheetSyncRun
from app.models.google_sheets_api import SheetUpdateRequest
from app.services.google_integration import (
    get_selected_sheet,
    get_user_sync_settings,
    resolve_polling_interval_seconds,
    set_selected_sheet,
    set_user_sync_settings,
)
from app.services.google_oauth import build_connect_url, exchange_code_for_tokens
from app.services.google_sheets import list_spreadsheet_tabs, list_user_sheets, read_sheet, update_sheet
from app.services.auth import create_user_session

router = APIRouter(prefix="/integrations/google", tags=["google-integrations"])
ALLOWED_POLLING_INTERVAL_SECONDS = [30, 35, 60, 90, 120, 150, 180, 210, 240, 270, 300]


@router.get("/connect")
async def google_connect(
    db: Session = Depends(get_db),
) -> dict:
    url = build_connect_url(db=db)
    return {"authorization_url": url}


@router.get("/callback")
async def google_callback(code: str, state: str, db: Session = Depends(get_db)) -> RedirectResponse:
    user_id, email, _ = exchange_code_for_tokens(code=code, state=state, db=db)
    session = create_user_session(user_id=int(user_id), db=db)
    response = RedirectResponse(url="/app/sheets")
    response.set_cookie(
        key="session_id",
        value=session.session_id,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,
    )
    response.set_cookie(
        key="session_email",
        value=email,
        httponly=False,
        secure=False,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,
    )
    return response


@router.get("/sheets")
async def google_sheets_list(
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    return {"files": list_user_sheets(user_id=user_id, db=db)}


@router.get("/sheets/{spreadsheet_id}")
async def google_sheet_read(
    spreadsheet_id: str,
    range: str = Query(default="Sheet1"),  # noqa: A002
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    return read_sheet(user_id=user_id, spreadsheet_id=spreadsheet_id, range_name=range, db=db)


@router.get("/sheets/{spreadsheet_id}/tabs")
async def google_sheet_tabs(
    spreadsheet_id: str,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    return {"tabs": list_spreadsheet_tabs(user_id=user_id, spreadsheet_id=spreadsheet_id, db=db)}


@router.post("/sheets/{spreadsheet_id}")
async def google_sheet_update(
    spreadsheet_id: str,
    request: SheetUpdateRequest,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    return update_sheet(
        user_id=user_id,
        spreadsheet_id=spreadsheet_id,
        range_name=request.range,
        values=request.values,
        db=db,
    )


@router.get("/selected-sheet")
async def google_selected_sheet(
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    row = get_selected_sheet(user_id=user_id, db=db)
    if row is None:
        return {"selected_sheet": None}
    return {
        "selected_sheet": {
            "spreadsheet_id": row.spreadsheet_id,
            "spreadsheet_name": row.spreadsheet_name,
        }
    }


@router.post("/selected-sheet")
async def google_set_selected_sheet(
    request: SelectSheetRequest,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    row = set_selected_sheet(
        user_id=user_id,
        spreadsheet_id=request.spreadsheet_id,
        spreadsheet_name=request.spreadsheet_name,
        db=db,
    )
    return {
        "success": True,
        "selected_sheet": {
            "spreadsheet_id": row.spreadsheet_id,
            "spreadsheet_name": row.spreadsheet_name,
        },
    }


@router.get("/sync-settings")
async def google_get_sync_settings(
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    row = get_user_sync_settings(user_id=user_id, db=db)
    return {
        "settings": {
            "polling_interval_seconds": resolve_polling_interval_seconds(row.polling_interval_minutes),
            "sync_enabled": row.sync_enabled,
        }
    }


@router.post("/sync-settings")
async def google_set_sync_settings(
    request: UserSyncSettingsRequest,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    if request.sync_enabled is True:
        selected = get_selected_sheet(user_id=user_id, db=db)
        if selected is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Select a sheet before enabling sync.",
            )
    seconds = request.polling_interval_seconds
    if seconds is None and request.polling_interval_minutes is not None:
        # Backward compatibility for older clients.
        seconds = request.polling_interval_minutes * 60
    if seconds is None and request.sync_enabled is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide polling_interval_seconds and/or sync_enabled.",
        )
    if seconds is not None and seconds not in ALLOWED_POLLING_INTERVAL_SECONDS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"polling_interval_seconds must be one of: {ALLOWED_POLLING_INTERVAL_SECONDS}",
        )
    # Reuse existing column for now; value is interpreted as seconds.
    row = set_user_sync_settings(
        user_id=user_id,
        polling_interval_minutes=seconds,
        db=db,
        sync_enabled=request.sync_enabled,
    )
    return {
        "success": True,
        "settings": {
            "polling_interval_seconds": resolve_polling_interval_seconds(row.polling_interval_minutes),
            "sync_enabled": row.sync_enabled,
        },
    }


@router.get("/sync-status")
async def google_sync_status(
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    setting = get_user_sync_settings(user_id=user_id, db=db)
    latest_run = (
        db.query(SheetSyncRun)
        .filter(SheetSyncRun.user_id == user_id)
        .order_by(SheetSyncRun.started_at.desc())
        .first()
    )
    now = datetime.now(UTC)
    if latest_run is None or latest_run.started_at is None:
        next_due = now
        last_run_at = None
        last_error = None
        status = "NEVER_RUN"
    else:
        last_run_at = latest_run.started_at
        interval_seconds = resolve_polling_interval_seconds(setting.polling_interval_minutes)
        next_due = latest_run.started_at + timedelta(seconds=interval_seconds)
        last_error = latest_run.error_message
        status = latest_run.status
    interval_seconds = resolve_polling_interval_seconds(setting.polling_interval_minutes)
    return {
        "status": {
            "polling_interval_seconds": interval_seconds,
            "last_run_at": last_run_at.isoformat() if last_run_at else None,
            "next_due_at": next_due.isoformat(),
            "last_run_status": status,
            "is_due": next_due <= now,
            "allowed_interval_seconds": ALLOWED_POLLING_INTERVAL_SECONDS,
            "sync_enabled": setting.sync_enabled,
        }
    }
