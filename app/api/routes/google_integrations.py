from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query
from fastapi import HTTPException, status
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.db import get_db
from app.dependencies.auth import get_current_user_id
from app.models.google_integration_api import SelectSheetRequest, UserSyncSettingsRequest
from app.models.google_oauth import SheetSyncRun, UserGoogleSheetConnection
from app.models.google_sheets_api import SheetUpdateRequest
from app.services.google_integration import (
    get_saved_sheet,
    get_selected_sheet,
    get_user_sync_settings,
    list_saved_sheets,
    resolve_polling_interval_seconds,
    save_sheet,
    set_active_sheet,
    set_selected_sheet,
    set_user_sync_settings,
)
from app.services.google_oauth import build_connect_url, exchange_code_for_tokens
from app.services.google_sheets import list_spreadsheet_tabs, list_user_sheets, read_sheet, update_sheet

router = APIRouter(prefix="/integrations/google", tags=["google-integrations"])
ALLOWED_POLLING_INTERVAL_SECONDS = [30, 35, 60, 90, 120, 150, 180, 210, 240, 270, 300]


@router.get("/connect")
async def google_connect(
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    url = build_connect_url(user_id=user_id, db=db)
    return {"authorization_url": url}


@router.get("/callback")
async def google_callback(code: str, state: str, db: Session = Depends(get_db)) -> RedirectResponse:
    user_id = exchange_code_for_tokens(code=code, state=state, db=db)
    return RedirectResponse(url=f"/app/success?google_connected=true&user_id={user_id}")


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


@router.get("/saved-sheets")
async def google_saved_sheets(
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    rows = list_saved_sheets(user_id=user_id, db=db)
    return {
        "saved_sheets": [
            {
                "spreadsheet_id": r.spreadsheet_id,
                "spreadsheet_name": r.spreadsheet_name,
                "is_active": r.is_active,
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
            }
            for r in rows
        ]
    }


@router.post("/saved-sheets")
async def google_save_sheet(
    request: SelectSheetRequest,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    row = save_sheet(
        user_id=user_id,
        spreadsheet_id=request.spreadsheet_id,
        spreadsheet_name=request.spreadsheet_name,
        db=db,
    )
    return {
        "success": True,
        "sheet": {
            "spreadsheet_id": row.spreadsheet_id,
            "spreadsheet_name": row.spreadsheet_name,
            "is_active": row.is_active,
        },
    }


@router.get("/saved-sheets/{spreadsheet_id}")
async def google_saved_sheet_details(
    spreadsheet_id: str,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    row = get_saved_sheet(user_id=user_id, spreadsheet_id=spreadsheet_id, db=db)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Saved sheet not found.")
    return {
        "sheet": {
            "spreadsheet_id": row.spreadsheet_id,
            "spreadsheet_name": row.spreadsheet_name,
            "is_active": row.is_active,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        },
        "tabs": list_spreadsheet_tabs(user_id=user_id, spreadsheet_id=spreadsheet_id, db=db),
    }


@router.post("/saved-sheets/{spreadsheet_id}/activate")
async def google_activate_saved_sheet(
    spreadsheet_id: str,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    row = set_active_sheet(user_id=user_id, spreadsheet_id=spreadsheet_id, db=db)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Saved sheet not found.")
    set_selected_sheet(
        user_id=user_id,
        spreadsheet_id=row.spreadsheet_id,
        spreadsheet_name=row.spreadsheet_name,
        db=db,
    )
    return {
        "success": True,
        "active_sheet": {
            "spreadsheet_id": row.spreadsheet_id,
            "spreadsheet_name": row.spreadsheet_name,
            "is_active": row.is_active,
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
        has_active_sheet = (
            db.query(UserGoogleSheetConnection.id)
            .filter(
                UserGoogleSheetConnection.user_id == user_id,
                UserGoogleSheetConnection.is_active.is_(True),
            )
            .first()
            is not None
        )
        if not has_active_sheet:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Set an active sheet before enabling sync.",
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
