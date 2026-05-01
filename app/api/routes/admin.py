from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db import get_db
from app.dependencies.auth import get_admin_session_context
from app.models.auth import UserAccount
from app.models.google_oauth import GoogleOAuthToken, SheetSyncEvent, SheetSyncRun, UserGoogleSheetSelection, UserSyncSetting

router = APIRouter(prefix="/admin-api", tags=["admin"])


@router.get("/users")
async def admin_users(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    connected_only: bool = Query(default=True),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    users = db.query(UserAccount).order_by(UserAccount.created_at.desc()).all()
    data: list[dict] = []
    for user in users:
        user_id_str = str(user.id)
        token = db.query(GoogleOAuthToken.id).filter(GoogleOAuthToken.user_id == user_id_str).first()
        if connected_only and token is None:
            continue
        selected = db.query(UserGoogleSheetSelection).filter(UserGoogleSheetSelection.user_id == user_id_str).one_or_none()
        setting = db.query(UserSyncSetting).filter(UserSyncSetting.user_id == user_id_str).one_or_none()
        latest_run = (
            db.query(SheetSyncRun)
            .filter(SheetSyncRun.user_id == user_id_str)
            .order_by(SheetSyncRun.started_at.desc())
            .first()
        )
        latest_completed_run = (
            db.query(SheetSyncRun)
            .filter(SheetSyncRun.user_id == user_id_str, SheetSyncRun.completed_at.is_not(None))
            .order_by(SheetSyncRun.completed_at.desc())
            .first()
        )
        total_runs = db.query(SheetSyncRun.id).filter(SheetSyncRun.user_id == user_id_str).count()
        totals = db.query(SheetSyncRun).filter(SheetSyncRun.user_id == user_id_str).all()
        total_rows_synced = sum((r.rows_inserted or 0) + (r.rows_updated or 0) for r in totals)
        recent_runs = (
            db.query(SheetSyncRun)
            .filter(SheetSyncRun.user_id == user_id_str)
            .order_by(SheetSyncRun.started_at.desc())
            .limit(2)
            .all()
        )
        recent_events = (
            db.query(SheetSyncEvent)
            .filter(SheetSyncEvent.user_id == user_id_str)
            .order_by(SheetSyncEvent.created_at.desc())
            .limit(2)
            .all()
        )
        last_sync_source = latest_completed_run or latest_run
        data.append(
            {
                "user_id": user_id_str,
                "email": user.email,
                "is_active": user.is_active,
                "google_connected": token is not None,
                "selected_sheet": (
                    {
                        "spreadsheet_id": selected.spreadsheet_id,
                        "spreadsheet_name": selected.spreadsheet_name,
                        "updated_at": selected.updated_at.isoformat() if selected.updated_at else None,
                    }
                    if selected
                    else None
                ),
                "sync_enabled": bool(setting.sync_enabled) if setting else False,
                "last_sync": (
                    {
                        "status": last_sync_source.status,
                        "started_at": last_sync_source.started_at.isoformat() if last_sync_source.started_at else None,
                        "completed_at": last_sync_source.completed_at.isoformat() if last_sync_source.completed_at else None,
                        "rows_inserted": last_sync_source.rows_inserted,
                        "rows_updated": last_sync_source.rows_updated,
                        "rows_failed": last_sync_source.rows_failed,
                    }
                    if last_sync_source
                    else None
                ),
                "total_runs": total_runs,
                "total_rows_synced": total_rows_synced,
                "raw_logs": {
                    "runs": [
                        {
                            "id": r.id,
                            "status": r.status,
                            "started_at": r.started_at.isoformat() if r.started_at else None,
                            "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                            "rows_scanned": r.rows_scanned,
                            "rows_inserted": r.rows_inserted,
                            "rows_updated": r.rows_updated,
                            "rows_failed": r.rows_failed,
                            "error_message": r.error_message,
                        }
                        for r in recent_runs
                    ],
                    "events": [
                        {
                            "id": e.id,
                            "run_id": e.run_id,
                            "action": e.action,
                            "status": e.status,
                            "message": e.message,
                            "created_at": e.created_at.isoformat() if e.created_at else None,
                        }
                        for e in recent_events
                    ],
                },
            }
        )
    total = len(data)
    paged = data[offset : offset + limit]
    return {"users": paged, "pagination": {"total": total, "offset": offset, "limit": limit}}


@router.get("/users/{user_id}/runs")
async def admin_user_runs(
    user_id: str,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    base = db.query(SheetSyncRun).filter(SheetSyncRun.user_id == user_id).order_by(SheetSyncRun.started_at.desc())
    total = base.count()
    rows = base.offset(offset).limit(limit).all()
    return {
        "runs": [
            {
                "id": r.id,
                "status": r.status,
                "spreadsheet_id": r.spreadsheet_id,
                "tab_name": r.tab_name,
                "rows_scanned": r.rows_scanned,
                "rows_inserted": r.rows_inserted,
                "rows_updated": r.rows_updated,
                "rows_failed": r.rows_failed,
                "error_message": r.error_message,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            }
            for r in rows
        ],
        "pagination": {"total": total, "offset": offset, "limit": limit},
    }


@router.get("/users/{user_id}/events")
async def admin_user_events(
    user_id: str,
    run_id: int | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    base = db.query(SheetSyncEvent).filter(SheetSyncEvent.user_id == user_id)
    if run_id is not None:
        base = base.filter(SheetSyncEvent.run_id == run_id)
    base = base.order_by(SheetSyncEvent.created_at.desc())
    total = base.count()
    rows = base.offset(offset).limit(limit).all()
    return {
        "events": [
            {
                "id": e.id,
                "run_id": e.run_id,
                "action": e.action,
                "status": e.status,
                "message": e.message,
                "tab_name": e.tab_name,
                "source_row_key": e.source_row_key,
                "row_number": e.row_number,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in rows
        ],
        "pagination": {"total": total, "offset": offset, "limit": limit},
    }


@router.get("/runs/{run_id}")
async def admin_run_detail(
    run_id: int,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    run = db.query(SheetSyncRun).filter(SheetSyncRun.id == run_id).one_or_none()
    if run is None:
        return {"run": None}
    return {
        "run": {
            "id": run.id,
            "user_id": run.user_id,
            "status": run.status,
            "spreadsheet_id": run.spreadsheet_id,
            "tab_name": run.tab_name,
            "rows_scanned": run.rows_scanned,
            "rows_inserted": run.rows_inserted,
            "rows_updated": run.rows_updated,
            "rows_failed": run.rows_failed,
            "error_message": run.error_message,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        }
    }


@router.get("/users/{user_id}/live-logs")
async def admin_user_live_logs(
    user_id: str,
    limit: int = Query(default=100, ge=1, le=500),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    run = (
        db.query(SheetSyncRun)
        .filter(
            SheetSyncRun.user_id == user_id,
            SheetSyncRun.status == "RUNNING",
            SheetSyncRun.completed_at.is_(None),
        )
        .order_by(SheetSyncRun.started_at.desc())
        .first()
    )
    if run is None:
        return {"run": None, "events": []}
    events = (
        db.query(SheetSyncEvent)
        .filter(SheetSyncEvent.user_id == user_id, SheetSyncEvent.run_id == run.id)
        .order_by(SheetSyncEvent.created_at.desc())
        .limit(limit)
        .all()
    )
    return {
        "run": {
            "id": run.id,
            "status": run.status,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        },
        "events": [
            {
                "id": e.id,
                "run_id": e.run_id,
                "action": e.action,
                "status": e.status,
                "message": e.message,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in events
        ],
    }


@router.get("/users/{user_id}/raw-logs")
async def admin_user_raw_logs(
    user_id: str,
    runs_limit: int = Query(default=20, ge=1, le=100),
    events_limit: int = Query(default=20, ge=1, le=200),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    runs = (
        db.query(SheetSyncRun)
        .filter(SheetSyncRun.user_id == user_id)
        .order_by(SheetSyncRun.started_at.desc())
        .limit(runs_limit)
        .all()
    )
    events = (
        db.query(SheetSyncEvent)
        .filter(SheetSyncEvent.user_id == user_id)
        .order_by(SheetSyncEvent.created_at.desc())
        .limit(events_limit)
        .all()
    )
    return {
        "runs": [
            {
                "id": r.id,
                "status": r.status,
                "spreadsheet_id": r.spreadsheet_id,
                "tab_name": r.tab_name,
                "rows_scanned": r.rows_scanned,
                "rows_inserted": r.rows_inserted,
                "rows_updated": r.rows_updated,
                "rows_failed": r.rows_failed,
                "error_message": r.error_message,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            }
            for r in runs
        ],
        "events": [
            {
                "id": e.id,
                "run_id": e.run_id,
                "action": e.action,
                "status": e.status,
                "message": e.message,
                "tab_name": e.tab_name,
                "source_row_key": e.source_row_key,
                "row_number": e.row_number,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in events
        ],
    }
