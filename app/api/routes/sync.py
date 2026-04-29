from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db import get_db
from app.dependencies.auth import get_current_user_id
from app.models.google_oauth import SheetSyncEvent, SheetSyncRowState, SheetSyncRun
from app.services.sheet_sync import run_sync_once_for_users

router = APIRouter(prefix="/sync", tags=["sheet-sync"])


@router.post("/run-once")
async def sync_run_once(
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    result = run_sync_once_for_users(db=db, user_ids=[user_id])
    return {"success": True, "result": result}


@router.get("/runs")
async def sync_runs(
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    rows = (
        db.query(SheetSyncRun)
        .filter(SheetSyncRun.user_id == user_id)
        .order_by(SheetSyncRun.started_at.desc())
        .limit(50)
        .all()
    )
    return {
        "runs": [
            {
                "id": r.id,
                "spreadsheet_id": r.spreadsheet_id,
                "tab_name": r.tab_name,
                "status": r.status,
                "rows_scanned": r.rows_scanned,
                "rows_inserted": r.rows_inserted,
                "rows_updated": r.rows_updated,
                "rows_failed": r.rows_failed,
                "error_message": r.error_message,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            }
            for r in rows
        ]
    }


@router.get("/runs/{run_id}/events")
async def sync_run_events(
    run_id: int,
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    events = (
        db.query(SheetSyncEvent)
        .filter(SheetSyncEvent.run_id == run_id, SheetSyncEvent.user_id == user_id)
        .order_by(SheetSyncEvent.id.asc())
        .all()
    )
    return {
        "events": [
            {
                "id": e.id,
                "action": e.action,
                "status": e.status,
                "message": e.message,
                "source_row_key": e.source_row_key,
                "row_number": e.row_number,
                "payload_snapshot": e.payload_snapshot,
                "destination_response": e.destination_response,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in events
        ]
    }


@router.get("/states")
async def sync_states(
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    rows = (
        db.query(SheetSyncRowState)
        .filter(SheetSyncRowState.user_id == user_id)
        .order_by(SheetSyncRowState.last_synced_at.desc())
        .limit(200)
        .all()
    )
    return {
        "states": [
            {
                "spreadsheet_id": r.spreadsheet_id,
                "tab_name": r.tab_name,
                "row_number": r.row_number,
                "source_row_key": r.source_row_key,
                "row_hash": r.row_hash,
                "status": r.status,
                "destination_row_number": r.destination_row_number,
                "last_synced_at": r.last_synced_at.isoformat() if r.last_synced_at else None,
                "attempt_count": r.attempt_count,
                "last_error": r.last_error,
            }
            for r in rows
        ]
    }


@router.get("/skipped-rows")
async def sync_skipped_rows(
    limit: int = Query(default=100, ge=1, le=500),
    user_id: str = Depends(get_current_user_id),
    db: Session = Depends(get_db),
) -> dict:
    rows = (
        db.query(SheetSyncEvent)
        .filter(
            SheetSyncEvent.user_id == user_id,
            SheetSyncEvent.action == "SKIP",
            SheetSyncEvent.message.like("Skipped incomplete row.%"),
        )
        .order_by(SheetSyncEvent.created_at.desc())
        .limit(limit)
        .all()
    )
    return {
        "skipped_rows": [
            {
                "id": r.id,
                "run_id": r.run_id,
                "spreadsheet_id": r.spreadsheet_id,
                "tab_name": r.tab_name,
                "source_row_key": r.source_row_key,
                "row_number": r.row_number,
                "reason": r.message,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]
    }
