from __future__ import annotations

import json
import os
import re
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from pinecone import Pinecone

from app.core.logger import info as log_info
from app.db import get_db
from app.dependencies.auth import get_admin_session_context
from app.models.auth import UserAccount
from app.models.workspace import AgentJob, Workspace, WorkspaceContextItem
from app.models.google_oauth import GoogleOAuthToken, SheetSyncEvent, SheetSyncRun, UserGoogleSheetSelection, UserSyncSetting
from app.services.player_memory_settings import (
    default_player_memory_settings,
    get_masked_player_memory_settings,
    get_player_memory_settings,
    upsert_player_memory_settings,
)
from app.services.destination_sheet import DestinationSheetService
from app.services.sql_player_sync import sync_sql_context_for_workspace
from app.services.workspace_enqueue import (
    enqueue_feedback_delegate_job,
    enqueue_sql_player_sync_single_job,
    enqueue_video_processing_stub_job,
    enqueue_workspace_context_refresh,
)
from app.services.workspace_service import ensure_workspace

router = APIRouter(prefix="/admin-api", tags=["admin"])


class PlayerMemorySettingsUpdateBody(BaseModel):
    vector_backend: str = Field(default="pinecone")
    sql_database_url: str = ""
    sql_query: str = ""
    watermark_column: str = ""
    table_names: list[str] = Field(default_factory=lambda: ["profile", "video", "evaluation", "club"])
    player_key_columns: str = "player_id"
    pinecone_api_key: str = ""
    pinecone_index: str = ""
    pinecone_namespace: str = "player-memory"
    top_k: int = Field(default=12, ge=1, le=50)
    context_max_chars: int = Field(default=12000, ge=500, le=200000)
    chunk_max_tokens: int = Field(default=650, ge=100, le=4000)
    chunk_overlap_ratio: float = Field(default=0.12, ge=0.0, le=0.5)


class PlayerMemorySingleSyncBody(BaseModel):
    player_id: int = Field(..., ge=1)
    first_name: str = Field(..., min_length=1, max_length=200)
    last_name: str = Field(..., min_length=1, max_length=200)


def _ensure_read_only_select_sql(sql_query: str) -> None:
    q = (sql_query or "").strip().lower()
    if not q:
        return
    if ";" in q:
        raise ValueError("Only a single SELECT query is allowed.")
    if not q.startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")
    blocked = ("insert ", "update ", "delete ", "drop ", "alter ", "create ", "truncate ")
    if any(token in q for token in blocked):
        raise ValueError("Query contains disallowed SQL keywords.")


def _normalize_settings_input(payload: PlayerMemorySettingsUpdateBody) -> dict:
    data = payload.model_dump()
    data.pop("destination_sheet_name", None)
    data["vector_backend"] = str(data.get("vector_backend") or "pinecone").strip().lower()
    if data["vector_backend"] not in {"pinecone", "pgvector"}:
        raise ValueError("vector_backend must be pinecone or pgvector.")
    _ensure_read_only_select_sql(str(data.get("sql_query") or ""))
    cleaned_tables = []
    for name in data.get("table_names") or []:
        t = str(name).strip().lower()
        if t and re.fullmatch(r"[a-z0-9_\.]+", t):
            cleaned_tables.append(t)
    data["table_names"] = cleaned_tables or ["profile", "video", "evaluation", "club"]
    return data


def _looks_masked_secret(value: str) -> bool:
    v = (value or "").strip()
    if not v:
        return False
    return "..." in v or set(v) == {"*"}


def _merge_masked_secrets_with_existing(db: Session, incoming: dict) -> dict:
    current = get_player_memory_settings(db)
    merged = dict(incoming)
    for key in ("sql_database_url", "pinecone_api_key"):
        raw = str(merged.get(key) or "")
        if _looks_masked_secret(raw):
            merged[key] = str(current.get(key) or "")
    return merged


def _normalize_external_db_url(raw_db_url: str) -> str:
    url = (raw_db_url or "").strip()
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    if url.startswith("mysql://"):
        return url.replace("mysql://", "mysql+pymysql://", 1)
    return url


def _preview_sql_bind_params(sql_query: str) -> dict[str, Any]:
    """Supply binds for Test SQL connection when query uses :first_name / :last_name / :last_watermark."""
    sql = sql_query or ""
    params: dict[str, Any] = {}
    if ":first_name" in sql:
        params["first_name"] = "Athlete"
    if ":last_name" in sql:
        params["last_name"] = "Focus"
    if ":last_watermark" in sql:
        params["last_watermark"] = "1970-01-01 00:00:00"
    return params


def _sql_append_limit(sql_query: str, limit: int = 5) -> str:
    """Append LIMIT for preview runs. Avoids SELECT * FROM (subquery), which still fails when subquery has duplicate column names."""
    s = (sql_query or "").strip().rstrip(";").strip()
    if not s:
        return s
    if re.search(r"\blimit\s+\d+\s*$", s, flags=re.I):
        return s
    lim = max(1, int(limit))
    return f"{s} LIMIT {lim}"


def _sql_cell_for_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return {"_type": "bytes", "length": len(value)}
    return value


def _preview_row_full(cols: list[str], row: tuple) -> dict[str, Any]:
    return {
        str(cols[i]): _sql_cell_for_json(row[i]) for i in range(min(len(cols), len(row)))
    }


@router.get("/player-memory/settings")
async def admin_get_player_memory_settings(
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    return {
        "settings": get_masked_player_memory_settings(db),
        "defaults": default_player_memory_settings(),
    }


@router.put("/player-memory/settings")
async def admin_update_player_memory_settings(
    body: PlayerMemorySettingsUpdateBody,
    admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    try:
        normalized = _normalize_settings_input(body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    normalized = _merge_masked_secrets_with_existing(db, normalized)
    updated = upsert_player_memory_settings(
        db,
        settings=normalized,
        updated_by=str(admin.get("email") or "admin"),
    )
    masked = dict(updated)
    masked["sql_database_url"] = get_masked_player_memory_settings(db).get("sql_database_url", "")
    masked["pinecone_api_key"] = get_masked_player_memory_settings(db).get("pinecone_api_key", "")
    return {"ok": True, "settings": masked}


@router.post("/player-memory/test-connection")
async def admin_test_player_memory_connection(
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
    preview_limit: int = Query(default=5, ge=1, le=100),
) -> dict:
    cfg = get_player_memory_settings(db)
    raw_db_url = str(cfg.get("sql_database_url") or "").strip()
    sql_query = str(cfg.get("sql_query") or "").strip()
    if not raw_db_url or not sql_query:
        return {"ok": False, "reason": "missing_sql_config"}
    try:
        _ensure_read_only_select_sql(sql_query)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    url = _normalize_external_db_url(raw_db_url)
    eng = create_engine(url, pool_pre_ping=True)
    preview_rows: list[dict] = []
    preview_params = _preview_sql_bind_params(sql_query)
    stmt = _sql_append_limit(sql_query, preview_limit)
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
        try:
            result = conn.execute(text(stmt), preview_params)
        except OperationalError as exc:
            msg = str(getattr(exc, "orig", None) or exc).lower()
            if "1060" in msg or "duplicate column" in msg:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "MySQL duplicate output column names (e.g. id from both e.* and v.*). "
                        "Use explicit aliases or JSON_OBJECT per table instead of e.* + v.*."
                    ),
                ) from exc
            raise
        cols = list(result.keys())
        for row in result.fetchall():
            preview_rows.append(_preview_row_full(cols, row))
    log_info(
        "player_memory_test_sql",
        preview_sql_ran=stmt,
        preview_bind_params_used=preview_params or None,
        preview_columns=cols,
        preview_row_count=len(preview_rows),
        preview_rows=preview_rows,
    )
    return {
        "ok": True,
        "preview_sql_ran": stmt,
        "preview_columns": cols,
        "preview_rows": preview_rows,
        "preview_bind_params_used": preview_params or None,
        "preview_limit": preview_limit,
        "preview_note": (
            "Do not use e.* and v.* together — duplicate id breaks MySQL. "
            f"Returned up to {preview_limit} rows; see server logs (player_memory_test_sql)."
        ),
    }


@router.post("/player-memory/test-pinecone")
async def admin_test_pinecone_connection(
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    cfg = get_player_memory_settings(db)
    api_key = str(cfg.get("pinecone_api_key") or "").strip()
    index_name = str(cfg.get("pinecone_index") or "").strip()
    namespace = str(cfg.get("pinecone_namespace") or "player-memory").strip()
    if not namespace or namespace == "__default__":
        namespace = "player-memory"
    if not api_key or not index_name:
        return {"ok": False, "reason": "missing_pinecone_config"}
    client = Pinecone(api_key=api_key)
    index = client.Index(index_name)
    stats = index.describe_index_stats()
    return {
        "ok": True,
        "index": index_name,
        "namespace": namespace,
        "stats": stats if isinstance(stats, dict) else getattr(stats, "to_dict", lambda: {})(),
    }


def _normalize_external_profile_row(row: dict[str, Any]) -> dict[str, Any]:
    """MySQL drivers may return mixed-case keys; UI needs stable snake_case strings."""
    d = {str(k).lower(): v for k, v in row.items()}
    pid = d.get("player_id")
    try:
        pid_int = int(pid) if pid is not None else 0
    except (TypeError, ValueError):
        pid_int = 0
    return {
        "player_id": pid_int,
        "first_name": str(d.get("first_name") or ""),
        "last_name": str(d.get("last_name") or ""),
        "email": str(d.get("email") or ""),
    }


@router.get("/player-memory/external-profiles")
async def admin_list_external_profiles(
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
    q: str = Query(default="", max_length=200),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0, le=500_000),
) -> dict[str, Any]:
    cfg = get_player_memory_settings(db)
    raw_db_url = str(cfg.get("sql_database_url") or "").strip()
    if not raw_db_url:
        raise HTTPException(status_code=400, detail="sql_database_url not configured")
    url = _normalize_external_db_url(raw_db_url)
    eng = create_engine(url, pool_pre_ping=True)
    search = q.strip()
    profiles: list[dict[str, Any]] = []
    with eng.connect() as conn:
        if search:
            pat = f"%{search.lower()}%"
            stmt = text(
                """
                SELECT id AS player_id, first_name, last_name, email
                FROM sportal.profile
                WHERE LOWER(CONCAT(
                    COALESCE(first_name, ''), ' ',
                    COALESCE(last_name, ''), ' ',
                    COALESCE(email, '')
                )) LIKE :pat
                ORDER BY id DESC
                LIMIT :lim OFFSET :off
                """
            )
            res = conn.execute(stmt, {"pat": pat, "lim": limit, "off": offset})
        else:
            stmt = text(
                """
                SELECT id AS player_id, first_name, last_name, email
                FROM sportal.profile
                ORDER BY id DESC
                LIMIT :lim OFFSET :off
                """
            )
            res = conn.execute(stmt, {"lim": limit, "off": offset})
        for row in res.mappings():
            profiles.append(_normalize_external_profile_row(dict(row)))
    return {"ok": True, "profiles": profiles, "limit": limit, "offset": offset}


@router.post("/player-memory/sync-sql-player")
async def admin_enqueue_player_sql_sync(
    body: PlayerMemorySingleSyncBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    ws = ensure_workspace(user_id="0", db=db)
    fn = body.first_name.strip()
    ln = body.last_name.strip()
    ok = enqueue_sql_player_sync_single_job(ws.id, body.player_id, fn, ln)
    if not ok:
        raise HTTPException(status_code=503, detail="Could not enqueue SQL sync job.")
    return {"queued": True, "workspace_id": ws.id, "player_id": body.player_id}


@router.post("/player-memory/run-sql-sync-player")
async def admin_run_player_sql_sync_inline(
    body: PlayerMemorySingleSyncBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    ws = ensure_workspace(user_id="0", db=db)
    fn = body.first_name.strip()
    ln = body.last_name.strip()
    result = sync_sql_context_for_workspace(
        db=db,
        workspace_id=ws.id,
        single_player={"player_id": body.player_id, "first_name": fn, "last_name": ln},
    )
    return dict(result)


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


# --- Agent Lab (admin): destination sheet tabs/rows + feedback jobs for any workspace user ---


class AdminAgentsFeedbackReviewBody(BaseModel):
    user_id: str = Field(..., min_length=1)
    video_url: str = Field(default="", max_length=8000)
    text_only: bool = Field(default=False)
    player_focus: str = ""
    sport: str = "Soccer"
    analysis_scope: str = ""
    coaching_focus: str = ""
    player_key: str = ""
    first_name: str = ""
    last_name: str = ""


class AdminAgentsUserIdBody(BaseModel):
    user_id: str = Field(..., min_length=1)


def _admin_require_user(db: Session, user_id: str) -> UserAccount:
    try:
        uid = int(str(user_id).strip())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid user_id.") from exc
    user = db.get(UserAccount, uid)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    return user


def _infer_user_id_for_destination_tab(db: Session, sheet_title: str) -> str | None:
    title = (sheet_title or "").strip()
    if not title:
        return None
    dest = DestinationSheetService()
    if not dest.is_enabled():
        return None
    for user in db.query(UserAccount).filter(UserAccount.is_active.is_(True)).all():
        if dest.user_sheet_name(user.email) == title:
            return str(user.id)
    return None


@router.get("/agents-lab/destination-tabs")
async def agents_lab_destination_tabs(
    _admin: dict = Depends(get_admin_session_context),
) -> dict[str, Any]:
    dest = DestinationSheetService()
    if not dest.is_enabled():
        return {"enabled": False, "tabs": []}
    try:
        tabs = dest.list_sheet_titles()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Could not list destination tabs: {exc}") from exc
    return {"enabled": True, "tabs": tabs}


@router.get("/agents-lab/destination-rows")
async def agents_lab_destination_rows(
    sheet_title: str = Query(..., min_length=1),
    _admin: dict = Depends(get_admin_session_context),
) -> dict[str, Any]:
    dest = DestinationSheetService()
    if not dest.is_enabled():
        raise HTTPException(status_code=503, detail="Destination sheet is not configured.")
    name = sheet_title.strip()
    try:
        headers, rows = dest.load_headers_and_rows(sheet_name=name, ensure_sheet=False)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    max_rows = int(os.getenv("WORKSPACE_SNAPSHOT_MAX_ROWS", "500"))
    trimmed = rows[:max_rows] if rows else []
    return {
        "sheet_title": name,
        "headers": headers,
        "rows": trimmed,
        "row_count": len(rows) if rows else 0,
    }


@router.get("/agents-lab/infer-user-from-tab")
async def agents_lab_infer_user_from_tab(
    sheet_title: str = Query(..., min_length=1),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    uid = _infer_user_id_for_destination_tab(db, sheet_title)
    return {"user_id": uid}


@router.get("/agents-lab/workspace")
async def agents_lab_workspace(
    user_id: str = Query(..., min_length=1),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _admin_require_user(db, user_id)
    ws = db.query(Workspace).filter(Workspace.user_id == user_id).one_or_none()
    if ws is None:
        return {"workspace": None, "context_items": [], "agent_jobs": []}
    items = (
        db.query(WorkspaceContextItem)
        .filter(WorkspaceContextItem.workspace_id == ws.id)
        .order_by(WorkspaceContextItem.created_at.desc())
        .limit(20)
        .all()
    )
    jobs = (
        db.query(AgentJob)
        .filter(AgentJob.workspace_id == ws.id)
        .order_by(AgentJob.created_at.desc())
        .limit(20)
        .all()
    )
    return {
        "workspace": {"id": ws.id, "user_id": ws.user_id, "updated_at": ws.updated_at.isoformat() if ws.updated_at else None},
        "context_items": [
            {
                "id": i.id,
                "item_type": i.item_type,
                "content_hash": i.content_hash[:16] + "...",
                "created_at": i.created_at.isoformat() if i.created_at else None,
            }
            for i in items
        ],
        "agent_jobs": [
            {
                "id": j.id,
                "agent_type": j.agent_type,
                "status": j.status,
                "external_ref": j.external_ref,
                "created_at": j.created_at.isoformat() if j.created_at else None,
                "completed_at": j.completed_at.isoformat() if j.completed_at else None,
                "error_message": j.error_message,
            }
            for j in jobs
        ],
    }


@router.get("/agents-lab/workspace/context/{item_id}")
async def agents_lab_workspace_context(
    item_id: int,
    user_id: str = Query(..., min_length=1),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _admin_require_user(db, user_id)
    ws = db.query(Workspace).filter(Workspace.user_id == user_id).one_or_none()
    if ws is None:
        raise HTTPException(status_code=404, detail="Workspace not found.")
    item = db.get(WorkspaceContextItem, item_id)
    if item is None or item.workspace_id != ws.id:
        raise HTTPException(status_code=404, detail="Context item not found.")
    return {"id": item.id, "item_type": item.item_type, "payload": json.loads(item.payload_json)}


@router.get("/agents-lab/jobs/{job_id}")
async def agents_lab_job(
    job_id: int,
    user_id: str = Query(..., min_length=1),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _admin_require_user(db, user_id)
    ws = db.query(Workspace).filter(Workspace.user_id == user_id).one_or_none()
    if ws is None:
        raise HTTPException(status_code=404, detail="Workspace not found.")
    job = db.get(AgentJob, job_id)
    if job is None or job.workspace_id != ws.id:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {
        "id": job.id,
        "agent_type": job.agent_type,
        "status": job.status,
        "payload": json.loads(job.payload_json) if job.payload_json else None,
        "result": json.loads(job.result_json) if job.result_json else None,
        "error_message": job.error_message,
        "external_ref": job.external_ref,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }


@router.post("/agents-lab/workspace-refresh")
async def agents_lab_workspace_refresh(
    body: AdminAgentsUserIdBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _admin_require_user(db, body.user_id)
    ok = enqueue_workspace_context_refresh(body.user_id.strip())
    if not ok:
        raise HTTPException(status_code=503, detail="Could not enqueue workspace refresh.")
    return {"success": True, "queued": True}


@router.post("/agents-lab/feedback-reviews")
async def agents_lab_create_feedback_review(
    body: AdminAgentsFeedbackReviewBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    uid = body.user_id.strip()
    _admin_require_user(db, uid)
    ws = ensure_workspace(user_id=uid, db=db)
    job = AgentJob(
        workspace_id=ws.id,
        agent_type="FEEDBACK_DELEGATE",
        status="PENDING",
        payload_json=json.dumps(
            {
                "video_url": body.video_url.strip(),
                "text_only": bool(body.text_only),
                "player_focus": (body.player_focus or "").strip(),
                "sport": (body.sport or "Soccer").strip(),
                "analysis_scope": (body.analysis_scope or "").strip(),
                "coaching_focus": (body.coaching_focus or "").strip(),
                "player_key": (body.player_key or "").strip(),
                "first_name": (body.first_name or "").strip(),
                "last_name": (body.last_name or "").strip(),
            },
            ensure_ascii=True,
        ),
        created_at=datetime.now(UTC),
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    ok = enqueue_feedback_delegate_job(job.id)
    if not ok:
        job.status = "FAILED"
        job.error_message = "Failed to enqueue feedback job."
        job.completed_at = datetime.now(UTC)
        db.commit()
        raise HTTPException(status_code=503, detail="Could not enqueue feedback job.")
    return {"success": True, "agent_job_id": job.id}


@router.post("/agents-lab/video-process-stub")
async def agents_lab_video_stub(
    body: AdminAgentsUserIdBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    uid = body.user_id.strip()
    _admin_require_user(db, uid)
    ws = ensure_workspace(user_id=uid, db=db)
    job = AgentJob(
        workspace_id=ws.id,
        agent_type="VIDEO_PROCESSING",
        status="PENDING",
        payload_json=json.dumps({"note": "stub"}, ensure_ascii=True),
        created_at=datetime.now(UTC),
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    ok = enqueue_video_processing_stub_job(job.id)
    if not ok:
        job.status = "FAILED"
        job.error_message = "Failed to enqueue video job."
        job.completed_at = datetime.now(UTC)
        db.commit()
        raise HTTPException(status_code=503, detail="Could not enqueue video job.")
    return {"success": True, "agent_job_id": job.id}
