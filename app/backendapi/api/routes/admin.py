from __future__ import annotations

import json
import os
import re
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.engine import Engine
from google.api_core.exceptions import DeadlineExceeded
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from backendapi.core.logger import info as log_info
from backendapi.db import get_db
from backendapi.services.external_sql_engine import get_external_sql_engine
from backendapi.dependencies.auth import get_admin_session_context
from backendapi.models.auth import UserAccount
from backendapi.models.workspace import AgentJob, Workspace, WorkspaceContextItem
from backendapi.models.google_oauth import GoogleOAuthToken, SheetSyncEvent, SheetSyncRun, UserGoogleSheetSelection, UserSyncSetting
from backendapi.services.context_scope import CONTEXT_SCOPE_PERSONAL, CONTEXT_SCOPE_SHARED, SHARED_PLAYER_KEY
from backendapi.services.player_memory_settings import (
    default_player_memory_settings,
    effective_player_context_sql,
    get_masked_player_memory_settings,
    get_player_memory_settings,
    player_memory_settings_decrypt_failed,
    sql_has_single_player_bind,
    upsert_player_memory_settings,
)
from backendapi.services.destination_sheet import DestinationSheetService
from backendapi.services.player_memory_service import (
    chunk_stats_by_scope,
    delete_chunk_by_id,
    insert_chunks,
    list_chunks,
    list_personal_player_keys,
    update_chunk_by_id,
)
from backendapi.services.shared_context_schema import format_shared_context_embed_text
from backendapi.services.shared_context_embed import sync_shared_context_from_sheet
from backendapi.services.sql_player_sync import sync_sql_context_for_workspace
from backendapi.services.workspace_enqueue import (
    enqueue_feedback_delegate_job,
    enqueue_sql_player_sync_single_job,
    enqueue_video_processing_stub_job,
    enqueue_workspace_context_refresh,
)
from backendapi.services.workspace_service import ensure_workspace
from backendapi.services.agent_job_cancel import (
    merge_agent_job_result_json,
    request_feedback_agent_cancel,
    set_agent_job_cancel_requested,
)
from backendapi.services.sync_queue import get_redis
from backendapi.workers.workspace_worker import feedback_agent_poll_progress_hint

router = APIRouter(prefix="/admin-api", tags=["admin"])


def _firestore_admin_error_hint(exc: Exception) -> str:
    msg = str(exc).lower()
    if "file" in msg and "was not found" in msg:
        return (
            "Firestore auth misconfigured: empty GOOGLE_APPLICATION_CREDENTIALS breaks ADC. "
            "On the VM, leave that var unset, redeploy api/worker, and grant the VM service account "
            "roles/datastore.user on GCP_PROJECT_ID / GCP_FIRESTORE_DATABASE."
        )
    if "default credentials" in msg or "could not automatically determine credentials" in msg:
        return (
            "No GCP credentials: attach a service account to the VM with roles/datastore.user, "
            "or set a valid GOOGLE_APPLICATION_CREDENTIALS path for local dev only."
        )
    return (
        "Check GCP_PROJECT_ID, GCP_FIRESTORE_DATABASE, and VM service account Firestore access."
    )


class PlayerMemorySettingsUpdateBody(BaseModel):
    vector_backend: str = Field(default="firestore")
    gcp_project_id: str = ""
    gcp_firestore_database: str = "(default)"
    vector_collection_personal: str = "player_personal_context"
    vector_collection_shared: str = "shared_context"
    sql_database_url: str = ""
    sql_query: str = ""
    watermark_column: str = ""
    table_names: list[str] = Field(default_factory=lambda: ["profile", "video", "evaluation", "club"])
    player_key_columns: str = "player_user_id,reviewee_id,player_id"
    top_k: int = Field(default=12, ge=1, le=50)
    shared_top_k: int = Field(default=6, ge=1, le=50)
    context_max_chars: int = Field(default=12000, ge=500, le=200000)
    shared_context_max_chars: int = Field(default=8000, ge=500, le=200000)
    chunk_max_tokens: int = Field(default=650, ge=100, le=4000)
    chunk_overlap_ratio: float = Field(default=0.12, ge=0.0, le=0.5)


class PlayerMemorySingleSyncBody(BaseModel):
    player_id: int = Field(..., ge=1)
    first_name: str = Field(default="", max_length=200)
    last_name: str = Field(default="", max_length=200)


class PlayerMemoryManualBody(BaseModel):
    player_key: str = Field(..., min_length=1)
    text: str = Field(..., min_length=1)
    label: str = Field(default="", max_length=256)


class VectorRetrievalTestBody(BaseModel):
    player_key: str = Field(..., min_length=1)
    query: str = Field(
        default="soccer player coaching technical skills development",
        max_length=2000,
    )


class SharedContextManualBody(BaseModel):
    text: str = Field(..., min_length=1)
    label: str = Field(default="", max_length=256)


class ChunkUpdateBody(BaseModel):
    content: str = Field(..., min_length=1, max_length=50_000)
    label: str = Field(default="", max_length=256)
    position: str = Field(default="", max_length=500)
    category: str = Field(default="", max_length=500)
    part_of_the_field: str = Field(default="", max_length=500)
    title: str = Field(default="", max_length=500)
    description: str = Field(default="", max_length=10_000)


class SqlPlayerPreviewBody(BaseModel):
    player_user_id: int = Field(..., ge=1)
    preview_limit: int = Field(default=5, ge=1, le=100)


class SqlConnectionTestBody(BaseModel):
    """Optional probe query; default checks Sportal profile table."""
    probe_sql: str = Field(default="SELECT * FROM profile")


def _strip_leading_sql_comments(sql_query: str) -> str:
    """Remove leading -- and /* */ comments so validation sees the first statement."""
    s = (sql_query or "").strip()
    while s:
        if s.startswith("--"):
            nl = s.find("\n")
            if nl < 0:
                return ""
            s = s[nl + 1 :].lstrip()
            continue
        if s.startswith("/*"):
            end = s.find("*/")
            if end < 0:
                break
            s = s[end + 2 :].lstrip()
            continue
        break
    return s


def _normalize_sql_for_validation(sql_query: str) -> str:
    """Strip comments and trailing semicolon so pasted SQL from editors/clients still validates."""
    return _strip_leading_sql_comments(sql_query).strip().rstrip(";").strip()


def _ensure_read_only_select_sql(sql_query: str) -> None:
    q = _normalize_sql_for_validation(sql_query).lower()
    if not q:
        return
    if ";" in q:
        raise ValueError("Only a single SELECT query is allowed.")
    if not q.startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")
    blocked = ("insert ", "update ", "delete ", "drop ", "alter ", "create ", "truncate ")
    if any(re.search(rf"\b{token.strip()}\b", q) for token in blocked):
        raise ValueError("Query contains disallowed SQL keywords.")


def _sql_has_single_player_bind(sql_query: str) -> bool:
    return sql_has_single_player_bind(sql_query)


def _ensure_single_player_sql_binds(sql_query: str) -> None:
    if not (sql_query or "").strip():
        return
    if _sql_has_single_player_bind(sql_query):
        return
    raise ValueError(
        "SQL must include :player_user_id (recommended) or both :first_name and :last_name "
        "for per-player sync. In the FROM clause use: SELECT :player_user_id AS player_user_id"
    )


def _normalize_settings_input(payload: PlayerMemorySettingsUpdateBody) -> dict:
    # Only fields sent in the request body — avoid resetting GCP/Firestore to model defaults.
    data = payload.model_dump(exclude_unset=True)
    data.pop("destination_sheet_name", None)
    if "vector_backend" in data:
        data["vector_backend"] = str(data.get("vector_backend") or "firestore").strip().lower()
        if data["vector_backend"] not in {"firestore", "gcp"}:
            raise ValueError("vector_backend must be firestore (native GCP Firestore vector search).")
    if "sql_query" in data:
        _ensure_read_only_select_sql(str(data.get("sql_query") or ""))
        _ensure_single_player_sql_binds(str(data.get("sql_query") or ""))
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
    for key in ("sql_database_url",):
        raw = str(merged.get(key) or "")
        if _looks_masked_secret(raw):
            merged[key] = str(current.get(key) or "")
    return merged


def _sportal_sql_engine(db: Session) -> tuple[Engine | None, str]:
    """Shared pooled Sportal MySQL engine (avoids per-request engines exhausting max_connections)."""
    cfg = get_player_memory_settings(db)
    raw_db_url = str(cfg.get("sql_database_url") or "").strip()
    if not raw_db_url:
        return None, ""
    return get_external_sql_engine(raw_db_url), raw_db_url


def _raise_sportal_db_http_error(exc: Exception) -> None:
    detail = str(exc)
    if "1040" in detail or "Too many connections" in detail:
        raise HTTPException(
            status_code=503,
            detail=(
                "Sportal MySQL refused the connection (too many connections). "
                "Wait a minute and retry. If this persists, close idle DB clients or raise "
                "max_connections on the Sportal server."
            ),
        ) from exc
    raise HTTPException(status_code=400, detail=detail) from exc


def _preview_sql_bind_params(sql_query: str) -> dict[str, Any]:
    """Supply binds for Test SQL connection when query uses :first_name / :last_name / :last_watermark."""
    sql = sql_query or ""
    params: dict[str, Any] = {}
    if ":first_name" in sql:
        params["first_name"] = "Athlete"
    if ":last_name" in sql:
        params["last_name"] = "Focus"
    if ":player_user_id" in sql:
        params["player_user_id"] = 28
    if ":last_watermark" in sql:
        params["last_watermark"] = "1970-01-01 00:00:00"
    return params


def _sql_append_limit(sql_query: str, limit: int = 5) -> str:
    """Append LIMIT for preview runs. Avoids SELECT * FROM (subquery), which still fails when subquery has duplicate column names."""
    s = _normalize_sql_for_validation(sql_query)
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
    decrypt_failed = player_memory_settings_decrypt_failed(db)
    settings = get_masked_player_memory_settings(db)
    sql_query = str(settings.get("sql_query") or "")
    return {
        "settings": settings,
        "defaults": default_player_memory_settings(),
        "decrypt_failed": decrypt_failed,
        "sql_player_bind_ok": _sql_has_single_player_bind(sql_query),
        "decrypt_hint": (
            "Saved admin settings could not be decrypted with PLAYER_MEMORY_SETTINGS_MASTER_KEY. "
            "Env defaults are active — open SQL & settings, verify values, and click Save to re-encrypt."
            if decrypt_failed
            else None
        ),
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
    return {"ok": True, "settings": masked}


@router.post("/player-memory/test-connection")
async def admin_test_player_memory_connection(
    body: SqlConnectionTestBody | None = None,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
    preview_limit: int = Query(default=5, ge=1, le=100),
) -> dict:
    cfg = get_player_memory_settings(db)
    raw_db_url = str(cfg.get("sql_database_url") or "").strip()
    if not raw_db_url:
        return {"ok": False, "reason": "missing_sql_database_url"}
    probe_sql = (body.probe_sql if body else "SELECT * FROM profile").strip()
    try:
        _ensure_read_only_select_sql(probe_sql)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    eng = get_external_sql_engine(raw_db_url)
    if eng is None:
        return {"ok": False, "reason": "invalid_sql_database_url"}
    preview_rows: list[dict] = []
    stmt = _sql_append_limit(probe_sql, preview_limit)
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
        try:
            result = conn.execute(text(stmt))
        except OperationalError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        cols = list(result.keys())
        for row in result.fetchall():
            preview_rows.append(_preview_row_full(cols, row))
    log_info(
        "player_memory_test_sql",
        probe_sql=stmt,
        preview_columns=cols,
        preview_row_count=len(preview_rows),
    )
    return {
        "ok": True,
        "preview_sql_ran": stmt,
        "preview_columns": cols,
        "preview_rows": preview_rows,
        "preview_limit": preview_limit,
        "preview_note": f"Connection OK. Probe query returned up to {preview_limit} row(s) from profile.",
    }


@router.post("/player-memory/test-vector")
async def admin_test_vector_connection(
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    from backendapi.services.vector_store import get_vector_store

    cfg = get_player_memory_settings(db)
    backend = str(cfg.get("vector_backend") or "firestore")
    try:
        store = get_vector_store(db)
        health = store.health_check()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "backend": backend, "reason": str(exc)}
    stats = chunk_stats_by_scope(db=db, workspace_id=ensure_workspace(user_id="0", db=db).id)
    return {"ok": True, "backend": backend, "health": health, "stats": stats}


@router.post("/player-memory/test-retrieval")
async def admin_test_vector_retrieval(
    body: VectorRetrievalTestBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Test Firestore list + vector search for one player_key (debug coaching RAG)."""
    from backendapi.services.feedback_memory import retrieve_player_memory_context

    ws = ensure_workspace(user_id="0", db=db)
    pk = body.player_key.strip()
    payload = {
        "coaching_focus": body.query.strip(),
        "player_focus": body.query.strip(),
        "sport": "Soccer",
    }
    mem, debug = retrieve_player_memory_context(
        db=db,
        workspace_id=ws.id,
        player_key=pk,
        payload=payload,
    )
    try:
        stored, total = list_chunks(
            db=db,
            workspace_id=ws.id,
            context_scope=CONTEXT_SCOPE_PERSONAL,
            player_key=pk,
            limit=5,
            offset=0,
        )
    except Exception as exc:  # noqa: BLE001
        stored, total = [], 0
        debug["list_chunks_error"] = str(exc)
    return {
        "ok": bool(mem) or total > 0,
        "workspace_id": ws.id,
        "player_key": pk,
        "stored_chunk_total": total,
        "retrieval_outcome": debug.get("outcome"),
        "retrieval_debug": debug,
        "context_chars": len(mem or ""),
        "context_preview": (mem or "")[:1500] if mem else None,
        "sample_stored_chunks": [
            {
                "source_type": ch.get("source_type"),
                "source_ref": ch.get("source_ref"),
                "content_preview": str(ch.get("content") or "")[:240],
            }
            for ch in stored[:3]
        ],
    }


@router.post("/player-memory/test-sql-player")
async def admin_test_sql_for_player(
    body: SqlPlayerPreviewBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    cfg = get_player_memory_settings(db)
    raw_db_url = str(cfg.get("sql_database_url") or "").strip()
    sql_query = effective_player_context_sql(cfg)
    if not raw_db_url or not sql_query:
        return {"ok": False, "reason": "missing_sql_config"}
    if ":player_user_id" not in sql_query:
        return {"ok": False, "reason": "sql_missing_player_user_id_bind"}
    try:
        _ensure_read_only_select_sql(sql_query)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    eng = get_external_sql_engine(raw_db_url)
    if eng is None:
        return {"ok": False, "reason": "invalid_sql_database_url"}
    params = {"player_user_id": body.player_user_id}
    stmt = _sql_append_limit(sql_query, body.preview_limit)
    preview_rows: list[dict] = []
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
        result = conn.execute(text(stmt), params)
        cols = list(result.keys())
        for row in result.fetchall():
            preview_rows.append(_preview_row_full(cols, row))
    return {
        "ok": True,
        "player_user_id": body.player_user_id,
        "preview_sql_ran": stmt,
        "preview_columns": cols,
        "preview_rows": preview_rows,
        "preview_limit": body.preview_limit,
    }


@router.get("/player-memory/chunks")
async def admin_list_memory_chunks(
    context_scope: str = Query(default=CONTEXT_SCOPE_PERSONAL),
    player_key: str = Query(default=""),
    source_type: str = Query(default=""),
    search: str = Query(default="", max_length=200),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    ws = ensure_workspace(user_id="0", db=db)
    scope = context_scope.strip().lower()
    if scope not in {CONTEXT_SCOPE_PERSONAL, CONTEXT_SCOPE_SHARED}:
        raise HTTPException(status_code=400, detail="context_scope must be personal or shared")
    try:
        if scope == CONTEXT_SCOPE_PERSONAL and search.strip():
            from backendapi.services.personal_context_store import list_chunks_with_search

            chunks, total = list_chunks_with_search(
                db=db,
                workspace_id=ws.id,
                player_key=player_key.strip() or None,
                source_type=source_type.strip() or None,
                search=search.strip(),
                limit=limit,
                offset=offset,
            )
        else:
            chunks, total = list_chunks(
                db=db,
                workspace_id=ws.id,
                context_scope=scope,
                player_key=player_key.strip() or None,
                source_type=source_type.strip() or None,
                limit=limit,
                offset=offset,
            )
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": str(exc),
            "hint": _firestore_admin_error_hint(exc),
            "chunks": [],
            "total": 0,
            "limit": limit,
            "offset": offset,
        }
    return {"ok": True, "chunks": chunks, "total": total, "limit": limit, "offset": offset}


@router.patch("/player-memory/chunks/{chunk_id}")
async def admin_update_memory_chunk(
    chunk_id: str,
    body: ChunkUpdateBody,
    context_scope: str = Query(default=CONTEXT_SCOPE_PERSONAL),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    ws = ensure_workspace(user_id="0", db=db)
    scope = context_scope.strip().lower()
    if scope not in {CONTEXT_SCOPE_PERSONAL, CONTEXT_SCOPE_SHARED}:
        raise HTTPException(status_code=400, detail="context_scope must be personal or shared")

    meta_patch: dict[str, Any] = {"origin": "admin_edit"}
    if body.label.strip():
        meta_patch["label"] = body.label.strip()
    rubric_keys = ("position", "category", "part_of_the_field", "title", "description")
    for key in rubric_keys:
        val = str(getattr(body, key) or "").strip()
        if val:
            meta_patch[key] = val

    content = body.content.strip()
    if scope == CONTEXT_SCOPE_SHARED and any(meta_patch.get(k) for k in rubric_keys):
        record = {key: str(meta_patch.get(key) or "") for key in rubric_keys}
        if any(record.values()):
            content = format_shared_context_embed_text(record)

    # Promote edited chunks to manual so sheet/SQL resync does not overwrite admin edits.
    ok = update_chunk_by_id(
        db=db,
        workspace_id=ws.id,
        chunk_id=chunk_id.strip(),
        context_scope=scope,
        content=content,
        metadata=meta_patch,
        source_type="manual",
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Chunk not found")
    return {"ok": True, "updated_id": chunk_id, "content": content}


@router.delete("/player-memory/chunks/{chunk_id}")
async def admin_delete_memory_chunk(
    chunk_id: str,
    context_scope: str = Query(default=CONTEXT_SCOPE_PERSONAL),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    ws = ensure_workspace(user_id="0", db=db)
    ok = delete_chunk_by_id(
        db=db,
        workspace_id=ws.id,
        chunk_id=chunk_id.strip(),
        context_scope=context_scope,
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Chunk not found")
    return {"ok": True, "deleted_id": chunk_id}


@router.post("/player-memory/shared/sync-sheet")
async def admin_sync_shared_context_sheet(
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    ws = ensure_workspace(user_id="0", db=db)
    try:
        return sync_shared_context_from_sheet(db=db, workspace_id=ws.id)
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": str(exc),
            "hint": _firestore_admin_error_hint(exc),
        }


@router.post("/player-memory/shared/manual")
async def admin_add_shared_context_manual(
    body: SharedContextManualBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    ws = ensure_workspace(user_id="0", db=db)
    slug = (body.label or "note").strip().replace(" ", "_")[:64]
    ref = f"shared_manual:{slug}:{uuid.uuid4().hex[:10]}"
    meta = {
        "origin": "admin_manual",
        "label": body.label,
        "context_scope": CONTEXT_SCOPE_SHARED,
        "preserve_on_sync": True,
    }
    n = insert_chunks(
        db=db,
        workspace_id=ws.id,
        player_key=SHARED_PLAYER_KEY,
        source_type="manual",
        texts_with_refs=[(body.text.strip(), ref, meta)],
        context_scope=CONTEXT_SCOPE_SHARED,
    )
    return {"ok": True, "chunks_written": n}


@router.post("/player-memory/personal/manual")
async def admin_add_personal_context_manual(
    body: PlayerMemoryManualBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    ws = ensure_workspace(user_id="0", db=db)
    slug = (body.label or "note").strip().replace(" ", "_")[:64]
    ref = f"manual:{slug}:{uuid.uuid4().hex[:10]}"
    meta = {
        "origin": "admin_manual",
        "label": body.label,
        "context_scope": CONTEXT_SCOPE_PERSONAL,
        "preserve_on_sync": True,
    }
    n = insert_chunks(
        db=db,
        workspace_id=ws.id,
        player_key=body.player_key.strip(),
        source_type="manual",
        texts_with_refs=[(body.text.strip(), ref, meta)],
        context_scope=CONTEXT_SCOPE_PERSONAL,
    )
    return {"ok": True, "chunks_written": n}


@router.get("/player-memory/stats")
async def admin_player_memory_stats(
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict:
    ws = ensure_workspace(user_id="0", db=db)
    try:
        return chunk_stats_by_scope(db=db, workspace_id=ws.id)
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "backend": "firestore",
            "reason": str(exc),
            "hint": _firestore_admin_error_hint(exc),
            "total_chunks": 0,
            "collections": {},
        }


@router.get("/player-memory/personal/player-keys")
async def admin_list_personal_player_keys(
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
    q: str = Query(default="", max_length=200),
) -> dict[str, Any]:
    """Players that have personal vectors in Firestore (not Sportal SQL)."""
    ws = ensure_workspace(user_id="0", db=db)
    try:
        players = list_personal_player_keys(db=db, workspace_id=ws.id, search=q.strip())
        return {"ok": True, "players": players, "total": len(players)}
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": str(exc),
            "hint": _firestore_admin_error_hint(exc),
            "players": [],
            "total": 0,
        }


class PersonalPlayerLabelBody(BaseModel):
    display_name: str = Field(..., min_length=1, max_length=512)


class PersonalContextVideoItem(BaseModel):
    summary: str | None = None
    description: str | None = None


class PersonalContextFeedbackBody(BaseModel):
    notes: list[str] = Field(default_factory=list)
    video_annotations: list[str] = Field(default_factory=list)


class PersonalContextDocumentBody(BaseModel):
    player_user_id: int = Field(..., ge=1)
    player_name: str = Field(default="", max_length=512)
    club_name: str = Field(default="", max_length=512)
    profile_text: str = Field(default="", max_length=50_000)
    videos: list[PersonalContextVideoItem] = Field(default_factory=list)
    feedback: PersonalContextFeedbackBody = Field(default_factory=PersonalContextFeedbackBody)


@router.get("/player-memory/personal/records")
async def admin_list_personal_context_records(
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
    q: str = Query(default="", max_length=200),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """Merged SQL + admin overlay documents per player (search: ID, name, club)."""
    from backendapi.services.personal_context_store import list_personal_records

    ws = ensure_workspace(user_id="0", db=db)
    try:
        records, total = list_personal_records(
            db=db,
            workspace_id=ws.id,
            search=q.strip(),
            limit=limit,
            offset=offset,
        )
        return {"ok": True, "records": records, "total": total, "limit": limit, "offset": offset}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": str(exc), "records": [], "total": 0, "limit": limit, "offset": offset}


@router.get("/player-memory/personal/records/{player_user_id}/diagnostics")
async def admin_personal_context_diagnostics(
    player_user_id: int,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Optional debug endpoint — not used by the admin UI."""
    from backendapi.services.personal_context_store import personal_context_diagnostics

    ws = ensure_workspace(user_id="0", db=db)
    try:
        return personal_context_diagnostics(db, workspace_id=ws.id, player_user_id=player_user_id)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": str(exc), "player_user_id": player_user_id}


@router.get("/player-memory/personal/records/{player_user_id}")
async def admin_get_personal_context_record(
    player_user_id: int,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    from backendapi.services.personal_context_store import merged_document_for_player

    ws = ensure_workspace(user_id="0", db=db)
    doc = merged_document_for_player(db, workspace_id=ws.id, player_user_id=player_user_id)
    return {"ok": True, "record": doc}


@router.put("/player-memory/personal/records/{player_user_id}")
async def admin_upsert_personal_context_record(
    player_user_id: int,
    body: PersonalContextDocumentBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    if int(body.player_user_id) != int(player_user_id):
        raise HTTPException(status_code=400, detail="player_user_id in body must match URL")
    from backendapi.services.personal_context_store import upsert_personal_record

    ws = ensure_workspace(user_id="0", db=db)
    document = body.model_dump()
    return upsert_personal_record(db=db, workspace_id=ws.id, document=document)


@router.post("/player-memory/personal/records")
async def admin_create_personal_context_record(
    body: PersonalContextDocumentBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    from backendapi.services.personal_context_store import upsert_personal_record

    ws = ensure_workspace(user_id="0", db=db)
    return upsert_personal_record(db=db, workspace_id=ws.id, document=body.model_dump())


@router.delete("/player-memory/personal/records/{player_user_id}")
async def admin_delete_personal_context_record(
    player_user_id: int,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    from backendapi.services.personal_context_store import delete_personal_record

    ws = ensure_workspace(user_id="0", db=db)
    return delete_personal_record(db=db, workspace_id=ws.id, player_user_id=player_user_id)


@router.patch("/player-memory/personal/players/{player_key}")
async def admin_update_personal_player_label(
    player_key: str,
    body: PersonalPlayerLabelBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Update workspace player directory display name (Firestore filter labels)."""
    from backendapi.services.player_directory import upsert_player_directory_entry

    ws = ensure_workspace(user_id="0", db=db)
    pk = player_key.strip()
    if not pk:
        raise HTTPException(status_code=400, detail="player_key is required")
    parts = body.display_name.strip().split(None, 1)
    first = parts[0] if parts else pk
    last = parts[1] if len(parts) > 1 else ""
    upsert_player_directory_entry(
        db=db,
        workspace_id=ws.id,
        player_key=pk,
        first_name=first,
        last_name=last,
    )
    db.commit()
    return {"ok": True, "player_key": pk, "display_name": body.display_name.strip()}


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
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0, le=500_000),
) -> dict[str, Any]:
    cfg = get_player_memory_settings(db)
    raw_db_url = str(cfg.get("sql_database_url") or "").strip()
    if not raw_db_url:
        return {
            "ok": False,
            "reason": "MySQL URL (sql_database_url) is not configured",
            "hint": (
                "Set PLAYER_CONTEXT_DATABASE_URL in app/backendapi/.env "
                "(e.g. mysql+pymysql://user:pass@host:3306/sportal) and restart the API, "
                "or open SQL & settings, enter the MySQL URL, and click Save settings."
            ),
            "profiles": [],
            "limit": limit,
            "offset": offset,
        }
    eng = get_external_sql_engine(raw_db_url)
    if eng is None:
        return {"ok": False, "reason": "invalid_sql_database_url", "profiles": [], "limit": limit, "offset": offset}
    search = q.strip()
    profiles: list[dict[str, Any]] = []
    try:
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
                    COALESCE(email, ''), ' ',
                    CAST(id AS CHAR)
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
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": str(exc),
            "hint": (
                "PLAYER_CONTEXT_DATABASE_URL must reach your Sportal MySQL (Cloud SQL). "
                "localhost only works if MySQL is running locally or Cloud SQL Auth Proxy is up."
            ),
            "profiles": [],
            "limit": limit,
            "offset": offset,
        }
    return {"ok": True, "profiles": profiles, "limit": limit, "offset": offset}


def _lookup_sportal_profile_by_id(db: Session, player_id: int) -> dict[str, Any] | None:
    """Return Sportal profile row by primary key, or None if missing / SQL not configured."""
    eng, raw_db_url = _sportal_sql_engine(db)
    if not raw_db_url:
        raise HTTPException(
            status_code=400,
            detail="MySQL URL is not configured. Set it in SQL & settings before syncing players.",
        )
    if eng is None:
        raise HTTPException(status_code=400, detail="Invalid MySQL URL in player memory settings.")
    try:
        with eng.connect() as conn:
            res = conn.execute(
                text(
                    """
                SELECT id AS player_id, first_name, last_name, email
                FROM sportal.profile
                WHERE id = :pid
                LIMIT 1
                """
                ),
                {"pid": player_id},
            )
            row = res.mappings().first()
    except OperationalError as exc:
        _raise_sportal_db_http_error(exc)
    except Exception as exc:  # noqa: BLE001
        _raise_sportal_db_http_error(exc)
    if row is None:
        return None
    return _normalize_external_profile_row(dict(row))


@router.get("/player-memory/sportal-player/{player_id}")
async def admin_get_sportal_player(
    player_id: int,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Validate that a Sportal player ID exists before SQL sync."""
    if player_id < 1:
        raise HTTPException(status_code=400, detail="Player ID must be a positive integer.")
    profile = _lookup_sportal_profile_by_id(db, player_id)
    if profile is None:
        return {
            "ok": False,
            "reason": "player_not_found",
            "detail": f"Player {player_id} does not exist in Sportal.",
        }
    return {"ok": True, "profile": profile}


@router.post("/player-memory/sync-sql-player")
async def admin_enqueue_player_sql_sync(
    body: PlayerMemorySingleSyncBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    profile = _lookup_sportal_profile_by_id(db, body.player_id)
    if profile is None:
        raise HTTPException(
            status_code=404,
            detail=f"Player {body.player_id} does not exist in Sportal. Sync was not started.",
        )
    ws = ensure_workspace(user_id="0", db=db)
    fn = body.first_name.strip() or str(profile.get("first_name") or "")
    ln = body.last_name.strip() or str(profile.get("last_name") or "")
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
    profile = _lookup_sportal_profile_by_id(db, body.player_id)
    if profile is None:
        raise HTTPException(
            status_code=404,
            detail=f"Player {body.player_id} does not exist in Sportal. Sync was not started.",
        )
    ws = ensure_workspace(user_id="0", db=db)
    fn = body.first_name.strip() or str(profile.get("first_name") or "")
    ln = body.last_name.strip() or str(profile.get("last_name") or "")
    try:
        result = sync_sql_context_for_workspace(
            db=db,
            workspace_id=ws.id,
            single_player={"player_id": body.player_id, "first_name": fn, "last_name": ln},
        )
    except OperationalError as exc:
        _raise_sportal_db_http_error(exc)
    except DeadlineExceeded as exc:
        raise HTTPException(
            status_code=504,
            detail=(
                "Firestore write timed out while saving vectors. "
                "Restart the API, re-sync this player (old chunks may be partial). "
                "New syncs use smaller batches; set FIRESTORE_WRITE_BATCH_SIZE=10 if needed."
            ),
        ) from exc
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


class AgentsLabRagChatTurn(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(..., min_length=1, max_length=8000)


class AgentsLabRagChatBody(BaseModel):
    message: str = Field(..., min_length=1, max_length=4000)
    player_key: str = Field(..., min_length=1, max_length=128)
    history: list[AgentsLabRagChatTurn] = Field(default_factory=list)
    include_shared: bool = True


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
                "progress_hint": feedback_agent_poll_progress_hint(j.result_json),
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


@router.post("/agents-lab/jobs/{job_id}/cancel")
async def agents_lab_cancel_agent_job(
    job_id: int,
    user_id: str = Query(..., min_length=1),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Stop a PENDING or RUNNING workspace agent job (Redis + optional RQ dequeue + feedback-agent cancel)."""
    from rq.job import Job

    _admin_require_user(db, user_id)
    ws = db.query(Workspace).filter(Workspace.user_id == user_id).one_or_none()
    if ws is None:
        raise HTTPException(status_code=404, detail="Workspace not found.")
    job = db.get(AgentJob, job_id)
    if job is None or job.workspace_id != ws.id:
        raise HTTPException(status_code=404, detail="Job not found.")
    st = (job.status or "").upper()
    if st in ("SUCCESS", "FAILED", "SKIPPED"):
        return {"ok": False, "reason": "job_already_finished", "status": job.status}

    merge_agent_job_result_json(
        job,
        {"cancel_requested_at": datetime.now(UTC).isoformat(), "cancel_requested": True},
    )
    set_agent_job_cancel_requested(job_id)

    review_id = (job.external_ref or "").strip()
    if not review_id and job.result_json:
        try:
            meta = json.loads(job.result_json)
            poll = meta.get("feedback_agent_poll")
            if isinstance(poll, dict):
                review_id = str(poll.get("review_id") or "").strip()
        except json.JSONDecodeError:
            review_id = ""
    if review_id:
        request_feedback_agent_cancel(review_id)

    rq_job_id: str | None = None
    if job.result_json:
        try:
            rq_job_id = str(json.loads(job.result_json).get("rq_job_id") or "").strip() or None
        except json.JSONDecodeError:
            rq_job_id = None
    if rq_job_id and st == "PENDING":
        try:
            rj = Job.fetch(rq_job_id, connection=get_redis())
            rj_status = str(rj.get_status()).lower()
            if rj_status in ("queued", "scheduled", "deferred"):
                rj.cancel()
        except Exception:  # noqa: BLE001
            pass

    if st == "PENDING":
        job.status = "FAILED"
        job.error_message = "Cancelled by user (stopped before worker started)."
        job.completed_at = datetime.now(UTC)
        db.commit()
        return {"ok": True, "stopped": "pending"}

    db.commit()
    return {"ok": True, "stopped": "running", "review_id": review_id or None}


@router.post("/agents-lab/rag-chat")
async def agents_lab_rag_chat(
    body: AgentsLabRagChatBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """
    RAG chat: embed the user message, search Firestore player + shared vectors, answer with OpenAI.
    Uses admin player-memory workspace (user_id=0); player_key is the Sportal / vector id (e.g. 19058).
    """
    from backendapi.services.rag_chat import run_rag_chat

    ws = ensure_workspace(user_id="0", db=db)
    history = [{"role": t.role, "content": t.content} for t in body.history]
    try:
        return run_rag_chat(
            db=db,
            workspace_id=ws.id,
            player_key=body.player_key.strip(),
            message=body.message.strip(),
            history=history,
            include_shared=body.include_shared,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/agents-lab/rag-players")
async def agents_lab_rag_players(
    search: str = Query(default="", max_length=200),
    limit: int = Query(default=50, ge=1, le=200),
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Players with vectors in Firestore (for Agent Lab chat player picker)."""
    ws = ensure_workspace(user_id="0", db=db)
    players = list_personal_player_keys(db=db, workspace_id=ws.id, search=search.strip())
    return {"players": players[:limit], "workspace_id": ws.id}


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
    rq_rid = enqueue_feedback_delegate_job(job.id)
    if not rq_rid:
        job.status = "FAILED"
        job.error_message = "Failed to enqueue feedback job."
        job.completed_at = datetime.now(UTC)
        db.commit()
        raise HTTPException(status_code=503, detail="Could not enqueue feedback job.")
    merge_agent_job_result_json(job, {"rq_job_id": rq_rid})
    db.commit()
    return {"success": True, "agent_job_id": job.id}


@router.post("/agents-lab/video-process")
async def agents_lab_video_process(
    body: AdminAgentsFeedbackReviewBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """YOLO pose only, then chain FEEDBACK_DELEGATE (same fields as feedback-reviews)."""
    uid = body.user_id.strip()
    _admin_require_user(db, uid)
    if not (body.video_url or "").strip():
        raise HTTPException(status_code=400, detail="video_url is required")
    ws = ensure_workspace(user_id=uid, db=db)
    payload = body.model_dump()
    payload["chain_feedback"] = True
    job = AgentJob(
        workspace_id=ws.id,
        agent_type="VIDEO_PROCESSING",
        status="PENDING",
        payload_json=json.dumps(payload, ensure_ascii=True),
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


@router.post("/agents-lab/video-process-stub")
async def agents_lab_video_stub(
    body: AdminAgentsUserIdBody,
    _admin: dict = Depends(get_admin_session_context),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Deprecated: use ``/agents-lab/feedback-reviews`` (pose + coaching) or ``/video-process``."""
    raise HTTPException(
        status_code=410,
        detail="Use POST /admin-api/agents-lab/feedback-reviews for the full pipeline.",
    )
