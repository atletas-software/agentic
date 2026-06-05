from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from backendapi.models.player_memory import SqlSyncCursor
from backendapi.services.chunk_semantics import (
    CHUNK_TYPES,
    extract_tags,
    format_embedding_input,
    infer_chunk_type,
)
from backendapi.services.context_scope import CONTEXT_SCOPE_PERSONAL
from backendapi.services.player_context_chunker import PLAYER_CONTEXT_VECTOR_FORMAT, structured_chunks_from_player_row
from backendapi.services.evaluation_text_clean import format_sql_cell_for_embedding
from backendapi.services.player_directory import upsert_player_directory_entry
from backendapi.services.player_key import player_key_column_names, row_dict_from_sql
from backendapi.services.player_memory_service import (
    delete_player_sql_sync_chunks_for_resync,
    delete_workspace_sql_sync_chunks_for_resync,
    insert_chunks,
)
from backendapi.services.player_memory_settings import get_player_memory_settings, effective_player_context_sql
from backendapi.services.player_summary import summarize_player_context
from backendapi.services.text_chunking import chunk_text_by_tokens


def _player_id_from_row_dict(rd: dict[str, str]) -> int:
    return (
        _safe_int(rd.get("player_user_id"))
        or _safe_int(rd.get("reviewee_id"))
        or _safe_int(rd.get("player_id"))
    )


def _is_structured_sql_columns(cols: list[str]) -> bool:
    norm = {str(c).strip().lower() for c in cols}
    return bool(norm & {"player_user_id", "profile_text", "videos", "feedback"})


def _single_player_bind_params(sql: str, sp: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Return (params, error_response) for single-player SQL execution."""
    pid = int(sp["player_id"])
    fn = str(sp.get("first_name") or "").strip()
    ln = str(sp.get("last_name") or "").strip()
    params: dict[str, Any] = {}
    if ":player_user_id" in sql:
        params["player_user_id"] = pid
    if ":first_name" in sql:
        params["first_name"] = fn
    if ":last_name" in sql:
        params["last_name"] = ln
    if params:
        return params, None
    return None, {
        "ok": False,
        "reason": "missing_player_binds",
        "detail": "Saved SQL must include :player_user_id or both :first_name and :last_name.",
        "single_player": True,
    }


def _sync_structured_sql_rows(
    *,
    db: Session,
    workspace_id: int,
    cols: list[str],
    rows: list[tuple],
    sp: dict[str, Any] | None,
    last_watermark: str,
    watermark_col: str,
    cursor: SqlSyncCursor | None,
) -> dict[str, Any]:
    deleted = 0
    written = 0
    players_seen: set[str] = set()
    watermark_max: str | None = None
    bind_used: dict[str, Any] | None = None

    if sp:
        bind_used = {"player_user_id": int(sp["player_id"])}
        rid_str = str(int(sp["player_id"]))
        deleted += delete_player_sql_sync_chunks_for_resync(
            db=db,
            workspace_id=workspace_id,
            player_key=rid_str,
        )
    else:
        deleted = delete_workspace_sql_sync_chunks_for_resync(
            db=db,
            workspace_id=workspace_id,
        )

    for row in rows:
        rd = row_dict_from_sql([str(c) for c in cols], row)
        rid_int = _player_id_from_row_dict(rd)
        if rid_int <= 0:
            continue
        rid_str = str(rid_int)
        structured = structured_chunks_from_player_row(rd)
        if not structured:
            continue

        player_name = _scalar_str(rd, "player_name", "reviewee_name")
        if not player_name and sp:
            player_name = f"{sp.get('first_name', '')} {sp.get('last_name', '')}".strip()

        batch: list[tuple[str, str, dict[str, Any]]] = []
        for idx, (embed_text, kind, extra) in enumerate(structured, start=1):
            ref = f"sql_sync:player_{rid_str}:{kind}"
            meta: dict[str, Any] = {
                "vector_format": PLAYER_CONTEXT_VECTOR_FORMAT,
                "context_scope": CONTEXT_SCOPE_PERSONAL,
                "player_id": rid_int,
                "player_key": rid_str,
                "player_name": player_name,
                "reviewee_id": rid_int,
                "club_name": _scalar_str(rd, "club_name"),
                "chunk_index": idx,
                "chunk_text": embed_text,
                "sync_mode": "single_player" if sp else "full_workspace",
                **extra,
            }
            batch.append((embed_text, ref, meta))

        written += insert_chunks(
            db=db,
            workspace_id=workspace_id,
            player_key=rid_str,
            source_type="sql_sync",
            texts_with_refs=batch,
            context_scope=CONTEXT_SCOPE_PERSONAL,
        )
        players_seen.add(rid_str)

        if player_name:
            parts = player_name.split(" ", 1)
            fn = parts[0]
            ln = parts[1] if len(parts) > 1 else ""
            upsert_player_directory_entry(
                db=db,
                workspace_id=workspace_id,
                player_key=rid_str,
                first_name=fn,
                last_name=ln,
            )

        if watermark_col and watermark_col in rd:
            wv = str(rd[watermark_col]).strip()
            if wv and (watermark_max is None or wv > watermark_max):
                watermark_max = wv

    if not sp:
        if cursor is None:
            cursor = SqlSyncCursor(workspace_id=workspace_id, source_name="default")
            db.add(cursor)
        if watermark_max is not None:
            cursor.watermark_value = watermark_max
        elif rows:
            cursor.watermark_value = json.dumps({"rows": len(rows)}, ensure_ascii=True)
    db.commit()

    out: dict[str, Any] = {
        "ok": True,
        "rows_read": len(rows),
        "players_processed": len(players_seen),
        "chunks_written": written,
        "chunks_deleted": deleted,
        "single_player": bool(sp),
        "structured": True,
    }
    if sp and bind_used is not None:
        out["bind_params_used"] = bind_used
    if cursor is not None and not sp:
        out["watermark"] = cursor.watermark_value
    return out


def _external_engine(database_url: str) -> Engine | None:
    from backendapi.services.external_sql_engine import get_external_sql_engine

    return get_external_sql_engine((database_url or "").strip())


def _normalize_external_db_url(raw_db_url: str) -> str:
    from backendapi.services.external_sql_engine import normalize_external_db_url

    return normalize_external_db_url(raw_db_url)


def _doc_lines_from_row(rd: dict[str, str]) -> list[str]:
    lines: list[str] = []
    for k, v in rd.items():
        disp = format_sql_cell_for_embedding(k, v)
        if disp:
            lines.append(f"{k}: {disp}")
    return lines


def _safe_int(val: Any) -> int:
    try:
        if val is None or val == "":
            return 0
        return int(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _scalar_str(rd: dict[str, str], *keys: str) -> str:
    for k in keys:
        v = str(rd.get(k) or "").strip()
        if v:
            return v
    return ""


def _format_date_cell(rd: dict[str, str], *keys: str) -> str:
    raw = _scalar_str(rd, *keys)
    if not raw:
        return ""
    if "T" in raw:
        return raw.split("T", 1)[0]
    return raw[:32]


def _evaluation_key(rd: dict[str, str]) -> str:
    v = str(rd.get("evaluation_id") or "").strip()
    return v if v else "none"


def sync_sql_context_for_workspace(
    *,
    db: Session,
    workspace_id: int,
    single_player: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Run PLAYER_CONTEXT_SQL against external DB and embed rows (source_type=sql_sync).

    Rows are grouped by (reviewee_id, evaluation_id) so join-expanded profile duplicates
    collapse to one logical evaluation. Vectors use reviewee_id as the canonical athlete key
    (not profile_id). Vectors use reviewee_id as the canonical athlete key
    """
    settings = get_player_memory_settings(db)
    eng = _external_engine(str(settings.get("sql_database_url") or ""))
    sql = effective_player_context_sql(settings)
    if eng is None or not sql:
        return {"ok": False, "reason": "not_configured"}

    cursor = (
        db.query(SqlSyncCursor)
        .filter(
            SqlSyncCursor.workspace_id == workspace_id,
            SqlSyncCursor.source_name == "default",
        )
        .one_or_none()
    )
    last_watermark = str(cursor.watermark_value or "").strip() if cursor is not None else ""

    sp: dict[str, Any] | None = None
    if single_player is not None:
        try:
            sp = {
                "player_id": int(single_player["player_id"]),
                "first_name": str(single_player.get("first_name") or ""),
                "last_name": str(single_player.get("last_name") or ""),
            }
        except (KeyError, TypeError, ValueError):
            return {"ok": False, "reason": "invalid_single_player"}
        if ":first_name" not in sql or ":last_name" not in sql:
            if ":player_user_id" not in sql:
                return {
                    "ok": False,
                    "reason": "single_player_requires_named_binds",
                    "detail": (
                        "Saved SQL must include :player_user_id or both :first_name and :last_name. "
                        "Open Admin → Vector memory → Settings, replace the hardcoded player list with "
                        "SELECT :player_user_id AS player_user_id in the FROM base subquery, then Save."
                    ),
                }
    try:
        with eng.connect() as conn:
            if sp:
                params, bind_err = _single_player_bind_params(sql, sp)
                if bind_err is not None:
                    return bind_err
                if ":last_watermark" in sql and last_watermark:
                    params["last_watermark"] = last_watermark
                result = conn.execute(text(sql), params)
                cols = list(result.keys())
                rows = [tuple(row) for row in result.fetchall()]
            else:
                stmt = text(sql)
                params: dict[str, Any] = {}
                if ":last_watermark" in sql and last_watermark:
                    params["last_watermark"] = last_watermark
                result = conn.execute(stmt, params)
                cols = list(result.keys())
                rows = [tuple(row) for row in result.fetchall()]
    except OperationalError as exc:
        detail = str(exc)
        if "1040" in detail or "Too many connections" in detail:
            return {
                "ok": False,
                "reason": "sportal_mysql_too_many_connections",
                "detail": (
                    "Sportal MySQL max_connections exceeded. Wait and retry, or reduce "
                    "concurrent syncs. The API uses a shared small pool (not a new pool per request)."
                ),
            }
        return {"ok": False, "reason": "sportal_mysql_error", "detail": detail}

    if _is_structured_sql_columns(cols):
        return _sync_structured_sql_rows(
            db=db,
            workspace_id=workspace_id,
            cols=cols,
            rows=rows,
            sp=sp,
            last_watermark=last_watermark,
            watermark_col=str(settings.get("watermark_column") or "").strip().lower(),
            cursor=cursor,
        )

    configured_key_cols = str(settings.get("player_key_columns") or "").strip()
    key_cols = player_key_column_names() if not configured_key_cols else [c.strip().lower() for c in configured_key_cols.split(",") if c.strip()]

    watermark_col = str(settings.get("watermark_column") or "").strip().lower()
    watermark_max: str | None = None

    group_docs: dict[tuple[str, str], list[str]] = defaultdict(list)
    group_doc_seen: dict[tuple[str, str], set[str]] = defaultdict(set)
    group_first_rd: dict[tuple[str, str], dict[str, str]] = {}
    by_reviewee_name: dict[str, tuple[str, str]] = {}

    for row in rows:
        rd = row_dict_from_sql([str(c) for c in cols], row)
        rid_int = _player_id_from_row_dict(rd)
        if rid_int <= 0:
            continue
        if not sp and _row_empty_for_keys(rd, key_cols):
            continue
        doc_lines = _doc_lines_from_row(rd)
        doc = "\n".join(doc_lines)
        if not doc:
            continue
        ek = _evaluation_key(rd)
        key = (str(rid_int), ek)
        if doc in group_doc_seen[key]:
            continue
        group_doc_seen[key].add(doc)
        group_docs[key].append(doc)
        if key not in group_first_rd:
            group_first_rd[key] = rd
        fn = str(rd.get("first_name") or "").strip()
        ln = str(rd.get("last_name") or "").strip()
        if fn or ln:
            by_reviewee_name[str(rid_int)] = (fn, ln)
        if watermark_col and watermark_col in rd:
            wv = str(rd[watermark_col]).strip()
            if wv and (watermark_max is None or wv > watermark_max):
                watermark_max = wv

    deleted = 0
    bind_used: dict[str, str] | None = None

    if sp:
        params_used, bind_err = _single_player_bind_params(sql, sp)
        if bind_err is not None:
            return bind_err
        bind_used = params_used
        if ":player_user_id" in sql and not rows:
            return {
                "ok": True,
                "rows_read": 0,
                "players_processed": 0,
                "chunks_written": 0,
                "chunks_deleted": 0,
                "single_player": True,
                "skipped": True,
                "bind_params_used": bind_used,
                "hint": "MySQL returned 0 rows for this player_user_id. Check SQL and player ID.",
            }
        fn = str(sp.get("first_name") or "").strip()
        ln = str(sp.get("last_name") or "").strip()
        if ":first_name" in sql and ":last_name" in sql:
            if not fn or not ln:
                return {
                    "ok": False,
                    "reason": "empty_player_names",
                    "detail": "first_name and last_name must be non-empty for :first_name / :last_name binds.",
                    "bind_params_used": bind_used,
                    "single_player": True,
                }
        if not rows:
            return {
                "ok": True,
                "rows_read": 0,
                "players_processed": 0,
                "chunks_written": 0,
                "chunks_deleted": 0,
                "single_player": True,
                "skipped": True,
                "bind_params_used": bind_used,
                "hint": (
                    "MySQL returned 0 rows for this name. Check saved SQL filters "
                    "(reviewee_name LIKE, e.status, values_json), spelling, and TRIM on profile columns."
                ),
            }
        if not group_docs:
            return {
                "ok": True,
                "rows_read": len(rows),
                "players_processed": 0,
                "chunks_written": 0,
                "chunks_deleted": 0,
                "single_player": True,
                "skipped": True,
                "bind_params_used": bind_used,
                "hint": (
                    f"{len(rows)} row(s) returned but no embeddable text after cleaning "
                    "(HTML/JSON/N/A). Check values_json and column names."
                ),
            }
        distinct_reviewees = {k[0] for k in group_docs if k[0] and k[0] != "0"}
        for rid_str in sorted(distinct_reviewees):
            deleted += delete_player_sql_sync_chunks_for_resync(
                db=db,
                workspace_id=workspace_id,
                player_key=rid_str,
            )
    else:
        deleted = delete_workspace_sql_sync_chunks_for_resync(
            db=db, workspace_id=workspace_id
        )

    written = 0
    players_seen: set[str] = set()

    for (rid_str, ek), docs in group_docs.items():
        merged = "\n\n".join(docs)
        if not merged.strip():
            continue
        summarized = summarize_player_context(player_key=rid_str, raw_context=merged)
        summary_short = (summarized or "")[:800]
        body_for_chunks = f"{summarized}\n\n---\n\n{merged}".strip()[:50_000]
        parts = chunk_text_by_tokens(body_for_chunks) if body_for_chunks else []
        if not parts:
            continue

        rd0 = group_first_rd.get((rid_str, ek)) or {}
        reviewee_id_int = _safe_int(rid_str)
        profile_id_int = _safe_int(_player_id_from_row(rd0))
        eval_id_int = _safe_int(ek) if ek != "none" else 0
        eval_slug = ek if ek != "none" else "na"
        name_from_row = (
            f"{_scalar_str(rd0, 'first_name')}{' ' if _scalar_str(rd0, 'first_name') else ''}"
            f"{_scalar_str(rd0, 'last_name')}"
        ).strip()
        if name_from_row:
            player_name = name_from_row
        elif sp:
            player_name = f"{sp['first_name']} {sp['last_name']}".strip()
        else:
            player_name = rid_str

        batch: list[tuple[str, str, dict[str, Any]]] = []
        for p_idx, part in enumerate(parts):
            chunk_type = infer_chunk_type(part)
            if chunk_type not in CHUNK_TYPES:
                chunk_type = "technical_skills"
            tags = extract_tags(part)
            embed_in = format_embedding_input(summary=summary_short, chunk_text=part)
            chunk_index = p_idx + 1
            vector_id = f"reviewee_{reviewee_id_int}_eval_{eval_slug}_chunk_{chunk_index}"
            ref = f"sql_sync:{vector_id}"
            meta: dict[str, Any] = {
                "vector_id": vector_id,
                "vector_format": "player_memory_v2",
                "workspace_id": workspace_id,
                "player_id": reviewee_id_int,
                "profile_id": profile_id_int,
                "player_key": rid_str,
                "player_name": player_name,
                "reviewee_id": reviewee_id_int,
                "reviewee_name": _scalar_str(rd0, "reviewee_name"),
                "evaluation_id": eval_id_int,
                "club_name": _scalar_str(rd0, "club_name"),
                "completion_date": _format_date_cell(rd0, "completion_date", "evaluation_created"),
                "chunk_index": chunk_index,
                "chunk_type": chunk_type,
                "source": "evaluation",
                "chunk_text": part,
                "summary": summary_short,
                "tags": tags,
                "form_id": _safe_int(rd0.get("form_id")),
                "sport": _scalar_str(rd0, "sport", "sport_id"),
                "age_group": _scalar_str(rd0, "age_group", "age_group_name"),
                "sync_mode": "single_player" if sp else "full_workspace",
            }
            batch.append((embed_in, ref, meta))
        written += insert_chunks(
            db=db,
            workspace_id=workspace_id,
            player_key=rid_str,
            source_type="sql_sync",
            texts_with_refs=batch,
        )
        players_seen.add(rid_str)
        nm = by_reviewee_name.get(rid_str)
        if nm is not None:
            upsert_player_directory_entry(
                db=db,
                workspace_id=workspace_id,
                player_key=rid_str,
                first_name=nm[0],
                last_name=nm[1],
            )

    if not sp:
        if cursor is None:
            cursor = SqlSyncCursor(workspace_id=workspace_id, source_name="default")
            db.add(cursor)
        if watermark_max is not None:
            cursor.watermark_value = watermark_max
        elif rows:
            cursor.watermark_value = json.dumps({"rows": len(rows)}, ensure_ascii=True)
    db.commit()

    out: dict[str, Any] = {
        "ok": True,
        "rows_read": len(rows),
        "players_processed": len(players_seen),
        "chunks_written": written,
        "chunks_deleted": deleted,
        "single_player": bool(sp),
    }
    if sp and bind_used is not None:
        out["bind_params_used"] = bind_used
    if cursor is not None and not sp:
        out["watermark"] = cursor.watermark_value
    return out


def _row_empty_for_keys(rd: dict[str, str], cols: list[str]) -> bool:
    return not any(str(rd.get(c, "")).strip() for c in cols)


def _player_id_from_row(rd: dict[str, str]) -> str:
    for key in ("player_id", "profile.player_id", "profile_player_id"):
        value = str(rd.get(key, "")).strip()
        if value:
            return value
    return ""
