"""Load organization-wide shared coaching context from a Google Sheet (by spreadsheet id + gid).

Uses the same service-account credentials as the destination sheet. Intended for RAG-style
injection into feedback delegate jobs (applies to all players); per-player memory stays separate.

Sheet columns used (others ignored):
  Position, Category, Part of the Field, Title, Description
"""

from __future__ import annotations

import os
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

from backendapi.core.logger import info
from backendapi.services.shared_context_schema import (
    format_shared_context_sheet_summary,
    rows_from_sheet_values,
)


def _resolve_credentials_file(path: str) -> str:
    raw = (path or "").strip()
    if not raw:
        return "credentials.json"
    if os.path.isdir(raw):
        candidate = os.path.join(raw, "credentials.json")
        if os.path.isfile(candidate):
            return candidate
        json_files = [name for name in os.listdir(raw) if name.lower().endswith(".json")]
        if json_files:
            json_files.sort()
            return os.path.join(raw, json_files[0])
    return raw


def _a1_range_for_tab_title(title: str) -> str:
    safe = str(title).replace("'", "''")
    return f"'{safe}'!A1:ZZ"


def _sheet_title_for_gid(service: Any, spreadsheet_id: str, sheet_gid: int) -> str | None:
    resp = (
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )
    for sheet in resp.get("sheets", []):
        props = sheet.get("properties") or {}
        sid = props.get("sheetId")
        if sid is not None and int(sid) == sheet_gid:
            t = str(props.get("title") or "").strip()
            return t or None
    return None


def _load_sheet_values(*, max_chars: int) -> tuple[list[dict[str, Any]] | None, dict[str, Any]]:
    debug: dict[str, Any] = {
        "source": "Google Sheets (org-wide shared coaching rubric)",
        "env_spreadsheet_id": "FEEDBACK_SHARED_CONTEXT_SPREADSHEET_ID",
        "env_sheet_gid": "FEEDBACK_SHARED_CONTEXT_SHEET_GID",
        "max_chars_cap": max_chars,
        "columns_used": ["Position", "Category", "Part of the Field", "Title", "Description"],
        "format_loaded": (
            "Structured rows: Position, Category, Part of the Field, Title, Description "
            "(other sheet columns ignored)."
        ),
    }
    spreadsheet_id = (os.getenv("FEEDBACK_SHARED_CONTEXT_SPREADSHEET_ID") or "").strip()
    debug["spreadsheet_id"] = spreadsheet_id or None
    if not spreadsheet_id:
        debug["outcome"] = "disabled"
        debug["reason"] = "FEEDBACK_SHARED_CONTEXT_SPREADSHEET_ID is not set"
        return None, debug

    raw_gid = (os.getenv("FEEDBACK_SHARED_CONTEXT_SHEET_GID") or "").strip()
    try:
        sheet_gid = int(raw_gid) if raw_gid else 0
    except ValueError:
        sheet_gid = 0
    debug["sheet_gid_raw"] = raw_gid or None
    debug["sheet_gid"] = sheet_gid
    if sheet_gid <= 0:
        info("shared_feedback_context_skip", reason="missing_or_invalid_gid", spreadsheet_id=spreadsheet_id)
        debug["outcome"] = "skipped"
        debug["reason"] = "FEEDBACK_SHARED_CONTEXT_SHEET_GID missing or not a positive integer"
        return None, debug

    credentials_file = _resolve_credentials_file(os.getenv("DESTINATION_GOOGLE_CREDENTIALS_FILE", "credentials.json"))
    debug["credentials_file_resolved"] = credentials_file
    try:
        creds = service_account.Credentials.from_service_account_file(
            credentials_file,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    except Exception as exc:  # noqa: BLE001
        info("shared_feedback_context_skip", reason="credentials_failed", error=str(exc))
        debug["outcome"] = "error"
        debug["reason"] = "credentials_failed"
        debug["error"] = str(exc)
        return None, debug

    title = _sheet_title_for_gid(service, spreadsheet_id, sheet_gid)
    debug["sheet_title_resolved"] = title
    if not title:
        info("shared_feedback_context_skip", reason="gid_not_found", spreadsheet_id=spreadsheet_id, sheet_gid=sheet_gid)
        debug["outcome"] = "error"
        debug["reason"] = "No tab found for this spreadsheet_id + sheet_gid"
        return None, debug

    range_a1 = _a1_range_for_tab_title(title)
    debug["range_read"] = range_a1
    try:
        resp = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_a1)
            .execute()
        )
    except Exception as exc:  # noqa: BLE001
        info("shared_feedback_context_skip", reason="values_get_failed", error=str(exc))
        debug["outcome"] = "error"
        debug["reason"] = "values().get failed"
        debug["error"] = str(exc)
        return None, debug

    values = resp.get("values") or []
    debug["raw_row_count"] = len(values)
    if not values:
        debug["outcome"] = "empty"
        debug["reason"] = "Sheet tab has no values"
        return None, debug

    headers = [str(c).strip() for c in values[0]]
    if not headers:
        debug["outcome"] = "empty"
        debug["reason"] = "Header row is empty"
        return None, debug
    debug["column_headers"] = headers

    records, parse_warnings = rows_from_sheet_values(headers, values[1:])
    debug["parse_warnings"] = parse_warnings
    debug["structured_row_count"] = len(records)
    if not records:
        debug["outcome"] = "empty"
        debug["reason"] = parse_warnings[0] if parse_warnings else "No usable rows after column filter"
        return None, debug

    debug["outcome"] = "success"
    info(
        "shared_feedback_context_loaded",
        spreadsheet_id=spreadsheet_id,
        sheet_gid=sheet_gid,
        sheet_title=title,
        structured_rows=len(records),
    )
    return records, debug


def fetch_shared_feedback_context_records(*, max_rows: int = 5000) -> tuple[list[dict[str, Any]] | None, dict[str, Any]]:
    """Load structured shared-context rows from the configured sheet."""
    records, debug = _load_sheet_values(max_chars=500_000)
    if records is None:
        return None, debug
    if len(records) > max_rows:
        records = records[:max_rows]
        debug["truncated_to_max_rows"] = max_rows
    return records, debug


def fetch_shared_feedback_context_text(*, max_chars: int = 14_000) -> tuple[str | None, dict[str, Any]]:
    """
    Read the configured shared-context tab and return (text_or_none, debug_for_review_ui).

    Text is structured coaching blocks (Position / Category / Part of the Field / Title / Description),
    truncated to max_chars.
    """
    records, debug = _load_sheet_values(max_chars=max_chars)
    if records is None:
        return None, debug

    text = format_shared_context_sheet_summary(records, max_chars=max_chars)
    if not text:
        debug["outcome"] = "empty"
        return None, debug

    full_len = len(text)
    truncated = len(text) >= max_chars and full_len >= max_chars
    debug["chars_before_truncation"] = full_len
    debug["chars_injected"] = len(text)
    debug["truncated_to_max_chars"] = truncated
    return text, debug
