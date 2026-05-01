from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import UTC, datetime
from typing import Any

import redis
from sqlalchemy.orm import Session

from app.core.logger import error, info
from app.models.google_oauth import (
    SheetSyncEvent,
    SheetSyncRowState,
    SheetSyncRun,
    UserGoogleSheetSelection,
    UserSyncSetting,
)
from app.models.auth import UserAccount
from app.services.destination_sheet import DestinationSheetService
from app.services.google_integration import resolve_polling_interval_seconds
from app.services.google_sheets import read_sheet, update_sheet
from app.services.sync_backoff import is_user_blocked

REQUIRED_SOURCE_HEADERS = [
    "First and Last name",
    "Team color",
    "Team Number",
    "Position Played",
    "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out",
    "Link to game",
]

BUSINESS_DESTINATION_HEADERS = [
    "Timestamp",
    "Date",
    "First and Last name",
    "Team color",
    "Team Number",
    "Position Played",
    "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out",
    "Link to game",
]

SYNC_DESTINATION_HEADERS = [
    "source_row_key",
    "source_row_hash",
    "source_spreadsheet_id",
    "source_sheet_name",
    "source_row_number",
    "source_last_seen_at",
    "sync_status",
    "sync_action",
    "sync_error",
    "last_synced_at",
    "trace_id",
]

DESTINATION_HEADERS = [
    *BUSINESS_DESTINATION_HEADERS,
    *SYNC_DESTINATION_HEADERS,
]

REQUIRED_DESTINATION_SYNC_HEADERS = SYNC_DESTINATION_HEADERS
SOURCE_SYNC_STATUS_HEADER = "sync_status"

SOURCE_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "First and Last name": (
        "name",
        "first and last name",
        "player name/ team name",
        "player name/team name",
        "player name team name",
    ),
    "Team color": (
        "team color",
        "team colour",
        "color",
        "colour",
        "clour",
    ),
    "Team Number": (
        "team number",
        "number",
        "jersey number",
        "jersey no",
        "jersey #",
    ),
    "Position Played": ("position played", "position", "position play"),
    "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out": (
        "game details",
        "game details - log in info",
        "game details - log in info for the site when you were subbed in or out",
    ),
    "Link to game": (
        "link to game",
        "link to the game",
        "game video",
        "link to the game video",
        "link to game video",
    ),
}

OPTIONAL_SOURCE_HEADERS = ["Timestamp", "Date"]

DEFAULT_HASH_FIELDS = ("name", "color", "jersey", "position", "game_details", "video", "timestamp", "date")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SYNC_USER_LOCK_TTL_SECONDS = int(os.getenv("SYNC_USER_LOCK_TTL_SECONDS", "300"))
SYNC_USER_LOCK_PREFIX = os.getenv("SYNC_USER_LOCK_PREFIX", "sheet-sync-lock")
SYNC_RECONCILE_DESTINATION = os.getenv("SYNC_RECONCILE_DESTINATION", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _parse_hash_fields() -> tuple[str, ...]:
    raw = os.getenv("SYNC_HASH_FIELDS", "")
    if not raw.strip():
        return DEFAULT_HASH_FIELDS
    parsed = tuple(field.strip().lower() for field in raw.split(",") if field.strip())
    return parsed or DEFAULT_HASH_FIELDS


HASH_FIELDS = _parse_hash_fields()


def _normalize_row(headers: list[str], row: list[Any]) -> dict[str, str]:
    values: dict[str, str] = {}
    for i, h in enumerate(headers):
        values[h] = str(row[i]).strip() if i < len(row) and row[i] is not None else ""
    return values


def _normalize_header_key(value: str) -> str:
    # Collapse newlines and extra spaces so "Game Details -\nLOG IN..." still matches.
    compact = re.sub(r"[^a-zA-Z0-9]+", " ", (value or "").strip())
    compact = re.sub(r"\s+", " ", compact)
    return compact.lower()


def _normalize_for_hash(field: str, value: str) -> str:
    compact = re.sub(r"\s+", " ", (value or "").strip())
    if field in {"name", "position", "game_details"}:
        return compact.lower()
    if field in {"color"}:
        return compact.lower().replace("colour", "color")
    if field == "jersey":
        return re.sub(r"[^\d]", "", compact) or compact.lower()
    if field == "video":
        lowered = compact.lower()
        lowered = re.sub(r"^https?://(www\.)?", "", lowered)
        return lowered.rstrip("/")
    if field in {"timestamp", "date"}:
        return compact
    return compact.lower()


def _canonical_source_value(source_values: dict[str, str], resolved_headers: dict[str, str], canonical: str) -> str:
    col = resolved_headers.get(canonical, canonical)
    return source_values.get(col, "").strip()


def _build_source_business_values(source_values: dict[str, str], resolved_headers: dict[str, str]) -> dict[str, str]:
    return {
        "name": _canonical_source_value(source_values, resolved_headers, "First and Last name"),
        "color": _canonical_source_value(source_values, resolved_headers, "Team color"),
        "jersey": _canonical_source_value(source_values, resolved_headers, "Team Number"),
        "position": _canonical_source_value(source_values, resolved_headers, "Position Played"),
        "game_details": _canonical_source_value(
            source_values,
            resolved_headers,
            "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out",
        ),
        "video": _canonical_source_value(source_values, resolved_headers, "Link to game"),
        "timestamp": _canonical_source_value(source_values, resolved_headers, "Timestamp"),
        "date": _canonical_source_value(source_values, resolved_headers, "Date"),
    }


def _build_hash_payload(source_business_values: dict[str, str]) -> dict[str, str]:
    payload: dict[str, str] = {}
    for field in HASH_FIELDS:
        payload[field] = _normalize_for_hash(field, source_business_values.get(field, ""))
    return payload


def _business_row_key(spreadsheet_id: str, tab_name: str, source_business_values: dict[str, str]) -> str:
    key_fields = {
        "name": _normalize_for_hash("name", source_business_values.get("name", "")),
        "jersey": _normalize_for_hash("jersey", source_business_values.get("jersey", "")),
        "video": _normalize_for_hash("video", source_business_values.get("video", "")),
        "date": _normalize_for_hash("date", source_business_values.get("date", "")),
        "timestamp": _normalize_for_hash("timestamp", source_business_values.get("timestamp", "")),
        "position": _normalize_for_hash("position", source_business_values.get("position", "")),
        "game_details": _normalize_for_hash("game_details", source_business_values.get("game_details", "")),
    }
    digest = hashlib.sha256(json.dumps(key_fields, sort_keys=True, ensure_ascii=True).encode("utf-8")).hexdigest()[:24]
    return f"{spreadsheet_id}:{tab_name}:{digest}"


def _destination_field_changes(
    *,
    existing_row: list[str] | None,
    destination_headers: list[str],
    new_row: list[str],
) -> dict[str, dict[str, str]]:
    if not existing_row:
        return {}
    changes: dict[str, dict[str, str]] = {}
    watch_fields = {
        "First and Last name",
        "Team color",
        "Team Number",
        "Position Played",
        "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out",
        "Link to game",
        "Timestamp",
        "Date",
    }
    for idx, header in enumerate(destination_headers):
        if header not in watch_fields:
            continue
        old = str(existing_row[idx]) if idx < len(existing_row) and existing_row[idx] is not None else ""
        new = str(new_row[idx]) if idx < len(new_row) and new_row[idx] is not None else ""
        if old != new:
            changes[header] = {"from": old, "to": new}
    return changes


def _with_user_lock(user_id: str):
    if not REDIS_URL:
        return None, None, True
    lock_key = f"{SYNC_USER_LOCK_PREFIX}:{user_id}"
    token = hashlib.sha256(f"{user_id}:{datetime.now(UTC).isoformat()}".encode("utf-8")).hexdigest()
    try:
        client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        acquired = bool(client.set(lock_key, token, nx=True, ex=SYNC_USER_LOCK_TTL_SECONDS))
        return client, (lock_key, token), acquired
    except Exception as exc:  # noqa: BLE001
        info("sync_lock_unavailable", user_id=user_id, error=str(exc))
        return None, None, True


def _release_user_lock(client: redis.Redis | None, lock_meta: tuple[str, str] | None) -> None:
    if client is None or lock_meta is None:
        return
    lock_key, token = lock_meta
    try:
        current = client.get(lock_key)
        if current == token:
            client.delete(lock_key)
    except Exception:  # noqa: BLE001
        pass


def _resolve_source_header_aliases(source_headers: list[str]) -> tuple[dict[str, str], list[str]]:
    normalized_to_original = {_normalize_header_key(h): h for h in source_headers}
    resolved: dict[str, str] = {}
    missing: list[str] = []
    for required in REQUIRED_SOURCE_HEADERS:
        candidates = (required, *SOURCE_HEADER_ALIASES.get(required, ()))
        matched = None
        for candidate in candidates:
            matched = normalized_to_original.get(_normalize_header_key(candidate))
            if matched is not None:
                break
        if matched is None and required.lower().startswith("game details"):
            # Fallback for variants around "Game Details - LOG IN INFO ...".
            matched = next(
                (
                    original
                    for norm, original in normalized_to_original.items()
                    if "game details" in norm and "log in info for the site" in norm
                ),
                None,
            )
        if matched is None:
            missing.append(required)
            continue
        resolved[required] = matched

    for optional in OPTIONAL_SOURCE_HEADERS:
        matched = normalized_to_original.get(_normalize_header_key(optional))
        if matched is not None:
            resolved[optional] = matched
    return resolved, missing


def _row_hash(payload: dict[str, str]) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _source_row_key(spreadsheet_id: str, tab_name: str, row_number: int) -> str:
    return f"{spreadsheet_id}:{tab_name}:{row_number}"


def _column_letter(col_idx_1based: int) -> str:
    result = ""
    n = col_idx_1based
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _ensure_sync_status_column(
    *,
    user_id: str,
    spreadsheet_id: str,
    tab_name: str,
    source_headers: list[str],
    db: Session,
) -> int:
    if SOURCE_SYNC_STATUS_HEADER in source_headers:
        return source_headers.index(SOURCE_SYNC_STATUS_HEADER) + 1
    updated_headers = [*source_headers, SOURCE_SYNC_STATUS_HEADER]
    update_sheet(
        user_id=user_id,
        spreadsheet_id=spreadsheet_id,
        range_name=f"{tab_name}!A1",
        values=[updated_headers],
        db=db,
    )
    source_headers.append(SOURCE_SYNC_STATUS_HEADER)
    return len(source_headers)


def _mark_source_row_failed(
    *,
    user_id: str,
    spreadsheet_id: str,
    tab_name: str,
    source_headers: list[str],
    row_number: int,
    db: Session,
) -> None:
    _mark_source_row_status(
        user_id=user_id,
        spreadsheet_id=spreadsheet_id,
        tab_name=tab_name,
        source_headers=source_headers,
        row_number=row_number,
        status_value="FAILED",
        db=db,
    )


def _mark_source_row_status(
    *,
    user_id: str,
    spreadsheet_id: str,
    tab_name: str,
    source_headers: list[str],
    row_number: int,
    status_value: str,
    db: Session,
) -> None:
    col_index = _ensure_sync_status_column(
        user_id=user_id,
        spreadsheet_id=spreadsheet_id,
        tab_name=tab_name,
        source_headers=source_headers,
        db=db,
    )
    cell = f"{tab_name}!{_column_letter(col_index)}{row_number}"
    update_sheet(
        user_id=user_id,
        spreadsheet_id=spreadsheet_id,
        range_name=cell,
        values=[[status_value]],
        db=db,
    )


def _build_destination_row(
    source_values: dict[str, str],
    source_row_key: str,
    row_hash: str,
    spreadsheet_id: str,
    tab_name: str,
    row_number: int,
    action: str,
    destination_headers: list[str],
    existing_row: list[str] | None = None,
) -> list[str]:
    now_iso = datetime.now(UTC).isoformat()
    mapped = {
        "source_row_key": source_row_key,
        "source_row_hash": row_hash,
        "source_spreadsheet_id": spreadsheet_id,
        "source_sheet_name": tab_name,
        "source_row_number": str(row_number),
        "source_last_seen_at": now_iso,
        "sync_status": "SUCCESS",
        "sync_action": action,
        "sync_error": "",
        "last_synced_at": now_iso,
        "trace_id": hashlib.md5(source_row_key.encode("utf-8")).hexdigest(),
        # Keep business headers same as source sheet headers.
        "First and Last name": source_values.get("First and Last name", ""),
        "Team color": source_values.get("Team color", ""),
        "Team Number": source_values.get("Team Number", ""),
        "Position Played": source_values.get("Position Played", ""),
        "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out": source_values.get(
            "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out", ""
        ),
        "Link to game": source_values.get("Link to game", ""),
        "Timestamp": source_values.get("Timestamp", ""),
        "Date": source_values.get("Date", ""),
    }
    mapped_normalized = {_normalize_header_key(k): v for k, v in mapped.items()}
    row = list(existing_row or [])
    if len(row) < len(destination_headers):
        row.extend([""] * (len(destination_headers) - len(row)))
    for index, header in enumerate(destination_headers):
        normalized_header = _normalize_header_key(header)
        if normalized_header in mapped_normalized:
            row[index] = mapped_normalized[normalized_header]
    return row


def _event(
    db: Session,
    run_id: int,
    user_id: str,
    spreadsheet_id: str,
    tab_name: str,
    source_row_key: str,
    row_number: int,
    action: str,
    status: str,
    message: str,
    payload_snapshot: dict[str, Any] | None = None,
    destination_response: dict[str, Any] | None = None,
) -> None:
    db.add(
        SheetSyncEvent(
            run_id=run_id,
            user_id=user_id,
            spreadsheet_id=spreadsheet_id,
            tab_name=tab_name,
            source_row_key=source_row_key,
            row_number=row_number,
            action=action,
            status=status,
            message=message,
            payload_snapshot=json.dumps(payload_snapshot or {}),
            destination_response=json.dumps(destination_response or {}),
        )
    )


def _log_skip_once(
    *,
    db: Session,
    run_id: int,
    user_id: str,
    spreadsheet_id: str,
    tab_name: str,
    source_row_key: str,
    row_number: int,
    message: str,
) -> None:
    existing = (
        db.query(SheetSyncEvent.id)
        .filter(
            SheetSyncEvent.user_id == user_id,
            SheetSyncEvent.source_row_key == source_row_key,
            SheetSyncEvent.action == "SKIP",
            SheetSyncEvent.message == message,
        )
        .first()
    )
    if existing is not None:
        return
    _event(
        db=db,
        run_id=run_id,
        user_id=user_id,
        spreadsheet_id=spreadsheet_id,
        tab_name=tab_name,
        source_row_key=source_row_key,
        row_number=row_number,
        action="SKIP",
        status="SUCCESS",
        message=message,
    )


def _clear_skip_logs_for_row(*, db: Session, user_id: str, source_row_key: str) -> None:
    (
        db.query(SheetSyncEvent)
        .filter(
            SheetSyncEvent.user_id == user_id,
            SheetSyncEvent.source_row_key == source_row_key,
            SheetSyncEvent.action == "SKIP",
        )
        .delete(synchronize_session=False)
    )


def _ensure_destination_headers(
    destination: DestinationSheetService,
    *,
    sheet_name: str,
    headers: list[str],
    rows: list[list[str]],
) -> tuple[list[str], list[list[str]]]:
    removed_headers = {
        "Type of Video",
        "Player Name/ Team Name",
        "Team Color",
        "Jersey Number",
        "Position played",
        "Game Details",
        "Link to the game video",
        "User ID",
        "User Detail",
    }
    existing_headers = [str(h).strip() for h in headers]
    existing_headers = [h for h in existing_headers if h not in removed_headers]
    existing_set = set(existing_headers)
    canonical_headers = [*DESTINATION_HEADERS, *[h for h in existing_headers if h not in DESTINATION_HEADERS]]
    needs_reorder = existing_headers != canonical_headers
    missing = [h for h in DESTINATION_HEADERS if h not in existing_set]

    if not needs_reorder and not missing:
        return existing_headers, rows

    final_headers = [*canonical_headers]
    original_headers = [str(h).strip() for h in headers]
    source_index = {header: idx for idx, header in enumerate(original_headers)}
    reordered_rows: list[list[str]] = []
    for row in rows:
        normalized_row = [str(v) if v is not None else "" for v in row]
        mapped = []
        for header in final_headers:
            idx = source_index.get(header)
            mapped.append(normalized_row[idx] if idx is not None and idx < len(normalized_row) else "")
        reordered_rows.append(mapped)

    destination.overwrite_values([final_headers, *reordered_rows], sheet_name=sheet_name)
    return final_headers, reordered_rows


def run_sync_once_for_active_sheets(db: Session) -> dict[str, int]:
    selected_user_rows = db.query(UserGoogleSheetSelection.user_id).distinct().all()
    return run_sync_once_for_users(db=db, user_ids=[row[0] for row in selected_user_rows])


def get_due_user_ids_for_sync(db: Session, now: datetime | None = None) -> list[str]:
    current = now or datetime.now(UTC)
    selected_user_rows = db.query(UserGoogleSheetSelection.user_id).distinct().all()
    selected_user_ids = [row[0] for row in selected_user_rows]
    due_user_ids: list[str] = []
    for user_id in selected_user_ids:
        try:
            user_id_int = int(user_id)
        except ValueError:
            info("sync_user_skipped_invalid_user_id", user_id=user_id)
            continue
        user_account = db.query(UserAccount).filter(UserAccount.id == user_id_int, UserAccount.is_active.is_(True)).one_or_none()
        if user_account is None:
            info("sync_user_skipped_not_authenticated", user_id=user_id)
            continue
        blocked, next_allowed_at = is_user_blocked(user_id=user_id, db=db, now=current)
        if blocked:
            info("sync_user_backoff_active", user_id=user_id, next_allowed_at=next_allowed_at.isoformat() if next_allowed_at else None)
            continue
        setting = db.query(UserSyncSetting).filter(UserSyncSetting.user_id == user_id).one_or_none()
        if setting is not None and not setting.sync_enabled:
            info("sync_user_skipped_disabled", user_id=user_id)
            continue
        interval_seconds = resolve_polling_interval_seconds(
            setting.polling_interval_minutes if setting is not None else 60
        )
        latest_run = (
            db.query(SheetSyncRun)
            .filter(SheetSyncRun.user_id == user_id)
            .order_by(SheetSyncRun.started_at.desc())
            .first()
        )
        if latest_run is None or latest_run.started_at is None:
            due_user_ids.append(user_id)
            continue
        elapsed_seconds = (current - latest_run.started_at).total_seconds()
        if elapsed_seconds >= interval_seconds:
            due_user_ids.append(user_id)
    return due_user_ids


def run_sync_once_for_users(db: Session, user_ids: list[str]) -> dict[str, int]:
    if not user_ids:
        return {"runs": 0, "rows": 0}
    destination = DestinationSheetService()
    if not destination.is_enabled():
        info("sync_skipped_destination_not_configured")
        return {"runs": 0, "rows": 0}

    selected_sheets = db.query(UserGoogleSheetSelection).filter(UserGoogleSheetSelection.user_id.in_(user_ids)).all()
    total_rows_processed = 0
    run_count = 0

    for selected in selected_sheets:
        lock_client, lock_meta, lock_acquired = _with_user_lock(selected.user_id)
        if not lock_acquired:
            info("sync_user_skipped_locked", user_id=selected.user_id)
            continue
        try:
            user_id_int = int(selected.user_id)
        except ValueError:
            info("sync_user_skipped_invalid_user_id", user_id=selected.user_id)
            continue
        user_account = db.query(UserAccount).filter(UserAccount.id == user_id_int, UserAccount.is_active.is_(True)).one_or_none()
        if user_account is None:
            info("sync_user_skipped_not_authenticated", user_id=selected.user_id)
            continue
        setting = db.query(UserSyncSetting).filter(UserSyncSetting.user_id == selected.user_id).one_or_none()
        if setting is None or not setting.sync_enabled:
            info("sync_user_skipped_disabled", user_id=selected.user_id)
            continue
        target_sheet_name = destination.user_sheet_name(user_account.email)
        headers, dest_rows = destination.load_headers_and_rows(
            sheet_name=target_sheet_name,
            ensure_sheet=True,
            initialize_headers=DESTINATION_HEADERS,
        )
        destination_headers, dest_rows = _ensure_destination_headers(
            destination,
            sheet_name=target_sheet_name,
            headers=[str(h).strip() for h in headers],
            rows=dest_rows,
        )
        missing_destination_headers = [
            h for h in REQUIRED_DESTINATION_SYNC_HEADERS if h not in destination_headers
        ]
        if missing_destination_headers:
            info(
                "sync_destination_headers_missing",
                user_id=selected.user_id,
                target_sheet_name=target_sheet_name,
                required=REQUIRED_DESTINATION_SYNC_HEADERS,
                missing=missing_destination_headers,
                found=destination_headers,
            )
            continue

        destination_index: dict[str, int] = {}
        source_row_key_col = destination_headers.index("source_row_key")
        for i, row in enumerate(dest_rows, start=2):
            if row and len(row) > source_row_key_col and row[source_row_key_col]:
                destination_index[row[source_row_key_col]] = i

        run = SheetSyncRun(
            user_id=selected.user_id,
            spreadsheet_id=selected.spreadsheet_id,
            tab_name="",
            status="RUNNING",
        )
        db.add(run)
        db.commit()
        db.refresh(run)
        run_count += 1

        try:
            read = read_sheet(
                user_id=selected.user_id,
                spreadsheet_id=selected.spreadsheet_id,
                range_name="A1:ZZ",
                db=db,
            )
            values = read.get("values", [])
            if not values:
                run.status = "SUCCESS"
                run.completed_at = datetime.now(UTC)
                db.commit()
                continue

            source_headers = [str(h).strip() for h in values[0]]
            resolved_headers, missing = _resolve_source_header_aliases(source_headers)
            tab_name = read.get("range", "Sheet1").split("!")[0]
            run.tab_name = tab_name
            rows = values[1:]
            run.rows_scanned = len(rows)
            if missing:
                run.error_message = f"Unresolved source columns: {', '.join(missing)}"
                info(
                    "sync_source_headers_partially_resolved",
                    user_id=selected.user_id,
                    spreadsheet_id=selected.spreadsheet_id,
                    tab_name=tab_name,
                    missing=missing,
                    source_headers=source_headers,
                )
                _event(
                    db=db,
                    run_id=run.id,
                    user_id=selected.user_id,
                    spreadsheet_id=selected.spreadsheet_id,
                    tab_name=tab_name,
                    source_row_key=f"{selected.spreadsheet_id}:{tab_name}:*",
                    row_number=0,
                    action="VALIDATE",
                    status="SUCCESS",
                    message=f"Proceeding with flexible header mapping; unresolved headers: {', '.join(missing)}",
                )

            source_keys_seen: set[str] = set()
            for idx, row in enumerate(rows, start=2):
                source_key = _source_row_key(selected.spreadsheet_id, tab_name, idx)
                try:
                    source_values = _normalize_row(source_headers, row)
                    source_name_col = resolved_headers.get("First and Last name", "First and Last name")
                    source_color_col = resolved_headers.get("Team color", "Team color")
                    source_jersey_col = resolved_headers.get("Team Number", "Team Number")
                    source_position_col = resolved_headers.get("Position Played", "Position Played")
                    source_game_details_col = resolved_headers.get(
                        "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out",
                        "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out",
                    )
                    source_link_col = resolved_headers.get("Link to game", "Link to game")
                    required_row_fields = [
                        ("First and Last name", source_name_col),
                        ("Team color", source_color_col),
                        ("Team Number", source_jersey_col),
                        ("Position Played", source_position_col),
                        (
                            "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out",
                            source_game_details_col,
                        ),
                        ("Link to game", source_link_col),
                    ]
                    missing_row_fields = [
                        display_name
                        for display_name, col_name in required_row_fields
                        if not source_values.get(col_name, "").strip()
                    ]
                    if missing_row_fields:
                        _log_skip_once(
                            db=db,
                            run_id=run.id,
                            user_id=selected.user_id,
                            spreadsheet_id=selected.spreadsheet_id,
                            tab_name=tab_name,
                            source_row_key=source_key,
                            row_number=idx,
                            message=f"Skipped incomplete row. Missing values: {', '.join(missing_row_fields)}",
                        )
                        try:
                            _mark_source_row_status(
                                user_id=selected.user_id,
                                spreadsheet_id=selected.spreadsheet_id,
                                tab_name=tab_name,
                                source_headers=source_headers,
                                row_number=idx,
                                status_value="SKIPPED",
                                db=db,
                            )
                        except Exception:  # noqa: BLE001
                            pass
                        continue
                    source_business_values = _build_source_business_values(source_values, resolved_headers)
                    payload_for_hash = _build_hash_payload(source_business_values)
                    digest = _row_hash(payload_for_hash)
                    source_key = _business_row_key(selected.spreadsheet_id, tab_name, source_business_values)
                    source_keys_seen.add(source_key)

                    state = (
                        db.query(SheetSyncRowState)
                        .filter(
                            SheetSyncRowState.user_id == selected.user_id,
                            SheetSyncRowState.spreadsheet_id == selected.spreadsheet_id,
                            SheetSyncRowState.tab_name == tab_name,
                            SheetSyncRowState.source_row_key == source_key,
                        )
                        .one_or_none()
                    )

                    destination_has_row = source_key in destination_index
                    if state is not None and state.row_hash == digest and destination_has_row:
                        _event(
                            db=db,
                            run_id=run.id,
                            user_id=selected.user_id,
                            spreadsheet_id=selected.spreadsheet_id,
                            tab_name=tab_name,
                            source_row_key=source_key,
                            row_number=idx,
                            action="NO_CHANGE",
                            status="SUCCESS",
                            message="No change",
                        )
                        _mark_source_row_status(
                            user_id=selected.user_id,
                            spreadsheet_id=selected.spreadsheet_id,
                            tab_name=tab_name,
                            source_headers=source_headers,
                            row_number=idx,
                            status_value="SUCCESS",
                            db=db,
                        )
                        continue

                    action = "UPDATE" if source_key in destination_index else "INSERT"
                    existing_destination_row = None
                    if action == "UPDATE":
                        existing_destination_row = dest_rows[destination_index[source_key] - 2]
                    dest_row = _build_destination_row(
                        source_values={
                            **source_values,
                            "First and Last name": source_business_values.get("name", ""),
                            "Team color": source_business_values.get("color", ""),
                            "Team Number": source_business_values.get("jersey", ""),
                            "Position Played": source_business_values.get("position", ""),
                            "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out": source_business_values.get(
                                "game_details", ""
                            ),
                            "Link to game": source_business_values.get("video", ""),
                            "Timestamp": source_business_values.get("timestamp", ""),
                            "Date": source_business_values.get("date", ""),
                        },
                        source_row_key=source_key,
                        row_hash=digest,
                        spreadsheet_id=selected.spreadsheet_id,
                        tab_name=tab_name,
                        row_number=idx,
                        action=action,
                        destination_headers=destination_headers,
                        existing_row=existing_destination_row,
                    )

                    if action == "INSERT":
                        resp = destination.append_row(dest_row, sheet_name=target_sheet_name)
                        run.rows_inserted += 1
                    else:
                        dest_row_number = destination_index[source_key]
                        resp = destination.update_row(dest_row_number, dest_row, sheet_name=target_sheet_name)
                        run.rows_updated += 1

                    if state is None:
                        state = SheetSyncRowState(
                            user_id=selected.user_id,
                            spreadsheet_id=selected.spreadsheet_id,
                            tab_name=tab_name,
                            row_number=idx,
                            source_row_key=source_key,
                            row_hash=digest,
                            status="SYNCED",
                            destination_row_number=destination_index.get(source_key),
                            last_synced_at=datetime.now(UTC),
                            attempt_count=1,
                        )
                        db.add(state)
                    else:
                        state.row_hash = digest
                        state.status = "SYNCED"
                        state.last_synced_at = datetime.now(UTC)
                        state.last_error = None
                        state.attempt_count += 1
                    _clear_skip_logs_for_row(db=db, user_id=selected.user_id, source_row_key=source_key)

                    _event(
                        db=db,
                        run_id=run.id,
                        user_id=selected.user_id,
                        spreadsheet_id=selected.spreadsheet_id,
                        tab_name=tab_name,
                        source_row_key=source_key,
                        row_number=idx,
                        action=action,
                        status="SUCCESS",
                        message="Synced",
                        payload_snapshot={
                            "hash_payload": payload_for_hash,
                            "changed_fields": _destination_field_changes(
                                existing_row=existing_destination_row,
                                destination_headers=destination_headers,
                                new_row=dest_row,
                            ),
                        },
                        destination_response=resp,
                    )
                    _mark_source_row_status(
                        user_id=selected.user_id,
                        spreadsheet_id=selected.spreadsheet_id,
                        tab_name=tab_name,
                        source_headers=source_headers,
                        row_number=idx,
                        status_value="SUCCESS",
                        db=db,
                    )
                    total_rows_processed += 1
                except Exception as row_exc:  # noqa: BLE001
                    run.rows_failed += 1
                    _event(
                        db=db,
                        run_id=run.id,
                        user_id=selected.user_id,
                        spreadsheet_id=selected.spreadsheet_id,
                        tab_name=tab_name,
                        source_row_key=source_key,
                        row_number=idx,
                        action="ROW_SYNC",
                        status="FAILED",
                        message=str(row_exc),
                    )
                    try:
                        _mark_source_row_failed(
                            user_id=selected.user_id,
                            spreadsheet_id=selected.spreadsheet_id,
                            tab_name=tab_name,
                            source_headers=source_headers,
                            row_number=idx,
                            db=db,
                        )
                    except Exception:  # noqa: BLE001
                        pass

            if SYNC_RECONCILE_DESTINATION:
                stale_keys = set(destination_index.keys()) - source_keys_seen
                if stale_keys:
                    stale_entries = sorted(
                        [(key, destination_index[key]) for key in stale_keys],
                        key=lambda item: item[1],
                        reverse=True,
                    )
                    info(
                        "sync_destination_reconcile_detected_stale_rows",
                        user_id=selected.user_id,
                        spreadsheet_id=selected.spreadsheet_id,
                        tab_name=run.tab_name,
                        stale_rows=len(stale_entries),
                    )
                    try:
                        delete_result = destination.delete_rows(
                            [row_number for _, row_number in stale_entries],
                            sheet_name=target_sheet_name,
                        )
                        for stale_key, row_number in stale_entries:
                            _event(
                                db=db,
                                run_id=run.id,
                                user_id=selected.user_id,
                                spreadsheet_id=selected.spreadsheet_id,
                                tab_name=tab_name,
                                source_row_key=stale_key,
                                row_number=row_number,
                                action="RECONCILE_DELETE",
                                status="SUCCESS",
                                message="Deleted stale destination row.",
                                destination_response=delete_result,
                            )
                    except Exception as reconcile_exc:  # noqa: BLE001
                        run.rows_failed += len(stale_entries)
                        _event(
                            db=db,
                            run_id=run.id,
                            user_id=selected.user_id,
                            spreadsheet_id=selected.spreadsheet_id,
                            tab_name=tab_name,
                            source_row_key=f"{selected.spreadsheet_id}:{tab_name}:stale:*",
                            row_number=0,
                            action="RECONCILE_DELETE",
                            status="FAILED",
                            message=f"Failed to delete stale destination rows: {reconcile_exc}",
                        )
            run.status = "FAILED" if run.rows_failed > 0 else "SUCCESS"
            run.completed_at = datetime.now(UTC)
            db.commit()
        except Exception as exc:  # noqa: BLE001
            run.status = "FAILED"
            run.error_message = str(exc)
            if "tab_name" in locals() and "source_headers" in locals() and "rows" in locals():
                for idx in range(2, len(rows) + 2):
                    try:
                        _mark_source_row_failed(
                            user_id=selected.user_id,
                            spreadsheet_id=selected.spreadsheet_id,
                            tab_name=tab_name,
                            source_headers=source_headers,
                            row_number=idx,
                            db=db,
                        )
                    except Exception:  # noqa: BLE001
                        pass
            _event(
                db=db,
                run_id=run.id,
                user_id=selected.user_id,
                spreadsheet_id=selected.spreadsheet_id,
                tab_name=run.tab_name or "unknown",
                source_row_key=f"{selected.spreadsheet_id}:{run.tab_name or 'unknown'}:*",
                row_number=0,
                action="RUN_SYNC",
                status="FAILED",
                message=str(exc),
            )
            run.completed_at = datetime.now(UTC)
            db.commit()
            error("sheet_sync_run_failed", run_id=run.id, error=str(exc))
        finally:
            _release_user_lock(lock_client, lock_meta)

    return {"runs": run_count, "rows": total_rows_processed}
