from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from app.core.logger import error, info
from app.models.google_oauth import (
    SheetSyncEvent,
    SheetSyncRowState,
    SheetSyncRun,
    UserGoogleSheetConnection,
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


def _normalize_row(headers: list[str], row: list[Any]) -> dict[str, str]:
    values: dict[str, str] = {}
    for i, h in enumerate(headers):
        values[h] = str(row[i]).strip() if i < len(row) and row[i] is not None else ""
    return values


def _normalize_header_key(value: str) -> str:
    # Collapse newlines and extra spaces so "Game Details -\nLOG IN..." still matches.
    compact = re.sub(r"\s+", " ", (value or "").strip())
    return compact.lower()


def _resolve_source_header_aliases(source_headers: list[str]) -> tuple[dict[str, str], list[str]]:
    normalized_to_original = {_normalize_header_key(h): h for h in source_headers}
    resolved: dict[str, str] = {}
    missing: list[str] = []
    for required in REQUIRED_SOURCE_HEADERS:
        normalized_required = _normalize_header_key(required)
        exact = normalized_to_original.get(normalized_required)
        if exact is not None:
            resolved[required] = exact
            continue
        # Fallback for headers that can vary by line breaks/spaces around "Game Details".
        if required.lower().startswith("game details"):
            candidate = next(
                (
                    original
                    for norm, original in normalized_to_original.items()
                    if "game details" in norm and "log in info for the site" in norm
                ),
                None,
            )
            if candidate is not None:
                resolved[required] = candidate
                continue
        missing.append(required)
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
    active_user_rows = (
        db.query(UserGoogleSheetConnection.user_id)
        .filter(UserGoogleSheetConnection.is_active.is_(True))
        .distinct()
        .all()
    )
    return run_sync_once_for_users(db=db, user_ids=[row[0] for row in active_user_rows])


def get_due_user_ids_for_sync(db: Session, now: datetime | None = None) -> list[str]:
    current = now or datetime.now(UTC)
    active_user_rows = (
        db.query(UserGoogleSheetConnection.user_id)
        .filter(UserGoogleSheetConnection.is_active.is_(True))
        .distinct()
        .all()
    )
    active_user_ids = [row[0] for row in active_user_rows]
    due_user_ids: list[str] = []
    for user_id in active_user_ids:
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

    active_sheets = (
        db.query(UserGoogleSheetConnection)
        .filter(UserGoogleSheetConnection.is_active.is_(True), UserGoogleSheetConnection.user_id.in_(user_ids))
        .all()
    )
    total_rows_processed = 0
    run_count = 0

    for active in active_sheets:
        try:
            user_id_int = int(active.user_id)
        except ValueError:
            info("sync_user_skipped_invalid_user_id", user_id=active.user_id)
            continue
        user_account = db.query(UserAccount).filter(UserAccount.id == user_id_int, UserAccount.is_active.is_(True)).one_or_none()
        if user_account is None:
            info("sync_user_skipped_not_authenticated", user_id=active.user_id)
            continue
        setting = db.query(UserSyncSetting).filter(UserSyncSetting.user_id == active.user_id).one_or_none()
        if setting is None or not setting.sync_enabled:
            info("sync_user_skipped_disabled", user_id=active.user_id)
            continue
        target_sheet_name = destination.user_sheet_name(active.user_id)
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
                user_id=active.user_id,
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
            user_id=active.user_id,
            spreadsheet_id=active.spreadsheet_id,
            tab_name="",
            status="RUNNING",
        )
        db.add(run)
        db.commit()
        db.refresh(run)
        run_count += 1

        try:
            read = read_sheet(
                user_id=active.user_id,
                spreadsheet_id=active.spreadsheet_id,
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
                run.status = "FAILED"
                run.error_message = f"Missing required columns: {', '.join(missing)}"
                for idx in range(2, len(rows) + 2):
                    try:
                        _mark_source_row_failed(
                            user_id=active.user_id,
                            spreadsheet_id=active.spreadsheet_id,
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
                    user_id=active.user_id,
                    spreadsheet_id=active.spreadsheet_id,
                    tab_name=tab_name,
                    source_row_key=f"{active.spreadsheet_id}:{tab_name}:*",
                    row_number=0,
                    action="VALIDATE",
                    status="FAILED",
                    message=run.error_message,
                )
                run.completed_at = datetime.now(UTC)
                db.commit()
                continue

            for idx, row in enumerate(rows, start=2):
                source_key = _source_row_key(active.spreadsheet_id, tab_name, idx)
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
                            user_id=active.user_id,
                            spreadsheet_id=active.spreadsheet_id,
                            tab_name=tab_name,
                            source_row_key=source_key,
                            row_number=idx,
                            message=f"Skipped incomplete row. Missing values: {', '.join(missing_row_fields)}",
                        )
                        continue
                    payload_for_hash = {
                        "name": source_values.get(source_name_col, ""),
                        "color": source_values.get(source_color_col, ""),
                        "jersey": source_values.get(source_jersey_col, ""),
                        "position": source_values.get(source_position_col, ""),
                        "game_details": source_values.get(source_game_details_col, ""),
                        "video": source_values.get(source_link_col, ""),
                        "type": source_values.get("Type of Video", ""),
                        "user_id": source_values.get("User ID", ""),
                        "user_email": source_values.get("User Email", ""),
                        "timestamp": source_values.get("Timestamp", ""),
                        "date": source_values.get("Date", ""),
                    }
                    digest = _row_hash(payload_for_hash)

                    state = (
                        db.query(SheetSyncRowState)
                        .filter(
                            SheetSyncRowState.user_id == active.user_id,
                            SheetSyncRowState.spreadsheet_id == active.spreadsheet_id,
                            SheetSyncRowState.tab_name == tab_name,
                            SheetSyncRowState.row_number == idx,
                        )
                        .one_or_none()
                    )

                    destination_has_row = source_key in destination_index
                    if state is not None and state.row_hash == digest and destination_has_row:
                        _event(
                            db=db,
                            run_id=run.id,
                            user_id=active.user_id,
                            spreadsheet_id=active.spreadsheet_id,
                            tab_name=tab_name,
                            source_row_key=source_key,
                            row_number=idx,
                            action="NO_CHANGE",
                            status="SUCCESS",
                            message="No change",
                        )
                        _mark_source_row_status(
                            user_id=active.user_id,
                            spreadsheet_id=active.spreadsheet_id,
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
                            "First and Last name": source_values.get(source_name_col, ""),
                            "Team color": source_values.get(source_color_col, ""),
                            "Team Number": source_values.get(source_jersey_col, ""),
                            "Position Played": source_values.get(source_position_col, ""),
                            "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out": source_values.get(
                                source_game_details_col, ""
                            ),
                            "Link to game": source_values.get(source_link_col, ""),
                        },
                        source_row_key=source_key,
                        row_hash=digest,
                        spreadsheet_id=active.spreadsheet_id,
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
                            user_id=active.user_id,
                            spreadsheet_id=active.spreadsheet_id,
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
                    _clear_skip_logs_for_row(db=db, user_id=active.user_id, source_row_key=source_key)

                    _event(
                        db=db,
                        run_id=run.id,
                        user_id=active.user_id,
                        spreadsheet_id=active.spreadsheet_id,
                        tab_name=tab_name,
                        source_row_key=source_key,
                        row_number=idx,
                        action=action,
                        status="SUCCESS",
                        message="Synced",
                        payload_snapshot=payload_for_hash,
                        destination_response=resp,
                    )
                    _mark_source_row_status(
                        user_id=active.user_id,
                        spreadsheet_id=active.spreadsheet_id,
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
                        user_id=active.user_id,
                        spreadsheet_id=active.spreadsheet_id,
                        tab_name=tab_name,
                        source_row_key=source_key,
                        row_number=idx,
                        action="ROW_SYNC",
                        status="FAILED",
                        message=str(row_exc),
                    )
                    try:
                        _mark_source_row_failed(
                            user_id=active.user_id,
                            spreadsheet_id=active.spreadsheet_id,
                            tab_name=tab_name,
                            source_headers=source_headers,
                            row_number=idx,
                            db=db,
                        )
                    except Exception:  # noqa: BLE001
                        pass

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
                            user_id=active.user_id,
                            spreadsheet_id=active.spreadsheet_id,
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
                user_id=active.user_id,
                spreadsheet_id=active.spreadsheet_id,
                tab_name=run.tab_name or "unknown",
                source_row_key=f"{active.spreadsheet_id}:{run.tab_name or 'unknown'}:*",
                row_number=0,
                action="RUN_SYNC",
                status="FAILED",
                message=str(exc),
            )
            run.completed_at = datetime.now(UTC)
            db.commit()
            error("sheet_sync_run_failed", run_id=run.id, error=str(exc))

    return {"runs": run_count, "rows": total_rows_processed}
