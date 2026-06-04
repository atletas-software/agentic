from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from backendapi.models.auth import UserAccount
from backendapi.models.workspace import Workspace, WorkspaceContextItem
from backendapi.services.destination_sheet import DestinationSheetService


def ensure_workspace(*, user_id: str, db: Session) -> Workspace:
    row = db.query(Workspace).filter(Workspace.user_id == user_id).one_or_none()
    if row is not None:
        row.updated_at = datetime.now(UTC)
        db.commit()
        db.refresh(row)
        return row
    ws = Workspace(user_id=user_id)
    db.add(ws)
    db.commit()
    db.refresh(ws)
    return ws


def build_destination_snapshot_payload(*, user_id: str, db: Session) -> dict | None:
    """Read the user's destination sub-sheet only (not source). Returns None if unavailable."""
    try:
        uid_int = int(user_id)
    except ValueError:
        return None
    user = db.get(UserAccount, uid_int)
    if user is None:
        return None
    dest = DestinationSheetService()
    if not dest.is_enabled():
        return None
    sheet_name = dest.user_sheet_name(user.email)
    headers, rows = dest.load_headers_and_rows(sheet_name=sheet_name, ensure_sheet=False)
    max_rows = int(os.getenv("WORKSPACE_SNAPSHOT_MAX_ROWS", "500"))
    trimmed = rows[:max_rows] if rows else []
    return {
        "sheet_name": sheet_name,
        "user_email": user.email,
        "headers": headers,
        "rows": trimmed,
        "row_count": len(rows) if rows else 0,
        "captured_at": datetime.now(UTC).isoformat(),
    }


def snapshot_content_hash(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def append_destination_snapshot_if_changed(*, workspace_id: int, payload: dict, db: Session) -> bool:
    """Returns True if a new context row was inserted."""
    h = snapshot_content_hash(payload)
    latest = (
        db.query(WorkspaceContextItem)
        .filter(
            WorkspaceContextItem.workspace_id == workspace_id,
            WorkspaceContextItem.item_type == "destination_snapshot",
        )
        .order_by(WorkspaceContextItem.created_at.desc())
        .first()
    )
    if latest is not None and latest.content_hash == h:
        return False
    db.add(
        WorkspaceContextItem(
            workspace_id=workspace_id,
            item_type="destination_snapshot",
            content_hash=h,
            payload_json=json.dumps(payload, ensure_ascii=True, default=str),
        )
    )
    db.commit()
    return True
