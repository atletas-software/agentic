from __future__ import annotations

from sqlalchemy.orm import Session

from app.models.google_oauth import UserGoogleSheetConnection, UserGoogleSheetSelection, UserSyncSetting


def resolve_polling_interval_seconds(raw_value: int | None) -> int:
    if raw_value is None:
        return 60
    # Backward compatibility: older values were minutes (1..60).
    if raw_value < 30:
        return raw_value * 60
    return raw_value


def set_selected_sheet(user_id: str, spreadsheet_id: str, spreadsheet_name: str, db: Session) -> UserGoogleSheetSelection:
    row = db.query(UserGoogleSheetSelection).filter(UserGoogleSheetSelection.user_id == user_id).one_or_none()
    if row is None:
        row = UserGoogleSheetSelection(
            user_id=user_id,
            spreadsheet_id=spreadsheet_id,
            spreadsheet_name=spreadsheet_name,
        )
        db.add(row)
    else:
        row.spreadsheet_id = spreadsheet_id
        row.spreadsheet_name = spreadsheet_name
    db.commit()
    db.refresh(row)
    return row


def get_selected_sheet(user_id: str, db: Session) -> UserGoogleSheetSelection | None:
    return db.query(UserGoogleSheetSelection).filter(UserGoogleSheetSelection.user_id == user_id).one_or_none()


def save_sheet(user_id: str, spreadsheet_id: str, spreadsheet_name: str, db: Session) -> UserGoogleSheetConnection:
    row = (
        db.query(UserGoogleSheetConnection)
        .filter(
            UserGoogleSheetConnection.user_id == user_id,
            UserGoogleSheetConnection.spreadsheet_id == spreadsheet_id,
        )
        .one_or_none()
    )
    if row is None:
        row = UserGoogleSheetConnection(
            user_id=user_id,
            spreadsheet_id=spreadsheet_id,
            spreadsheet_name=spreadsheet_name,
            is_active=False,
        )
        db.add(row)
    else:
        row.spreadsheet_name = spreadsheet_name
    db.commit()
    db.refresh(row)
    return row


def list_saved_sheets(user_id: str, db: Session) -> list[UserGoogleSheetConnection]:
    return (
        db.query(UserGoogleSheetConnection)
        .filter(UserGoogleSheetConnection.user_id == user_id)
        .order_by(UserGoogleSheetConnection.updated_at.desc())
        .all()
    )


def get_saved_sheet(user_id: str, spreadsheet_id: str, db: Session) -> UserGoogleSheetConnection | None:
    return (
        db.query(UserGoogleSheetConnection)
        .filter(
            UserGoogleSheetConnection.user_id == user_id,
            UserGoogleSheetConnection.spreadsheet_id == spreadsheet_id,
        )
        .one_or_none()
    )


def set_active_sheet(user_id: str, spreadsheet_id: str, db: Session) -> UserGoogleSheetConnection | None:
    target = get_saved_sheet(user_id=user_id, spreadsheet_id=spreadsheet_id, db=db)
    if target is None:
        return None
    (
        db.query(UserGoogleSheetConnection)
        .filter(UserGoogleSheetConnection.user_id == user_id, UserGoogleSheetConnection.is_active.is_(True))
        .update({"is_active": False})
    )
    target.is_active = True
    db.commit()
    db.refresh(target)
    return target


def get_user_sync_settings(user_id: str, db: Session) -> UserSyncSetting:
    row = db.query(UserSyncSetting).filter(UserSyncSetting.user_id == user_id).one_or_none()
    if row is None:
        row = UserSyncSetting(user_id=user_id, polling_interval_minutes=60, sync_enabled=False)
        db.add(row)
        db.commit()
        db.refresh(row)
    return row


def set_user_sync_settings(
    user_id: str,
    polling_interval_minutes: int | None,
    db: Session,
    sync_enabled: bool | None = None,
) -> UserSyncSetting:
    row = get_user_sync_settings(user_id=user_id, db=db)
    if polling_interval_minutes is not None:
        row.polling_interval_minutes = polling_interval_minutes
    if sync_enabled is not None:
        row.sync_enabled = sync_enabled
    db.commit()
    db.refresh(row)
    return row
