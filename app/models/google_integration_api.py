from __future__ import annotations

from pydantic import BaseModel


class SelectSheetRequest(BaseModel):
    spreadsheet_id: str
    spreadsheet_name: str


class UserSyncSettingsRequest(BaseModel):
    polling_interval_seconds: int | None = None
    polling_interval_minutes: int | None = None
    sync_enabled: bool | None = None
