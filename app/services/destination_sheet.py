from __future__ import annotations

import os
import re
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

from app.core.logger import info


class DestinationSheetService:
    def __init__(self) -> None:
        credentials_file = os.getenv("DESTINATION_GOOGLE_CREDENTIALS_FILE", "credentials.json")
        self._spreadsheet_id = os.getenv("DESTINATION_SPREADSHEET_ID", "")
        self._sheet_name = os.getenv("DESTINATION_SHEET_NAME", "Sheet1")
        self._user_sheet_prefix = os.getenv("DESTINATION_USER_SHEET_PREFIX", "user")
        self._enabled = bool(self._spreadsheet_id)
        if self._enabled:
            creds = service_account.Credentials.from_service_account_file(
                credentials_file,
                scopes=["https://www.googleapis.com/auth/spreadsheets"],
            )
            self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        else:
            self._service = None
        info(
            "destination_sheet_service_config",
            enabled=self._enabled,
            destination_sheet_name=self._sheet_name,
            spreadsheet_id_set=bool(self._spreadsheet_id),
            destination_user_sheet_prefix=self._user_sheet_prefix,
        )

    def is_enabled(self) -> bool:
        return self._enabled and self._service is not None

    def _normalize_user_segment(self, value: str) -> str:
        sanitized = re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_")
        return sanitized[:80] if sanitized else "anonymous"

    def user_sheet_name(self, user_id: str) -> str:
        return f"{self._user_sheet_prefix}_{self._normalize_user_segment(user_id)}"

    def _sheet_titles(self) -> list[str]:
        if not self.is_enabled():
            return []
        resp = self._service.spreadsheets().get(spreadsheetId=self._spreadsheet_id, fields="sheets(properties(title))").execute()
        return [
            s.get("properties", {}).get("title", "")
            for s in resp.get("sheets", [])
            if s.get("properties", {}).get("title")
        ]

    def ensure_sheet_exists(self, sheet_name: str) -> None:
        if not self.is_enabled():
            return
        if sheet_name in self._sheet_titles():
            return
        self._service.spreadsheets().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
        ).execute()

    def load_headers_and_rows(
        self,
        *,
        sheet_name: str | None = None,
        ensure_sheet: bool = False,
        initialize_headers: list[str] | None = None,
    ) -> tuple[list[str], list[list[str]]]:
        if not self.is_enabled():
            return [], []
        target_sheet = sheet_name or self._sheet_name
        if ensure_sheet:
            self.ensure_sheet_exists(target_sheet)
        resp = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=self._spreadsheet_id, range=f"{target_sheet}!A1:ZZ")
            .execute()
        )
        values = resp.get("values", [])
        if not values:
            if initialize_headers:
                self._service.spreadsheets().values().update(
                    spreadsheetId=self._spreadsheet_id,
                    range=f"{target_sheet}!A1",
                    valueInputOption="USER_ENTERED",
                    body={"values": [initialize_headers]},
                ).execute()
                return [str(v) for v in initialize_headers], []
            return [], []
        return [str(v) for v in values[0]], values[1:]

    def append_row(self, row: list[str], *, sheet_name: str | None = None) -> dict[str, Any]:
        if not self.is_enabled():
            return {"mode": "disabled"}
        target_sheet = sheet_name or self._sheet_name
        resp = (
            self._service.spreadsheets()
            .values()
            .append(
                spreadsheetId=self._spreadsheet_id,
                range=f"{target_sheet}!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": [row]},
            )
            .execute()
        )
        return resp

    def update_row(self, row_number: int, row: list[str], *, sheet_name: str | None = None) -> dict[str, Any]:
        if not self.is_enabled():
            return {"mode": "disabled"}
        target_sheet = sheet_name or self._sheet_name
        resp = (
            self._service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self._spreadsheet_id,
                range=f"{target_sheet}!A{row_number}",
                valueInputOption="USER_ENTERED",
                body={"values": [row]},
            )
            .execute()
        )
        return resp

    def overwrite_values(self, values: list[list[str]], *, sheet_name: str | None = None) -> dict[str, Any]:
        if not self.is_enabled():
            return {"mode": "disabled"}
        target_sheet = sheet_name or self._sheet_name
        resp = (
            self._service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self._spreadsheet_id,
                range=f"{target_sheet}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": values},
            )
            .execute()
        )
        return resp
