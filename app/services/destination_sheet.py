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
            spreadsheet_id_set=bool(self._spreadsheet_id),
        )

    def is_enabled(self) -> bool:
        return self._enabled and self._service is not None

    def _normalize_email_local_part(self, email: str) -> str:
        local_part = (email or "").split("@", 1)[0].strip().lower()
        # Keep only letters and numbers in the destination sheet title.
        sanitized = re.sub(r"[^a-z0-9]+", "", local_part)
        return sanitized[:80] if sanitized else "anonymous"

    def user_sheet_name(self, user_email: str) -> str:
        return self._normalize_email_local_part(user_email)

    def _sheet_titles(self) -> list[str]:
        if not self.is_enabled():
            return []
        resp = self._service.spreadsheets().get(spreadsheetId=self._spreadsheet_id, fields="sheets(properties(title))").execute()
        return [
            s.get("properties", {}).get("title", "")
            for s in resp.get("sheets", [])
            if s.get("properties", {}).get("title")
        ]

    def _sheet_id(self, sheet_name: str) -> int:
        if not self.is_enabled():
            raise RuntimeError("Destination sheet service is not enabled.")
        resp = self._service.spreadsheets().get(
            spreadsheetId=self._spreadsheet_id,
            fields="sheets(properties(sheetId,title))",
        ).execute()
        for sheet in resp.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("title") == sheet_name and props.get("sheetId") is not None:
                return int(props["sheetId"])
        raise ValueError(f"Sheet '{sheet_name}' not found in destination spreadsheet.")

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
        sheet_name: str,
        ensure_sheet: bool = False,
        initialize_headers: list[str] | None = None,
    ) -> tuple[list[str], list[list[str]]]:
        if not self.is_enabled():
            return [], []
        if ensure_sheet:
            self.ensure_sheet_exists(sheet_name)
        resp = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=self._spreadsheet_id, range=f"{sheet_name}!A1:ZZ")
            .execute()
        )
        values = resp.get("values", [])
        if not values:
            if initialize_headers:
                self._service.spreadsheets().values().update(
                    spreadsheetId=self._spreadsheet_id,
                    range=f"{sheet_name}!A1",
                    valueInputOption="USER_ENTERED",
                    body={"values": [initialize_headers]},
                ).execute()
                return [str(v) for v in initialize_headers], []
            return [], []
        return [str(v) for v in values[0]], values[1:]

    def append_row(self, row: list[str], *, sheet_name: str) -> dict[str, Any]:
        if not self.is_enabled():
            return {"mode": "disabled"}
        resp = (
            self._service.spreadsheets()
            .values()
            .append(
                spreadsheetId=self._spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": [row]},
            )
            .execute()
        )
        return resp

    def update_row(self, row_number: int, row: list[str], *, sheet_name: str) -> dict[str, Any]:
        if not self.is_enabled():
            return {"mode": "disabled"}
        resp = (
            self._service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self._spreadsheet_id,
                range=f"{sheet_name}!A{row_number}",
                valueInputOption="USER_ENTERED",
                body={"values": [row]},
            )
            .execute()
        )
        return resp

    def overwrite_values(self, values: list[list[str]], *, sheet_name: str) -> dict[str, Any]:
        if not self.is_enabled():
            return {"mode": "disabled"}
        resp = (
            self._service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self._spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": values},
            )
            .execute()
        )
        return resp

    def delete_rows(self, row_numbers: list[int], *, sheet_name: str) -> dict[str, Any]:
        if not self.is_enabled():
            return {"mode": "disabled", "deleted_rows": []}
        if not row_numbers:
            return {"deleted_rows": []}
        sheet_id = self._sheet_id(sheet_name)
        unique_desc_rows = sorted({row for row in row_numbers if row > 1}, reverse=True)
        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": row_number - 1,
                        "endIndex": row_number,
                    }
                }
            }
            for row_number in unique_desc_rows
        ]
        if not requests:
            return {"deleted_rows": []}
        self._service.spreadsheets().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body={"requests": requests},
        ).execute()
        return {"deleted_rows": unique_desc_rows}
