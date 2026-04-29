from __future__ import annotations

import os
from typing import Any

from app.core.env_loader import ensure_env_loaded
from app.core.logger import info, mask_value


class GoogleSheetsAdapter:
    def __init__(self) -> None:
        ensure_env_loaded()
        self._enabled = os.getenv("ENABLE_SOURCE_STATUS_UPDATES", "false").lower() == "true"
        self._sheet_id = os.getenv("SOURCE_SHEET_ID", "")
        self._sheet_name = os.getenv("SOURCE_SHEET_NAME", "Sheet1")
        self._columns = {
            "status": os.getenv("SOURCE_STATUS_COLUMN", "Status"),
            "last_error": os.getenv("SOURCE_ERROR_COLUMN", "Last Error"),
            "attempt_count": os.getenv("SOURCE_ATTEMPTS_COLUMN", "Attempt Count"),
            "last_processed_at": os.getenv("SOURCE_LAST_PROCESSED_COLUMN", "Last Processed At"),
            "feedback_link": os.getenv("SOURCE_FEEDBACK_LINK_COLUMN", "Feedback Link"),
            "job_id": os.getenv("SOURCE_JOB_ID_COLUMN", "Job ID"),
        }
        info(
            "source_sheet_adapter_config",
            status_updates_enabled=self._enabled,
            source_sheet_id_masked=mask_value(self._sheet_id),
            source_sheet_name=self._sheet_name,
            credentials_file="<oauth_db_managed>",
            configured_status_columns=self._columns,
        )
        if self._enabled and self._sheet_id:
            info(
                "source_sheet_status_update_note",
                detail="Status writeback from orchestrator remains skipped in this build; use user OAuth Sheets service endpoints.",
            )
        self._service = None

    async def update_row_status(
        self,
        row_number: int,
        status: str,
        attempt_count: int = 0,
        last_error: str | None = None,
        feedback_link: str | None = None,
        job_id: str | None = None,
        processed_at: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "status": status,
            "attempt_count": attempt_count,
            "last_error": last_error or "",
            "feedback_link": feedback_link or "",
            "job_id": job_id or "",
            "last_processed_at": processed_at or "",
        }
        # Status updates disabled or sheet not configured.
        if self._service is None:
            info("sheet_status_skipped", row_number=row_number, payload=payload)
            return {"mode": "skipped", "row_number": row_number, "payload": payload}

        # Keep a simple metadata write by placing values in configured columns in row.
        headers_res = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=self._sheet_id, range=f"{self._sheet_name}!1:1")
            .execute()
        )
        headers = headers_res.get("values", [[]])[0]
        updates: list[dict[str, Any]] = []

        for idx, header in enumerate(headers):
            trimmed = str(header).strip()
            cell_range = f"{self._sheet_name}!{self._column_letter(idx + 1)}{row_number}"
            if trimmed == self._columns["status"]:
                updates.append({"range": cell_range, "values": [[status]]})
            elif trimmed == self._columns["last_error"]:
                updates.append({"range": cell_range, "values": [[payload["last_error"]]]})
            elif trimmed == self._columns["attempt_count"]:
                updates.append({"range": cell_range, "values": [[str(attempt_count)]]})
            elif trimmed == self._columns["last_processed_at"]:
                updates.append({"range": cell_range, "values": [[payload["last_processed_at"]]]})
            elif trimmed == self._columns["feedback_link"]:
                updates.append({"range": cell_range, "values": [[payload["feedback_link"]]]})
            elif trimmed == self._columns["job_id"]:
                updates.append({"range": cell_range, "values": [[payload["job_id"]]]})

        if not updates:
            info("sheet_status_columns_missing", row_number=row_number, payload=payload)
            return {"mode": "skipped", "row_number": row_number, "payload": payload}

        self._service.spreadsheets().values().batchUpdate(
            spreadsheetId=self._sheet_id,
            valueInputOption="USER_ENTERED",
            body={"data": updates},
        ).execute()
        info(
            "sheet_status_updated",
            row_number=row_number,
            update_count=len(updates),
            status=status,
        )

        return {"mode": "live", "row_number": row_number, "payload": payload}

    @staticmethod
    def _column_letter(index: int) -> str:
        result = ""
        while index > 0:
            index, remainder = divmod(index - 1, 26)
            result = chr(65 + remainder) + result
        return result
