from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, status
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy.orm import Session

from app.core.logger import error, info
from app.models.google_oauth import GoogleOAuthToken
from app.services.google_oauth import SCOPES


def _has_required_scopes(granted: list[str], required: list[str]) -> bool:
    granted_set = set(granted)
    return all(scope in granted_set for scope in required)


def _user_credentials(user_id: str, db: Session) -> Credentials:
    token_row = db.query(GoogleOAuthToken).filter(GoogleOAuthToken.user_id == user_id).one_or_none()
    if token_row is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Google account not connected.")

    granted_scopes = token_row.scopes.split() if token_row.scopes else []
    if not _has_required_scopes(granted_scopes, SCOPES):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Google permissions are incomplete. Please reconnect Google to grant required scopes.",
        )

    creds = Credentials(
        token=token_row.access_token,
        refresh_token=token_row.refresh_token,
        token_uri=token_row.token_uri,
        client_id=os.getenv("GOOGLE_CLIENT_ID"),
        client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
        scopes=granted_scopes or SCOPES,
    )

    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            token_row.access_token = creds.token
            if creds.refresh_token:
                token_row.refresh_token = creds.refresh_token
            token_row.expiry = creds.expiry
            token_row.updated_at = datetime.now(UTC)
            db.commit()
            info("google_token_refreshed", user_id=user_id)
        except Exception as exc:  # noqa: BLE001
            error("google_token_refresh_failed", user_id=user_id, error=str(exc))
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Google token expired or revoked. Reconnect Google account.",
            ) from exc

    return creds


def list_user_sheets(user_id: str, db: Session) -> list[dict[str, Any]]:
    creds = _user_credentials(user_id, db)
    try:
        drive = build("drive", "v3", credentials=creds)
        resp = (
            drive.files()
            .list(
                q="mimeType='application/vnd.google-apps.spreadsheet' and trashed=false",
                fields="files(id,name,modifiedTime)",
                pageSize=100,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )
        return resp.get("files", [])
    except HttpError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Google Drive access failed: {exc.reason}") from exc


def list_spreadsheet_tabs(user_id: str, spreadsheet_id: str, db: Session) -> list[str]:
    creds = _user_credentials(user_id, db)
    try:
        sheets = build("sheets", "v4", credentials=creds)
        resp = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
        tabs = []
        for sheet in resp.get("sheets", []):
            title = sheet.get("properties", {}).get("title")
            if title:
                tabs.append(title)
        return tabs
    except HttpError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Google Sheets tabs fetch failed: {exc.reason}",
        ) from exc


def read_sheet(user_id: str, spreadsheet_id: str, range_name: str, db: Session) -> dict[str, Any]:
    creds = _user_credentials(user_id, db)
    try:
        sheets = build("sheets", "v4", credentials=creds)
        resp = sheets.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        return {
            "range": resp.get("range", range_name),
            "majorDimension": resp.get("majorDimension", "ROWS"),
            "values": resp.get("values", []),
        }
    except HttpError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Google Sheets read failed: {exc.reason}") from exc


def update_sheet(user_id: str, spreadsheet_id: str, range_name: str, values: list[list[Any]], db: Session) -> dict[str, Any]:
    creds = _user_credentials(user_id, db)
    try:
        sheets = build("sheets", "v4", credentials=creds)
        resp = (
            sheets.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="USER_ENTERED",
                body={"values": values},
            )
            .execute()
        )
        return {
            "updatedRange": resp.get("updatedRange"),
            "updatedRows": resp.get("updatedRows"),
            "updatedColumns": resp.get("updatedColumns"),
            "updatedCells": resp.get("updatedCells"),
        }
    except HttpError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Google Sheets update failed: {exc.reason}") from exc
