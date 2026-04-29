from __future__ import annotations

from enum import Enum
from hashlib import sha256
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, HttpUrl


class WorkflowStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SYNCED = "SYNCED"
    FAILED = "FAILED"


class SourceMetadata(BaseModel):
    provider: str = Field(default="google_sheets")
    spreadsheet_id: str
    sheet_name: str
    row_number: int = Field(gt=0)
    row_version: str


class NormalizedRecord(BaseModel):
    name: str
    color: str
    jerseyNumber: str
    position: str
    videoLink: HttpUrl
    gameInstructions: str | None = None
    userId: str | None = None
    userEmail: EmailStr | None = None
    typeOfVideo: str | None = None
    club_id: str | None = None


class SheetEvent(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    event_id: UUID
    trace_id: UUID
    event_type: str = Field(pattern=r"^sheet\.row\.(created|updated)$")
    source: SourceMetadata
    record: NormalizedRecord


class CanonicalEvent(SheetEvent):
    idempotency_key: str


class JobStatus(BaseModel):
    trace_id: UUID
    event_id: UUID
    status: WorkflowStatus
    feedback_link: str | None = None
    attempts: int = 0
    last_error: str | None = None


def compute_idempotency_key(source: SourceMetadata) -> str:
    raw = f"{source.spreadsheet_id}:{source.sheet_name}:{source.row_number}:{source.row_version}"
    return sha256(raw.encode("utf-8")).hexdigest()
