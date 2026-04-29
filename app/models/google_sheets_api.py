from __future__ import annotations

from pydantic import BaseModel, Field


class SheetUpdateRequest(BaseModel):
    range: str = Field(default="Sheet1")
    values: list[list[str | int | float | bool | None]]
