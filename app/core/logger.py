from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger("sheet_mcp")
logging.basicConfig(level=logging.INFO)


def info(message: str, **context: Any) -> None:
    logger.info(json.dumps({"level": "info", "message": message, **context}, default=str))


def error(message: str, **context: Any) -> None:
    logger.error(json.dumps({"level": "error", "message": message, **context}, default=str))


def mask_value(value: str | None, visible: int = 3) -> str:
    if value is None:
        return "<unset>"
    if value == "":
        return "<empty>"
    if len(value) <= visible:
        return "*" * len(value)
    return f"{value[:visible]}{'*' * (len(value) - visible)}"
