from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


async def with_exponential_backoff(
    operation: Callable[[], Awaitable[T]], max_attempts: int = 3, base_delay_ms: int = 400
) -> T:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await operation()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == max_attempts:
                break
            await asyncio.sleep((base_delay_ms * (2 ** (attempt - 1))) / 1000)
    if last_error is None:
        raise RuntimeError("Retry operation failed without captured exception")
    raise last_error
