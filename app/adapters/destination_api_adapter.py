from __future__ import annotations

import os
from typing import Any

import httpx

from app.core.env_loader import ensure_env_loaded
from app.core.logger import error, info, mask_value
from app.models.contracts import CanonicalEvent


class DestinationApiAdapter:
    def __init__(self) -> None:
        ensure_env_loaded()
        self._base_url = os.getenv("DESTINATION_API_URL", "https://sheet.athlete-focus.com/default")
        self._timeout_s = float(os.getenv("DESTINATION_API_TIMEOUT_SECONDS", "10"))
        self._default_user_id = os.getenv("DESTINATION_DEFAULT_USER_ID", "sheet-trigger")
        self._default_user_email = os.getenv("DESTINATION_DEFAULT_USER_EMAIL", "sheet-trigger@athlete-focus.local")
        info(
            "destination_adapter_config",
            destination_api_url=self._base_url,
            destination_timeout_seconds=self._timeout_s,
            default_user_id_masked=mask_value(self._default_user_id),
            default_user_email_masked=mask_value(self._default_user_email),
        )

    async def route_to_destination(self, event: CanonicalEvent, test_mode: bool = True) -> dict[str, Any]:
        path = "/test" if test_mode else ""
        payload = {
            "name": event.record.name,
            "color": event.record.color,
            "jerseyNumber": event.record.jerseyNumber,
            "position": event.record.position,
            "videoLink": str(event.record.videoLink),
            "gameInstructions": event.record.gameInstructions or "",
            "userId": event.record.userId or self._default_user_id,
            "userEmail": event.record.userEmail or self._default_user_email,
            "typeOfVideo": event.record.typeOfVideo,
        }
        headers = {"x-trace-id": str(event.trace_id)}
        info(
            "destination_api_request",
            trace_id=event.trace_id,
            event_id=event.event_id,
            url=f"{self._base_url}{path}",
            has_fallback_user=event.record.userId is None or event.record.userEmail is None,
        )
        async with httpx.AsyncClient(timeout=self._timeout_s) as client:
            resp = await client.post(f"{self._base_url}{path}", json=payload, headers=headers)
            data = resp.json() if resp.content else {}
            if resp.status_code >= 400:
                error(
                    "destination_api_response_error",
                    trace_id=event.trace_id,
                    event_id=event.event_id,
                    status_code=resp.status_code,
                    body=data,
                )
            else:
                info(
                    "destination_api_response_success",
                    trace_id=event.trace_id,
                    event_id=event.event_id,
                    status_code=resp.status_code,
                )
            return {
                "ok": resp.status_code < 400,
                "status_code": resp.status_code,
                "body": data,
            }
