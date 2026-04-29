from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from app.services.orchestrator import SheetWorkflowOrchestrator


def _event_payload() -> dict:
    return {
        "event_id": str(uuid4()),
        "trace_id": str(uuid4()),
        "event_type": "sheet.row.updated",
        "source": {
            "provider": "google_sheets",
            "spreadsheet_id": "sheet",
            "sheet_name": "Source",
            "row_number": 2,
            "row_version": datetime.now(UTC).isoformat(),
        },
        "record": {
            "name": "Player A",
            "color": "Red",
            "jerseyNumber": "11",
            "position": "Forward",
            "gameInstructions": "Player came in during second half",
            "videoLink": "https://example.com/video.mp4",
            "typeOfVideo": "Match",
        },
    }


@pytest.mark.asyncio
async def test_happy_path_syncs(monkeypatch: pytest.MonkeyPatch) -> None:
    orchestrator = SheetWorkflowOrchestrator()
    payload = _event_payload()

    async def _mock_route(event, test_mode=True):  # noqa: ANN001
        return {"ok": True, "status_code": 200, "body": {"success": True}}

    monkeypatch.setattr(orchestrator.destination, "route_to_destination", _mock_route)
    result = await orchestrator.run_end_to_end(payload, test_mode=True)
    assert result.status.value == "SYNCED"


@pytest.mark.asyncio
async def test_duplicate_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    orchestrator = SheetWorkflowOrchestrator()
    payload = _event_payload()

    async def _mock_route(event, test_mode=True):  # noqa: ANN001
        return {"ok": True, "status_code": 200, "body": {"success": True}}

    monkeypatch.setattr(orchestrator.destination, "route_to_destination", _mock_route)
    first = await orchestrator.run_end_to_end(payload)
    second = await orchestrator.run_end_to_end(payload)
    assert first.status.value == "SYNCED"
    assert second.status.value == "SYNCED"


@pytest.mark.asyncio
async def test_validation_failure() -> None:
    orchestrator = SheetWorkflowOrchestrator()
    payload = _event_payload()
    del payload["record"]["name"]
    with pytest.raises(Exception):
        await orchestrator.run_end_to_end(payload)


@pytest.mark.asyncio
async def test_source_row_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    orchestrator = SheetWorkflowOrchestrator()
    payload = _event_payload()
    payload.pop("record")
    payload["source_row"] = {
        "First and Last name": "Mapped Player",
        "Team color": "White",
        "Team Number": "7",
        "Position Played": "CM",
        "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out": "Subbed in at 1:06:33",
        "Link to game": "https://example.com/video.mp4",
        "Type of Video": "Match",
        "Confirm you want this video edited": "Confirm",
    }

    async def _mock_route(event, test_mode=True):  # noqa: ANN001
        assert event.record.name == "Mapped Player"
        assert event.record.color == "White"
        assert event.record.jerseyNumber == "7"
        assert event.record.gameInstructions == "Subbed in at 1:06:33"
        return {"ok": True, "status_code": 200, "body": {"success": True}}

    monkeypatch.setattr(orchestrator.destination, "route_to_destination", _mock_route)
    result = await orchestrator.run_end_to_end(payload, test_mode=True)
    assert result.status.value == "SYNCED"
