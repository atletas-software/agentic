from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from uuid import uuid4

from fastapi import HTTPException

from app.adapters.destination_api_adapter import DestinationApiAdapter
from app.adapters.google_sheets_adapter import GoogleSheetsAdapter
from app.core.logger import error, info
from app.core.retry import with_exponential_backoff
from app.models.contracts import (
    CanonicalEvent,
    JobStatus,
    SheetEvent,
    WorkflowStatus,
    compute_idempotency_key,
)
from app.store.idempotency_store import IdempotencyStore
from app.store.job_store import JobStore
from app.workflow.video_pipeline import VideoPipeline


class SheetWorkflowOrchestrator:
    def __init__(self) -> None:
        self.idempotency = IdempotencyStore()
        self.jobs = JobStore()
        self.sheets = GoogleSheetsAdapter()
        self.destination = DestinationApiAdapter()
        self.pipeline = VideoPipeline()

    def ingest_sheet_event(self, payload: dict) -> SheetEvent:
        payload = self._normalize_payload(payload)
        event = SheetEvent.model_validate(payload)
        info(
            "ingest_sheet_event",
            trace_id=event.trace_id,
            event_id=event.event_id,
            event_type=event.event_type,
            row_number=event.source.row_number,
            sheet_name=event.source.sheet_name,
        )
        return event

    @staticmethod
    def _source_field_map() -> dict[str, str]:
        default_map = {
            "First and Last name": "name",
            "Team color": "color",
            "Team Number": "jerseyNumber",
            "Position Played": "position",
            "Game Details - LOG IN INFO FOR THE SITE When you were subbed in or out": "gameInstructions",
            "Link to game": "videoLink",
            "Type of Video": "typeOfVideo",
            "User ID": "userId",
            "User Email": "userEmail",
        }
        raw = os.getenv("SOURCE_FIELD_MAP", "").strip()
        if not raw:
            return default_map
        try:
            configured = json.loads(raw)
            if isinstance(configured, dict):
                return {str(k): str(v) for k, v in configured.items()}
        except json.JSONDecodeError:
            pass
        return default_map

    def _normalize_payload(self, payload: dict) -> dict:
        # Supports two input styles:
        # 1) canonical input with `record`
        # 2) source row input with `source_row` and header-based values
        if "record" in payload:
            return payload
        source_row = payload.get("source_row")
        if not isinstance(source_row, dict):
            return payload

        mapped_record: dict[str, str] = {}
        for source_header, target_field in self._source_field_map().items():
            if source_header in source_row:
                value = source_row.get(source_header)
                if value is not None and str(value).strip() != "":
                    mapped_record[target_field] = str(value).strip()

        # Be lenient for wrapped/variant header labels from Google Sheets exports.
        if "gameInstructions" not in mapped_record:
            for header, value in source_row.items():
                normalized_header = str(header).strip().lower()
                if normalized_header.startswith("game details"):
                    if value is not None and str(value).strip() != "":
                        mapped_record["gameInstructions"] = str(value).strip()
                    break

        normalized = dict(payload)
        normalized["record"] = mapped_record
        normalized.pop("source_row", None)
        info(
            "normalize_source_row",
            mapped_fields=sorted(mapped_record.keys()),
            source_field_count=len(source_row.keys()),
        )
        return normalized

    def validate_and_map_record(self, event: SheetEvent) -> CanonicalEvent:
        canonical = CanonicalEvent.model_validate(
            {
                **event.model_dump(),
                "idempotency_key": compute_idempotency_key(event.source),
            }
        )
        info(
            "validate_and_map_record",
            trace_id=event.trace_id,
            event_id=event.event_id,
            idempotency_key=canonical.idempotency_key,
        )
        return canonical

    def dedupe_check(self, event: CanonicalEvent) -> bool:
        if self.idempotency.seen(event.idempotency_key):
            info(
                "dedupe_check_duplicate",
                trace_id=event.trace_id,
                event_id=event.event_id,
                idempotency_key=event.idempotency_key,
            )
            return False
        self.idempotency.mark(event.idempotency_key)
        info(
            "dedupe_check_new",
            trace_id=event.trace_id,
            event_id=event.event_id,
            idempotency_key=event.idempotency_key,
        )
        return True

    async def route_to_destination(self, event: CanonicalEvent, test_mode: bool = True) -> dict:
        info(
            "route_to_destination_start",
            trace_id=event.trace_id,
            event_id=event.event_id,
            test_mode=test_mode,
        )

        async def _call() -> dict:
            result = await self.destination.route_to_destination(event, test_mode=test_mode)
            if not result["ok"]:
                error(
                    "route_to_destination_failed",
                    trace_id=event.trace_id,
                    event_id=event.event_id,
                    status_code=result["status_code"],
                    response=result["body"],
                )
                raise HTTPException(status_code=result["status_code"], detail=result["body"])
            return result

        result = await with_exponential_backoff(_call)
        info(
            "route_to_destination_success",
            trace_id=event.trace_id,
            event_id=event.event_id,
            status_code=result["status_code"],
        )
        return result

    async def start_processing_workflow(self, event: CanonicalEvent) -> JobStatus:
        info(
            "start_processing_workflow",
            trace_id=event.trace_id,
            event_id=event.event_id,
            row_number=event.source.row_number,
        )
        processing_job = JobStatus(
            trace_id=event.trace_id,
            event_id=event.event_id,
            status=WorkflowStatus.PROCESSING,
            attempts=1,
        )
        self.jobs.upsert(processing_job)
        info(
            "job_status_update",
            trace_id=event.trace_id,
            event_id=event.event_id,
            status=processing_job.status.value,
            attempts=processing_job.attempts,
        )
        await self.sheets.update_row_status(
            row_number=event.source.row_number,
            status=WorkflowStatus.PROCESSING.value,
            attempt_count=processing_job.attempts,
            job_id=str(event.event_id),
            processed_at=datetime.now(UTC).isoformat(),
        )

        try:
            result = await self.pipeline.run(str(event.event_id))
            info(
                "video_pipeline_completed",
                trace_id=event.trace_id,
                event_id=event.event_id,
                feedback_link=result["feedback_link"],
            )
            synced_job = JobStatus(
                trace_id=event.trace_id,
                event_id=event.event_id,
                status=WorkflowStatus.SYNCED,
                feedback_link=result["feedback_link"],
                attempts=processing_job.attempts,
            )
            self.jobs.upsert(synced_job)
            info(
                "job_status_update",
                trace_id=event.trace_id,
                event_id=event.event_id,
                status=synced_job.status.value,
                attempts=synced_job.attempts,
            )
            await self.sheets.update_row_status(
                row_number=event.source.row_number,
                status=WorkflowStatus.SYNCED.value,
                attempt_count=synced_job.attempts,
                feedback_link=synced_job.feedback_link,
                job_id=str(event.event_id),
                processed_at=result["completed_at"],
            )
            return synced_job
        except Exception as exc:  # noqa: BLE001
            failed_job = JobStatus(
                trace_id=event.trace_id,
                event_id=event.event_id,
                status=WorkflowStatus.FAILED,
                attempts=processing_job.attempts,
                last_error=str(exc),
            )
            self.jobs.upsert(failed_job)
            error(
                "job_status_update",
                trace_id=event.trace_id,
                event_id=event.event_id,
                status=failed_job.status.value,
                attempts=failed_job.attempts,
                error=str(exc),
            )
            await self.sheets.update_row_status(
                row_number=event.source.row_number,
                status=WorkflowStatus.FAILED.value,
                attempt_count=failed_job.attempts,
                last_error=failed_job.last_error,
                job_id=str(event.event_id),
                processed_at=datetime.now(UTC).isoformat(),
            )
            error("workflow_failed", trace_id=event.trace_id, error=str(exc))
            return failed_job

    def get_job_status(self, event_id: str) -> JobStatus:
        job = self.jobs.get(event_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        info("get_job_status", event_id=event_id, status=job.status.value)
        return job

    async def run_end_to_end(self, payload: dict, test_mode: bool = True) -> JobStatus:
        event = self.ingest_sheet_event(payload)
        canonical = self.validate_and_map_record(event)
        if not self.dedupe_check(canonical):
            info(
                "run_end_to_end_duplicate",
                trace_id=canonical.trace_id,
                event_id=canonical.event_id,
            )
            existing = self.jobs.get(str(canonical.event_id))
            if existing is None:
                return JobStatus(
                    trace_id=canonical.trace_id,
                    event_id=canonical.event_id,
                    status=WorkflowStatus.SYNCED,
                    attempts=1,
                )
            return existing
        await self.route_to_destination(canonical, test_mode=test_mode)
        final_job = await self.start_processing_workflow(canonical)
        info(
            "run_end_to_end_completed",
            trace_id=canonical.trace_id,
            event_id=canonical.event_id,
            final_status=final_job.status.value,
        )
        return final_job


def build_example_event() -> dict:
    return {
        "event_id": str(uuid4()),
        "trace_id": str(uuid4()),
        "event_type": "sheet.row.updated",
        "source": {
            "provider": "google_sheets",
            "spreadsheet_id": "example-sheet-id",
            "sheet_name": "Source",
            "row_number": 2,
            "row_version": datetime.now(UTC).isoformat(),
        },
        "record": {
            "name": "John Doe",
            "color": "Blue",
            "jerseyNumber": "10",
            "position": "Midfielder",
            "gameInstructions": "Subbed in at 1:06:33",
            "videoLink": "https://example.com/video.mp4",
            "typeOfVideo": "Match",
        },
    }
