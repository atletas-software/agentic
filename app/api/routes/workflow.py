from __future__ import annotations

from fastapi import APIRouter, Query

from app.services.orchestrator import SheetWorkflowOrchestrator, build_example_event

router = APIRouter()
orchestrator = SheetWorkflowOrchestrator()


@router.post("/mcp/ingest_sheet_event")
async def ingest_sheet_event(payload: dict) -> dict:
    event = orchestrator.ingest_sheet_event(payload)
    return {"event": event.model_dump(mode="json")}


@router.post("/mcp/validate_and_map_record")
async def validate_and_map_record(payload: dict) -> dict:
    event = orchestrator.ingest_sheet_event(payload)
    canonical = orchestrator.validate_and_map_record(event)
    return {"canonical_event": canonical.model_dump(mode="json")}


@router.post("/mcp/dedupe_check")
async def dedupe_check(payload: dict) -> dict:
    event = orchestrator.ingest_sheet_event(payload)
    canonical = orchestrator.validate_and_map_record(event)
    is_new = orchestrator.dedupe_check(canonical)
    return {"is_new": is_new, "idempotency_key": canonical.idempotency_key}


@router.post("/mcp/route_to_destination")
async def route_to_destination(payload: dict, test_mode: bool = Query(default=True)) -> dict:
    event = orchestrator.ingest_sheet_event(payload)
    canonical = orchestrator.validate_and_map_record(event)
    result = await orchestrator.route_to_destination(canonical, test_mode=test_mode)
    return {"result": result}


@router.post("/mcp/start_processing_workflow")
async def start_processing_workflow(payload: dict) -> dict:
    event = orchestrator.ingest_sheet_event(payload)
    canonical = orchestrator.validate_and_map_record(event)
    job = await orchestrator.start_processing_workflow(canonical)
    return {"job": job.model_dump(mode="json")}


@router.post("/mcp/update_sheet_status")
async def update_sheet_status(payload: dict) -> dict:
    result = await orchestrator.sheets.update_row_status(**payload)
    return {"result": result}


@router.get("/mcp/get_job_status/{event_id}")
async def get_job_status(event_id: str) -> dict:
    job = orchestrator.get_job_status(event_id)
    return {"job": job.model_dump(mode="json")}


@router.post("/workflow/run")
async def run_workflow(payload: dict, test_mode: bool = Query(default=True)) -> dict:
    job = await orchestrator.run_end_to_end(payload, test_mode=test_mode)
    return {"job": job.model_dump(mode="json")}


@router.get("/workflow/example_event")
async def example_event() -> dict:
    return {"example": build_example_event()}
