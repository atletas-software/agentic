from __future__ import annotations

from backendapi.core.logger import error, info
from backendapi.db import SessionLocal
from backendapi.models.google_oauth import SheetSyncRun
from backendapi.services.sheet_sync import run_sync_once_for_users
from backendapi.services.sync_backoff import register_sync_result
from backendapi.services.sync_queue import release_user_enqueue_lock
from backendapi.services.workspace_enqueue import enqueue_workspace_context_refresh


def process_user_sync_job(user_id: str) -> dict[str, int | str | None]:
    db = SessionLocal()
    try:
        result = run_sync_once_for_users(db=db, user_ids=[user_id])
        latest_run = (
            db.query(SheetSyncRun)
            .filter(SheetSyncRun.user_id == user_id)
            .order_by(SheetSyncRun.started_at.desc())
            .first()
        )
        run_error = latest_run.error_message if latest_run is not None else None
        backoff = register_sync_result(user_id=user_id, run_error_message=run_error, db=db)
        info(
            "sync_worker_job_complete",
            user_id=user_id,
            runs=result["runs"],
            rows=result["rows"],
            backoff=backoff,
        )
        # Refresh per-user workspace context from destination sheet (not source).
        enqueue_workspace_context_refresh(user_id)
        return {
            "runs": result["runs"],
            "rows": result["rows"],
            "quota_errors": int(backoff["consecutive_quota_errors"]),
            "next_allowed_at": backoff["next_allowed_at"],
        }
    except Exception as exc:  # noqa: BLE001
        error("sync_worker_job_failed", user_id=user_id, error=str(exc))
        raise
    finally:
        release_user_enqueue_lock(user_id=user_id)
        db.close()
