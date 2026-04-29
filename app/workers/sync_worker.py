from __future__ import annotations

from app.core.logger import error, info
from app.db import SessionLocal
from app.models.google_oauth import SheetSyncRun
from app.services.sheet_sync import run_sync_once_for_users
from app.services.sync_backoff import register_sync_result
from app.services.sync_queue import release_user_enqueue_lock


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
