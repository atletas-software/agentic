from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR.parent / "data" / "pose_api"))
JOBS_DIR = DATA_DIR / "jobs"


def ensure_directories() -> None:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)


def job_dir(job_id: str) -> Path:
    return JOBS_DIR / job_id


def job_path(job_id: str) -> Path:
    return job_dir(job_id) / "job.json"


def pose_result_path(job_id: str) -> Path:
    return job_dir(job_id) / "pose_results.json"


def cancel_flag_path(job_id: str) -> Path:
    return job_dir(job_id) / "cancel_requested"


def job_cancel_requested(job_id: str) -> bool:
    return cancel_flag_path(job_id).exists()


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
