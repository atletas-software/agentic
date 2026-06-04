from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))


def ensure_directories() -> None:
    (DATA_DIR / "reviews").mkdir(parents=True, exist_ok=True)


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def review_cancel_requested(review_id: str) -> bool:
    """Workspace Stop calls POST /api/reviews/{id}/cancel which touches this file."""
    return (DATA_DIR / "reviews" / review_id / "cancel_requested").exists()
