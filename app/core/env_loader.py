from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

from app.core.logger import info

_loaded = False


def ensure_env_loaded() -> None:
    global _loaded
    if _loaded:
        return

    env_path = Path(os.getenv("ENV_FILE", ".env"))
    loaded = load_dotenv(dotenv_path=env_path, override=True)
    info("env_loaded", env_file=str(env_path), loaded=loaded)
    _loaded = True
