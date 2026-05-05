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

    configured = os.getenv("ENV_FILE")
    env_path = Path(configured) if configured else Path("app/.env")
    # Keep already-exported runtime env vars (e.g. docker-compose environment)
    # and only fill missing values from dotenv files.
    loaded = load_dotenv(dotenv_path=env_path, override=False)
    fallback_loaded = False
    if not loaded and env_path != Path(".env"):
        # Backward compatibility for existing setups still using repo-root .env
        fallback_loaded = load_dotenv(dotenv_path=Path(".env"), override=False)
    info(
        "env_loaded",
        env_file=str(env_path),
        loaded=loaded,
        fallback_env_file=".env",
        fallback_loaded=fallback_loaded,
    )
    _loaded = True
