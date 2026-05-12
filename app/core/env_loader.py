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
    primary = Path(configured) if configured else Path("app/.env")
    root_env = Path(".env")
    # Keep already-exported runtime env vars (e.g. docker-compose environment)
    # and only fill missing values from dotenv files.
    loaded_primary = load_dotenv(dotenv_path=primary, override=False) if primary.exists() else False
    loaded_root = False
    if not loaded_primary and primary.resolve() != root_env.resolve() and root_env.exists():
        # Backward compatibility: only repo-root .env (no app/.env)
        loaded_root = load_dotenv(dotenv_path=root_env, override=False)
    elif primary.exists() and root_env.exists() and primary.resolve() != root_env.resolve():
        # Merge repo-root .env after app/.env — fills POSTGRES_* etc. without overriding app keys.
        loaded_root = load_dotenv(dotenv_path=root_env, override=False)
    info(
        "env_loaded",
        env_file=str(primary),
        loaded=loaded_primary,
        fallback_env_file=str(root_env),
        fallback_loaded=loaded_root,
    )
    _loaded = True
