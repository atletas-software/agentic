from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import inspect, text

from backendapi.api.routes.auth import router as auth_router
from backendapi.api.routes.admin import router as admin_router
from backendapi.api.routes.admin_auth import router as admin_auth_router
from backendapi.api.routes.google_integrations import router as google_integrations_router
from backendapi.api.routes.agents import router as agents_router
from backendapi.api.routes.feedback_proxy import router as feedback_proxy_router
from backendapi.api.routes.sync import router as sync_router
from backendapi.api.routes.ui import router as ui_router
from backendapi.api.routes.workflow import router as workflow_router
from backendapi.core.env_loader import ensure_env_loaded
from backendapi.db import Base, engine
from backendapi.models import google_oauth  # noqa: F401
from backendapi.models import auth as auth_models  # noqa: F401
from backendapi.models import workspace as workspace_models  # noqa: F401
from backendapi.services.sync_scheduler import SyncScheduler

ensure_env_loaded()
Base.metadata.create_all(bind=engine)


def _ensure_player_memory_schema() -> None:
    """Create player memory metadata tables (vectors live in GCP Firestore)."""
    from backendapi.models.player_memory import (
        PlayerDirectoryEntry,
        PlayerMemorySettings,
        PlayerPersonalContextOverlay,
        SqlSyncCursor,
    )

    SqlSyncCursor.__table__.create(bind=engine, checkfirst=True)
    PlayerMemorySettings.__table__.create(bind=engine, checkfirst=True)
    PlayerDirectoryEntry.__table__.create(bind=engine, checkfirst=True)
    PlayerPersonalContextOverlay.__table__.create(bind=engine, checkfirst=True)


_ensure_player_memory_schema()

from backendapi.api.routes.player_memory import router as player_memory_router  # noqa: E402


def _ensure_sync_enabled_column() -> None:
    with engine.begin() as connection:
        inspector = inspect(connection)
        columns = {col["name"] for col in inspector.get_columns("user_sync_settings")}
        if "sync_enabled" in columns:
            return
        if engine.dialect.name == "sqlite":
            connection.execute(
                text(
                    "ALTER TABLE user_sync_settings "
                    "ADD COLUMN sync_enabled BOOLEAN NOT NULL DEFAULT 0"
                )
            )
            return
        connection.execute(
            text(
                "ALTER TABLE user_sync_settings "
                "ADD COLUMN sync_enabled BOOLEAN NOT NULL DEFAULT FALSE"
            )
        )


_ensure_sync_enabled_column()


def _ensure_google_oauth_state_code_verifier_column() -> None:
    with engine.begin() as connection:
        inspector = inspect(connection)
        columns = {col["name"] for col in inspector.get_columns("google_oauth_states")}
        if "code_verifier" in columns:
            return
        connection.execute(text("ALTER TABLE google_oauth_states ADD COLUMN code_verifier TEXT"))


_ensure_google_oauth_state_code_verifier_column()

_frontend_origins = [
    o.strip()
    for o in (os.getenv("FRONTEND_BASE_URL") or "http://localhost:3000").split(",")
    if o.strip()
]

app = FastAPI(title="Sheet MCP Workflow", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_frontend_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
sync_scheduler = SyncScheduler()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.on_event("startup")
async def on_startup() -> None:
    sync_scheduler.start()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await sync_scheduler.stop()


app.include_router(feedback_proxy_router)
app.include_router(ui_router)
app.include_router(auth_router)
app.include_router(admin_auth_router)
app.include_router(admin_router)
app.include_router(workflow_router)
app.include_router(google_integrations_router)
app.include_router(sync_router)
app.include_router(agents_router)
app.include_router(player_memory_router)
