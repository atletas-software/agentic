from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from sqlalchemy import inspect, text

from app.api.routes.auth import router as auth_router
from app.api.routes.google_integrations import router as google_integrations_router
from app.api.routes.sync import router as sync_router
from app.api.routes.ui import router as ui_router
from app.api.routes.workflow import router as workflow_router
from app.core.env_loader import ensure_env_loaded
from app.db import Base, engine
from app.models import google_oauth  # noqa: F401
from app.models import auth as auth_models  # noqa: F401
from app.services.sync_scheduler import SyncScheduler

ensure_env_loaded()
Base.metadata.create_all(bind=engine)


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

app = FastAPI(title="Sheet MCP Workflow", version="0.1.0")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
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


app.include_router(ui_router)
app.include_router(auth_router)
app.include_router(workflow_router)
app.include_router(google_integrations_router)
app.include_router(sync_router)
