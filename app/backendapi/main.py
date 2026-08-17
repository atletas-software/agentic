from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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
from backendapi.core.startup import init_database_schema
from backendapi.core.logger import error
from backendapi.models import google_oauth  # noqa: F401
from backendapi.models import auth as auth_models  # noqa: F401
from backendapi.models import workspace as workspace_models  # noqa: F401
from backendapi.api.routes.player_memory import router as player_memory_router
from backendapi.services.frontend_origin import frontend_cors_origins
from backendapi.services.sync_scheduler import SyncScheduler

ensure_env_loaded()

_frontend_origins = frontend_cors_origins()

app = FastAPI(title="Athlete Agent API", version="0.1.0")
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
    try:
        init_database_schema()
    except Exception as exc:  # noqa: BLE001
        error(
            "database_schema_init_failed",
            error=str(exc),
            hint=(
                "With --profile cloudsql, set DATABASE_URL host to cloud-sql-proxy in "
                "repo-root .env (DATABASE_URL_DOCKER) or app/backendapi/.env. "
                "Grant the VM or service-account JSON roles/cloudsql.client."
            ),
        )
        raise
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
