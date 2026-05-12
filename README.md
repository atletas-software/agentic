# Sheet MCP Workflow (FastAPI)

FastAPI implementation of the sheet-triggered MCP workflow:

- `ingest_sheet_event`
- `validate_and_map_record`
- `dedupe_check`
- `route_to_destination`
- `start_processing_workflow`
- `update_sheet_status`
- `get_job_status`

It reuses your existing append API endpoint as the destination adapter while keeping orchestration modular for future integrations.

Google OAuth + Sheets integration is included with token storage in the application database.
Polling-based source-to-destination sync is included for active user sheets.
Authentication uses Google OAuth with database-backed browser sessions.

The repository contains **two runnable services**:

1. **Platform API** (`app/`) — main FastAPI app (sheets, sync, auth, workspace/agent job orchestration).
2. **Feedback agent** (`agents/feedback/`) — separate FastAPI service that runs video review jobs (OpenAI + ffmpeg/OpenCV). The platform worker can **delegate** feedback jobs to it over HTTP when `FEEDBACK_AGENT_BASE_URL` is set.

You can run only the platform (without the feedback agent), or run both when you need automated video reviews from workspace jobs.

## Architecture and processes

At a high level, local or production setups involve these processes:

| Process | Role |
|--------|------|
| **Platform API** (`uvicorn app.main:app`) | HTTP API, OAuth, enqueueing sync jobs, workspace context refresh, agent job creation. Listens on port **8000** by default. |
| **RQ worker** (`python -m app.workers.run_worker`) | Consumes **Redis** queues `SYNC_QUEUE_NAME` (default `sheet-sync`) and `WORKSPACE_QUEUE_NAME` (default `agent-workspace`). Runs sheet sync jobs, refreshes workspace snapshots from the **destination** sheet, and optionally **POST**s feedback work to the feedback agent. |
| **Redis** | Job queue backend for RQ. |
| **Postgres** (or SQLite for quick dev) | Application database (`DATABASE_URL`). |
| **Feedback agent** (`uvicorn agents.feedback.main:app`) | Optional. Accepts `POST /api/reviews`, runs review pipeline, stores reviews under `DATA_DIR`. Default port **5055**. |

Typical flow when everything is enabled:

1. User connects Google Sheets and sync runs (worker + destination sheet).
2. After a successful sync, the worker may enqueue a **workspace context** job (destination sheet snapshot).
3. When a **feedback** `AgentJob` is created in the platform, the worker calls `FEEDBACK_AGENT_BASE_URL` (if set) with `POST /api/reviews` and polls until the agent reports completion.

## Configuration (local and Docker use the same files)

| File | Purpose |
|------|--------|
| **`app/.env`** | Primary application config: OAuth, `DATABASE_URL`, `REDIS_URL`, player memory, destination sheet, etc. Used when you run `make run-api` / `make run-worker` locally and **mounted into API/worker containers** when using Compose. |
| **`.env`** (repo root, optional) | Copy from **`.env.example`** at the repo root. Docker Compose reads this automatically for **interpolation** in `docker-compose.yml` (`POSTGRES_PASSWORD`, `POSTGRES_DB`, ports). Keep Postgres credentials **consistent** with the user/password in `app/.env`’s `DATABASE_URL` when your local URL points at the same Postgres (e.g. `localhost:5432`). |
| **`agents/.env`** | Feedback agent secrets (`OPENAI_API_KEY`, etc.). Used by the feedback container via `env_file`; mirror variables locally when running `make run-feedback`. |

At runtime, the platform loads **`app/.env` first**, then merges **repo-root `.env`** only for keys that are still unset (`override=False`). Variables injected by Compose (`DATABASE_URL`, `REDIS_URL`, …) are never overwritten by dotenv.

Inside Compose, **`docker-compose.yml`** overrides `DATABASE_URL`, `REDIS_URL`, `FEEDBACK_AGENT_BASE_URL`, and the destination credentials path so containers talk to `postgres`, `redis`, and `feedback-agent` by service name. Defaults match **`DATABASE_URL_DOCKER`**, **`REDIS_URL_DOCKER`**, etc., so you usually **do not** duplicate those in `.env` unless you customize clusters.

Player memory SQL/vector runtime settings are now managed from the admin panel (`/app/player-memory`) and persisted encrypted in the DB. Keep `PLAYER_MEMORY_SETTINGS_MASTER_KEY` set in `app/.env`; `PLAYER_CONTEXT_*`, `PINECONE_*`, and chunk/retrieval values in env act as bootstrap defaults until admin settings are saved.

## Run locally (venv)

Prerequisites: **Python 3.11+**, **Redis** (e.g. `brew services start redis`), and **`app/.env`** from `app/.env.example`.

- **General platform dev:** SQLite works if `DATABASE_URL` is omitted (defaults to `sqlite:///./app.db`).
- **Player vector memory:** PostgreSQL with **`pgvector`** (`CREATE EXTENSION vector`). Not available on SQLite.

```bash
make setup-app
make setup-agents   # optional, for feedback agent
cp app/.env.example app/.env   # then edit
make run-local       # prints reminder: terminals for api / worker / feedback
```

Terminals:

```bash
make run-api
make run-worker
make run-feedback    # optional
```

## Local development: platform only

Prerequisites: **Python 3.11+** recommended, **Redis** running, and a database (Postgres or omit `DATABASE_URL` to use default SQLite `app.db`).

Quick setup with `Makefile`:

```bash
make setup-app
```

```bash
python3 -m venv .venv-app
source .venv-app/bin/activate   # Windows: .venv-app\Scripts\activate
pip install -r app/requirements.txt
cp app/.env.example app/.env
# Edit app/.env: at minimum REDIS_URL, DATABASE_URL (or keep SQLite), Google OAuth, destination sheet vars as needed.
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

In a **second** terminal (same app venv):

```bash
source .venv-app/bin/activate
python -m app.workers.run_worker
```

Health check: [http://localhost:8000/health](http://localhost:8000/health)

## Local development: platform and feedback agent

Prerequisites for the feedback agent: **ffmpeg** on your `PATH` (for example `brew install ffmpeg` on macOS), and an **OpenAI API key**.

**Terminal 1 — platform API** (same as above):

```bash
source .venv-app/bin/activate
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**Terminal 2 — RQ worker** (must see the same `FEEDBACK_AGENT_BASE_URL` as in `.env`):

```bash
source .venv-app/bin/activate
python -m app.workers.run_worker
```

**Terminal 3 — feedback agent** (from repository root so `agents` is importable). Create a dedicated agent venv and install agent dependencies:

```bash
make setup-agents
```

```bash
python3 -m venv .venv-agents
source .venv-agents/bin/activate   # Windows: .venv-agents\Scripts\activate
pip install -r agents/requirements.txt
export OPENAI_API_KEY=sk-...   # or add OPENAI_API_KEY to a .env file in this directory
PYTHONPATH=. uvicorn agents.feedback.main:app --host 0.0.0.0 --port 5055
```

Run shortcuts:

```bash
make run-api
make run-worker
OPENAI_API_KEY=sk-... make run-feedback
```

In the **platform** `app/.env` (used by the API and worker), set:

```bash
FEEDBACK_AGENT_BASE_URL=http://127.0.0.1:5055
```

For local feedback setup, prefer **`make run-feedback`**; Compose can run the same agent image instead if you use **`make run-all`**.

## Run with Docker (same `app/.env` + optional repo-root `.env`)

1) Copy templates:

```bash
cp app/.env.example app/.env
cp .env.example .env        # repo root — supplies POSTGRES_* for compose interpolation
```

Edit **`app/.env`** with OAuth, secrets, and (for host-side tools) `DATABASE_URL` pointing at `localhost` if you use the compose Postgres port mapping. Align **`POSTGRES_PASSWORD`** in repo-root **`.env`** with the password in that URL.

Optional: **`agents/.env`** from `agents/.env.example` for the feedback container.

2) Start the full stack:

```bash
make run-all
# equivalent: make docker-up   →  docker compose up -d --build
```

This starts:

- `api` (FastAPI app)
- `worker` (`python -m app.workers.run_worker`)
- `feedback-agent` (FastAPI feedback service on `:5055`)
- `redis`
- `postgres`

3) Verify deployment:

```bash
docker compose ps
docker compose logs -f api
docker compose logs -f worker
```

Health check endpoint:
- [http://localhost:8000/health](http://localhost:8000/health)

Stop stack:

```bash
make docker-down
```

### Production safety notes

- Do not commit real `app/.env` or `credentials.json`.
- `credentials.json` is mounted read-only into containers at `/run/secrets/google-credentials.json`.
- Use a managed Postgres/Redis and a secret manager in cloud production when possible.

Open backend UI pages (local or Docker):
- [http://localhost:8000/app/connect](http://localhost:8000/app/connect)
- [http://localhost:8000/app/sheets](http://localhost:8000/app/sheets)
- [http://localhost:8000/app/agents](http://localhost:8000/app/agents)
- [http://localhost:8000/app/player-memory](http://localhost:8000/app/player-memory) (admin login required)
- [http://localhost:8000/app/sheets/details?spreadsheet_id=YOUR_ID](http://localhost:8000/app/sheets/details?spreadsheet_id=YOUR_ID)

## Environment variables

Copy `app/.env.example` to `app/.env` and adjust values. The **platform** reads the variables below from the environment (or `app/.env` by default). The **feedback agent** uses a smaller set; only the platform needs database and Google OAuth for the main app.

### Platform (`app/`) — core

| Variable | Required | Default / notes |
|----------|----------|-----------------|
| `DATABASE_URL` | Recommended for production | Default `sqlite:///./app.db` if unset |
| `GOOGLE_CLIENT_ID` | Yes (OAuth) | — |
| `GOOGLE_CLIENT_SECRET` | Yes (OAuth) | — |
| `GOOGLE_REDIRECT_URI` | Yes (OAuth) | Must match Google Cloud console (e.g. `http://localhost:8000/integrations/google/callback`) |
| `SESSION_TTL_DAYS` | No | `30` |
| `REDIS_URL` | Yes (workers / queues) | `redis://localhost:6379/0` |
| `SYNC_QUEUE_NAME` | No | `sheet-sync` |
| `SYNC_POLL_ENABLED` | No | `true` |
| `SYNC_POLL_TICK_SECONDS` | No | `30` (also accepts legacy `SYNC_POLL_INTERVAL_SECONDS`) |
| `SYNC_QUOTA_BACKOFF_BASE_MINUTES` | No | `1` |
| `SYNC_QUOTA_BACKOFF_MAX_MINUTES` | No | `60` |
| `DESTINATION_GOOGLE_CREDENTIALS_FILE` | For destination sync | `credentials.json` |
| `DESTINATION_SPREADSHEET_ID` | For polling sync to destination | Empty disables real destination writes |
| `DESTINATION_USER_SHEET_PREFIX` | No | `user` (tab name pattern `<prefix>_<sanitized_user_id>`) |

### Platform — admin UI

| Variable | Required | Notes |
|----------|----------|--------|
| `ADMIN_EMAILS` | For admin login | Comma-separated emails |
| `ADMIN_PASSWORD` | For admin login | Shared password for listed admins |
| `ADMIN_SESSION_TTL_DAYS` | No | `7` |

### Platform — workspace and feedback delegation

| Variable | Required | Notes |
|----------|----------|--------|
| `WORKSPACE_QUEUE_NAME` | No | `agent-workspace`; must match worker queue subscription |
| `WORKSPACE_SNAPSHOT_MAX_ROWS` | No | `500` cap when snapshotting destination sheet for workspace |
| `FEEDBACK_AGENT_BASE_URL` | No | If empty, feedback delegate jobs fail with a clear message. Set to feedback agent URL, e.g. `http://127.0.0.1:5055` |
| `FEEDBACK_AGENT_HTTP_TIMEOUT_SECONDS` | No | `30` for HTTP client when calling the agent |

### Platform — optional source / workflow / sync tuning

| Variable | Default / notes |
|----------|-----------------|
| `SOURCE_FIELD_MAP` | Optional JSON map for `source_row` headers → normalized fields |
| `SOURCE_SHEET_ID` | If missing, some source status updates run in mock mode |
| `SOURCE_SHEET_NAME` | `Sheet1` |
| `SOURCE_STATUS_COLUMN`, `SOURCE_ERROR_COLUMN`, `SOURCE_ATTEMPTS_COLUMN`, `SOURCE_LAST_PROCESSED_COLUMN`, `SOURCE_FEEDBACK_LINK_COLUMN`, `SOURCE_JOB_ID_COLUMN` | Column names for source sheet status writes |
| `ENABLE_SOURCE_STATUS_UPDATES` | `false` |
| `SYNC_USER_LOCK_TTL_SECONDS` | `300` |
| `SYNC_USER_LOCK_PREFIX` | `sheet-sync-lock` |
| `SYNC_RECONCILE_DESTINATION` | `false` |
| `SYNC_HASH_FIELDS` | Optional comma-separated fields for row hashing |
| `ENV_FILE` | `.env` path override (see `app/core/env_loader.py`) |

### Feedback agent (`agents/feedback/`)

These apply to the **feedback agent process** (or its container). At startup the agent loads, in order: the **current working directory** `.env` (usually the repo root when you run `uvicorn` from there), then **`agents/.env`** if it exists (overrides for agent-level secrets). Template: `agents/.env.example` — copy it to `agents/.env` (gitignored) or set the same variables in the repo root `.env`.

| Variable | Required | Default / notes |
|----------|----------|-----------------|
| `OPENAI_API_KEY` | **Yes** (for reviews) | Used by `openai_service` for storyboard and manual feedback |
| `OPENAI_MODEL` | No | `gpt-4.1-mini` |
| `DATA_DIR` | No | Defaults to `agents/feedback/data` under the package (persistent review storage) |
| `HOST` | No | `127.0.0.1` (used when building `review_url` in job payload if `PUBLIC_BASE_URL` unset) |
| `PORT` | No | `5055` (match the port you pass to `uvicorn`) |
| `PUBLIC_BASE_URL` | No | If set (no trailing slash), share links and `review_url` use this origin instead of `http://HOST:PORT` |

**System dependency:** `ffmpeg` must be installed on the host (or in the agent Docker image) for video frame extraction.

## Endpoints

- `POST /mcp/ingest_sheet_event`
- `POST /mcp/validate_and_map_record`
- `POST /mcp/dedupe_check`
- `POST /mcp/route_to_destination?test_mode=true`
- `POST /mcp/start_processing_workflow`
- `POST /mcp/update_sheet_status`
- `GET /mcp/get_job_status/{event_id}`
- `POST /workflow/run?test_mode=true`
- `GET /workflow/example_event`
- `GET /integrations/google/connect` (returns OAuth URL)
- `GET /integrations/google/callback` (exchanges code, stores user tokens, creates app session)
- `GET /integrations/google/sheets` (lists spreadsheet files)
- `GET /integrations/google/sheets/{spreadsheet_id}` (reads range, default `Sheet1`)
- `GET /integrations/google/sheets/{spreadsheet_id}/tabs` (lists tab names inside selected spreadsheet)
- `POST /integrations/google/sheets/{spreadsheet_id}` (updates range values)
- `GET /integrations/google/selected-sheet` (get user's chosen sheet)
- `POST /integrations/google/selected-sheet` (save user's chosen sheet)
- `GET /integrations/google/sync-settings` (per-user polling settings)
- `POST /integrations/google/sync-settings` (update per-user polling settings in seconds presets)
- `GET /integrations/google/sync-status` (last run, next due, last error for current user)
- `GET /auth/me`
- `POST /auth/logout`
- `POST /sync/run-once` (manual polling sync trigger)
- `GET /sync/runs` (latest sync runs for current user)
- `GET /sync/runs/{run_id}/events` (row-level sync execution logs)
- `GET /sync/states` (latest row snapshot states/hash history)
- `GET /app/connect` (backend hosted connect page)
- `GET /app/sheets` (backend hosted sheets browser page)
- `GET /app/login` (redirects to connect page)
- `GET /app/register` (redirects to connect page)

OAuth scopes used:
- `https://www.googleapis.com/auth/spreadsheets`
- `https://www.googleapis.com/auth/drive.file`
- `https://www.googleapis.com/auth/drive.metadata.readonly`

## Queue Worker Architecture

- FastAPI process runs a lightweight scheduler loop that only enqueues due user sync jobs.
- RQ workers process sync jobs concurrently (run multiple worker processes to scale).
- Job retries are configured on enqueue.
- Per-user quota backoff is persisted in DB and respected before enqueueing due jobs.

## Source Row Mapping

If your trigger sends raw row headers, post payloads using `source_row` instead of `record`.

Example:

```json
{
  "event_id": "uuid",
  "trace_id": "uuid",
  "event_type": "sheet.row.updated",
  "source": {
    "provider": "google_sheets",
    "spreadsheet_id": "sheet-id",
    "sheet_name": "Form Responses 1",
    "row_number": 2,
    "row_version": "2026-04-24T08:00:00Z"
  },
  "source_row": {
    "First and Last name": "Landon Jesse",
    "Team color": "White",
    "Team Number": "30",
    "Position Played": "CDM",
    "Game Details": "Subbed in at 1:06:33",
    "Link to game": "https://app.hudl.com/...",
    "Type of Video": "Match"
  }
}
```
