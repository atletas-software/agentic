# Athlete Agent Platform (FastAPI)

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

Application code lives under **`app/`**:

```
app/
  backendapi/    platform FastAPI (sheets, sync, auth, workers) — Python package `backendapi`
  agents/        feedback agent service — Python package `agents`
  yolo_model/    YOLO training + pose pipeline — Python package `yolo_model`
```

Set **`PYTHONPATH=app`** (or `export PYTHONPATH="${PWD}/app"` from the repo root) when running locally.

The repository contains **two runnable services**:

1. **Platform API** (`app/backendapi/`) — main FastAPI app (sheets, sync, auth, workspace/agent job orchestration).
2. **Feedback agent** (`app/agents/feedback/`) — separate FastAPI service that runs video review jobs (OpenAI + ffmpeg/OpenCV). The platform worker can **delegate** feedback jobs to it over HTTP when `FEEDBACK_AGENT_BASE_URL` is set.

You can run only the platform (without the feedback agent), or run both when you need automated video reviews from workspace jobs.

## Architecture and processes

At a high level, local or production setups involve these processes:

| Process | Role |
|--------|------|
| **Platform API** (`uvicorn backendapi.main:app`) | HTTP API, OAuth, enqueueing sync jobs, workspace context refresh, agent job creation. Listens on port **8000** by default. |
| **RQ worker** (`python -m backendapi.workers.run_worker`) | Consumes **Redis** queues `SYNC_QUEUE_NAME` (default `sheet-sync`) and `WORKSPACE_QUEUE_NAME` (default `agent-workspace`). Runs sheet sync jobs, refreshes workspace snapshots from the **destination** sheet, and optionally **POST**s feedback work to the feedback agent. |
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
| **`app/backendapi/.env`** | Primary application config: OAuth, `DATABASE_URL`, `REDIS_URL`, player memory, destination sheet, etc. Used when you run `make run-api` / `make run-worker` locally and **mounted into API/worker containers** when using Compose. |
| **`.env`** (repo root, optional) | Copy from **`.env.example`** at the repo root. Used only for Compose **interpolation** (ports, `GOOGLE_CREDENTIALS_FILE_HOST`, optional `CLOUD_SQL_CONNECTION_NAME`). Application URLs and secrets belong in **`app/backendapi/.env`**. |
| **`app/agents/.env`** | Feedback agent secrets (`OPENAI_API_KEY`, etc.). Used by the feedback container via `env_file`; mirror variables locally when running `make run-feedback`. |

At runtime, the platform loads **`app/backendapi/.env` first**, then merges **repo-root `.env`** only for keys that are still unset (`override=False`).

For Docker, set in-cluster URLs in **`app/backendapi/.env`**: `REDIS_URL=redis://redis:6379/0`, `FEEDBACK_AGENT_BASE_URL=http://feedback-agent:5055`, and `DATABASE_URL` pointing at Cloud SQL (private IP or `cloud-sql-proxy` — see **GCP VM** below).

Player memory SQL/vector runtime settings are now managed from the admin panel (`/admin/player-memory`) and persisted encrypted in the DB. Keep `PLAYER_MEMORY_SETTINGS_MASTER_KEY` set in `app/backendapi/.env`; `PLAYER_CONTEXT_*`, `PINECONE_*`, and chunk/retrieval values in env act as bootstrap defaults until admin settings are saved.

## Frontend (Next.js)

The browser UI lives in **`agentic-frontend/`** (Next.js 15, App Router). Run it alongside the platform API:

```bash
cd agentic-frontend && cp .env.example .env.local && npm install && npm run dev
```

Set `FRONTEND_BASE_URL=http://localhost:3000` in `app/backendapi/.env`. See [`agentic-frontend/README.md`](agentic-frontend/README.md) for routes and env details.

## Run locally (venv)

Prerequisites: **Python 3.11+**, **Redis** (e.g. `brew services start redis`), and **`app/backendapi/.env`** from `app/backendapi/.env.example`.

- **General platform dev:** SQLite works if `DATABASE_URL` is omitted (defaults to `sqlite:///./app.db`).
- **Player vector memory:** PostgreSQL with **`pgvector`** (`CREATE EXTENSION vector`). Not available on SQLite.

```bash
make setup-app
make setup-agents   # optional, for feedback agent
cp app/backendapi/.env.example app/backendapi/.env   # then edit
make run-local       # prints reminder: terminals for api / worker / feedback
```

Terminals:

```bash
make run-api
make run-worker
make run-feedback    # optional
cd agentic-frontend && npm run dev   # UI on http://localhost:3000
```

Or from **`app/backendapi/`** (with venv activated or `./run.sh` using `app/backendapi/venv`):

```bash
cd app/backendapi
./run.sh              # API on :8000
./run-worker.sh       # RQ worker (second terminal)
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
pip install -r app/backendapi/requirements.txt
cp app/backendapi/.env.example app/backendapi/.env
# Edit app/backendapi/.env: at minimum REDIS_URL, DATABASE_URL (or keep SQLite), Google OAuth, destination sheet vars as needed.
cd /path/to/agentic && PYTHONPATH=app uvicorn backendapi.main:app --reload --host 0.0.0.0 --port 8000
# Or: cd app/backendapi && ./run.sh
```

In a **second** terminal (same app venv):

```bash
source .venv-app/bin/activate
python -m backendapi.workers.run_worker
```

Health check: [http://localhost:8000/health](http://localhost:8000/health)

## Local development: platform and feedback agent

Prerequisites for the feedback agent: **ffmpeg** on your `PATH` (for example `brew install ffmpeg` on macOS), and an **OpenAI API key**.

**Terminal 1 — platform API** (same as above):

```bash
source .venv-app/bin/activate
uvicorn backendapi.main:app --reload --host 0.0.0.0 --port 8000
```

**Terminal 2 — RQ worker** (must see the same `FEEDBACK_AGENT_BASE_URL` as in `.env`):

```bash
source .venv-app/bin/activate
python -m backendapi.workers.run_worker
```

**Terminal 3 — feedback agent** (from repository root so `agents` is importable). Create a dedicated agent venv and install agent dependencies:

```bash
make setup-agents
```

```bash
python3 -m venv .venv-agents
source .venv-agents/bin/activate   # Windows: .venv-agents\Scripts\activate
pip install -r app/agents/requirements.txt
export OPENAI_API_KEY=sk-...   # or add OPENAI_API_KEY to a .env file in this directory
PYTHONPATH=app uvicorn agents.feedback.main:app --host 0.0.0.0 --port 5055
```

Run shortcuts:

```bash
make run-api
make run-worker
OPENAI_API_KEY=sk-... make run-feedback
```

In the **platform** `app/backendapi/.env` (used by the API and worker), set:

```bash
FEEDBACK_AGENT_BASE_URL=http://127.0.0.1:5055
```

For local feedback setup, prefer **`make run-feedback`**; Compose can run the same agent image instead if you use **`make run-all`**.

## Run with Docker (same `app/backendapi/.env` + optional repo-root `.env`)

1) Copy templates:

```bash
cp app/backendapi/.env.example app/backendapi/.env
cp .env.example .env
cp app/agents/.env.example app/agents/.env
```

Edit **`app/backendapi/.env`** with OAuth, secrets, and Docker URLs (`REDIS_URL`, `FEEDBACK_AGENT_BASE_URL`, `DATABASE_URL`).

2) Start the stack:

**GCP VM (production)** — Cloud SQL + Firestore + in-compose Redis; no local Postgres:

```bash
make run-prod
# or: docker compose --profile cloudsql up -d --build   # when using Cloud SQL Auth Proxy
```

**Local dev** — adds Postgres, bind mounts, and hot reload:

```bash
make run-all
```

Production Compose services: `api`, `worker`, `feedback-agent`, `redis`, optional `cloud-sql-proxy` (`--profile cloudsql`).

Local dev also starts `postgres` via `docker-compose.dev.yml`.

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

### GCP VM: VM service account vs `credentials.json`

| Service | Auth |
|--------|------|
| **Firestore** (player memory vectors) | GCE VM attached service account (`roles/datastore.user`). Do **not** set `GOOGLE_APPLICATION_CREDENTIALS`. |
| **Cloud SQL** | Cloud SQL Auth Proxy container uses the **same VM service account** (`roles/cloudsql.client`). Do **not** pass `credentials.json` to the proxy. |
| **Destination / shared Google Sheets** | Dedicated `credentials.json` (Sheets API service account), mounted at `/run/secrets/google-credentials.json` and referenced by `DESTINATION_GOOGLE_CREDENTIALS_FILE` only. |

**VM setup (you already have this on `athlete-agent-vm@athletefocus-agents.iam.gserviceaccount.com`):**

1. Attach that service account to the VM (Compute Engine → VM → Security).
2. IAM roles on that SA: **Cloud SQL Client**, **Cloud Datastore User** (and any other app roles you need).
3. On the VM, `app/backendapi/.env`:
   - `PLAYER_MEMORY_VECTOR_BACKEND=firestore`
   - `GCP_PROJECT_ID=athletefocus-agents`
   - `GCP_FIRESTORE_DATABASE=...`
   - **No** `GOOGLE_APPLICATION_CREDENTIALS` line
   - `DESTINATION_GOOGLE_CREDENTIALS_FILE=/run/secrets/google-credentials.json`
4. Repo-root `.env`: `CLOUD_SQL_CONNECTION_NAME`, `CLOUDSQL_USER`, `CLOUDSQL_PASSWORD` (URL-encoded), `CLOUDSQL_DATABASE`.
5. Deploy:

```bash
docker compose -f docker-compose.yml -f docker-compose.cloudsql.yml --profile cloudsql up -d --build
```

**Sheets-only JSON:** put `credentials.json` on the VM; share the destination spreadsheet with that SA email. The JSON does **not** need Cloud SQL or Firestore roles.

### Production safety notes

- Do not commit real `app/backendapi/.env` or `credentials.json`.
- `credentials.json` is for **Sheets only**, mounted at `/run/secrets/google-credentials.json`.
- Use a secret manager in production when possible.

Open the Next.js UI (local dev):

- [http://localhost:3000/connect](http://localhost:3000/connect)
- [http://localhost:3000/sheets](http://localhost:3000/sheets)
- [http://localhost:3000/settings](http://localhost:3000/settings)
- [http://localhost:3000/admin](http://localhost:3000/admin) (admin login required)
- [http://localhost:3000/admin/agents-lab](http://localhost:3000/admin/agents-lab)
- [http://localhost:3000/admin/player-memory](http://localhost:3000/admin/player-memory)

Legacy backend paths (`/app/*`, `/admin/*` on port 8000) redirect to the frontend when `FRONTEND_BASE_URL` is set.

## Environment variables

Copy `app/backendapi/.env.example` to `app/backendapi/.env` and adjust values. The **platform** reads the variables below from the environment (or `app/backendapi/.env` by default). The **feedback agent** uses a smaller set; only the platform needs database and Google OAuth for the main app.

### Platform (`app/backendapi/`) — core

| Variable | Required | Default / notes |
|----------|----------|-----------------|
| `DATABASE_URL` | Recommended for production | Default `sqlite:///./app.db` if unset |
| `GOOGLE_CLIENT_ID` | Yes (OAuth) | — |
| `GOOGLE_CLIENT_SECRET` | Yes (OAuth) | — |
| `GOOGLE_REDIRECT_URI` | Yes (OAuth) | Must match Google Cloud console (e.g. `http://localhost:8000/integrations/google/callback`) |
| `FRONTEND_BASE_URL` | Recommended | Next.js origin for post-OAuth redirect and legacy path redirects (e.g. `http://localhost:3000`) |
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

### Feedback agent (`app/agents/feedback/`)

These apply to the **feedback agent process** (or its container). At startup the agent loads, in order: the **current working directory** `.env` (usually the repo root when you run `uvicorn` from there), then **`app/agents/.env`** if it exists (overrides for agent-level secrets). Template: `app/agents/.env.example` — copy it to `app/agents/.env` (gitignored) or set the same variables in the repo root `.env`.

| Variable | Required | Default / notes |
|----------|----------|-----------------|
| `OPENAI_API_KEY` | **Yes** (for reviews) | Used by `openai_service` for storyboard and manual feedback |
| `OPENAI_MODEL` | No | `gpt-4.1-mini` |
| `DATA_DIR` | No | Defaults to `app/agents/feedback/data` under the package (persistent review storage) |
| `HOST` | No | `127.0.0.1` (used when building `review_url` if `FRONTEND_BASE_URL` and `PUBLIC_BASE_URL` are unset) |
| `PORT` | No | `5055` (match the port you pass to `uvicorn`) |
| `FRONTEND_BASE_URL` | Recommended | Next.js origin for share/review links (e.g. `http://localhost:3000` or `http://VM_IP:3000`). Docker also loads this from `app/backendapi/.env`. |
| `PUBLIC_BASE_URL` | No | Fallback origin if `FRONTEND_BASE_URL` is unset |

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
- `GET /`, `/app/*`, `/admin/*` (redirect to Next.js frontend when `FRONTEND_BASE_URL` is set)

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
