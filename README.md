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
  agents/        pluggable feedback agents — Python package `agents` (v1 = feedback/, v2 template = feedback_v2/)
  yolo_model/    YOLO training + pose pipeline — Python package `yolo_model`
  pose_api/      optional GPU pose HTTP service
```

Set **`PYTHONPATH=app`** (or `export PYTHONPATH="${PWD}/app"` from the repo root) when running locally.

Default runtime is **two processes** (feedback runs **in-process** on the worker via `FEEDBACK_AGENT_VERSION`):

1. **Platform API** (`app/backendapi/`) — sheets, sync, auth, workspace/agent job orchestration.
2. **RQ worker** — sheet sync + feedback jobs (`get_feedback_agent().run_review(...)`).

Optional extras:

- **Standalone feedback HTTP** (`uvicorn agents.feedback.main:app` on `:5055`) — only if `FEEDBACK_DELEGATE_HTTP=true` and `FEEDBACK_AGENT_BASE_URL` are set (legacy).
- **Pose API** (`app/pose_api`) — only if `FEEDBACK_USE_POSE_PIPELINE=true` and usually `POSE_API_BASE_URL`.

See [`docs/feedback-agents-yolo-pose.md`](docs/feedback-agents-yolo-pose.md) for switching YOLO / pose and plugging in agent versions.

## Architecture and processes

At a high level, local or production setups involve these processes:

| Process | Role |
|--------|------|
| **Platform API** (`uvicorn backendapi.main:app`) | HTTP API, OAuth, enqueueing sync jobs, workspace context refresh, agent job creation. Port **8000**. |
| **RQ worker** (`python -m backendapi.workers.run_worker`) | Redis queues for sheet sync + workspace/feedback jobs. Runs the selected feedback agent **in-process** by default (`FEEDBACK_AGENT_VERSION`). |
| **Redis** | Job queue backend for RQ. |
| **Postgres** (or SQLite for quick dev) | Application database (`DATABASE_URL`). |
| **Feedback HTTP agent** (`uvicorn agents.feedback.main:app`) | **Legacy / optional.** Only when `FEEDBACK_DELEGATE_HTTP=true` and `FEEDBACK_AGENT_BASE_URL` are set. Port **5055**. |
| **Pose API** (`uvicorn pose_api.main:app`) | **Optional.** GPU pose when `FEEDBACK_USE_POSE_PIPELINE=true`. Port **5060**. |

Typical flow:

1. User connects Google Sheets and sync runs (worker + destination sheet).
2. After a successful sync, the worker may enqueue a **workspace context** job.
3. When a **feedback** `AgentJob` is created, the worker attaches personal/shared RAG context and calls `get_feedback_agent().run_review(...)` (unless legacy HTTP delegate is enabled).

## Configuration (local and Docker use the same files)

| File | Purpose |
|------|--------|
| **`app/backendapi/.env`** | Primary application config: OAuth, `DATABASE_URL`, `REDIS_URL`, player memory, destination sheet, `FEEDBACK_AGENT_VERSION`, YOLO/pose toggles. Used by `make run-api` / `make run-worker` and mounted into API/worker containers. |
| **`.env`** (repo root, optional) | Copy from **`.env.example`**. Compose **interpolation** only (ports, credentials path, Cloud SQL). |

At runtime, the platform loads **`app/backendapi/.env` first**, then merges **repo-root `.env`** only for keys that are still unset (`override=False`).

For Docker, set in-cluster URLs in **`app/backendapi/.env`**: `REDIS_URL=redis://redis:6379/0`, and `DATABASE_URL` pointing at Cloud SQL (private IP or `cloud-sql-proxy` — see **GCP VM** below). Feedback runs in-process on the worker by default (`FEEDBACK_AGENT_VERSION=v1`). Only set `FEEDBACK_AGENT_BASE_URL` + `FEEDBACK_DELEGATE_HTTP=true` if you intentionally use the legacy HTTP agent.

Player memory SQL/vector runtime settings are managed from the admin panel (`/admin/player-memory`) and persisted encrypted in the DB (Firestore). Keep `PLAYER_MEMORY_SETTINGS_MASTER_KEY` set in `app/backendapi/.env`.

## Frontend (Next.js)

The browser UI lives in **`athlete-agent-frontend/`** (sibling repo / Next.js 15). Run it alongside the platform API:

```bash
cd athlete-agent-frontend && cp .env.example .env.local && npm install && npm run dev
```

Set `FRONTEND_BASE_URL=http://localhost:3000` in `app/backendapi/.env`.

## Run locally (venv)

Prerequisites: **Python 3.11+**, **Redis** (e.g. `brew services start redis`), and **`app/backendapi/.env`** from `app/backendapi/.env.example`.

- **General platform dev:** SQLite works if `DATABASE_URL` is omitted (defaults to `sqlite:///./app.db`).
- **Player vector memory:** Firestore (see admin Player Memory settings). App DB can be Postgres or SQLite for local platform data.

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
cd athlete-agent-frontend && npm run dev   # UI on http://localhost:3000
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

## Local development: platform (default — in-process feedback)

Prerequisites for feedback: **ffmpeg** on `PATH`, and **`OPENAI_API_KEY`** in `app/backendapi/.env`.

**Terminal 1 — platform API:**

```bash
source .venv-app/bin/activate
uvicorn backendapi.main:app --reload --host 0.0.0.0 --port 8000
```

**Terminal 2 — RQ worker** (runs the selected feedback agent in-process):

```bash
source .venv-app/bin/activate
python -m backendapi.workers.run_worker
```

```bash
# app/backendapi/.env
FEEDBACK_AGENT_VERSION=v1
# FEEDBACK_DELEGATE_HTTP=false   # default — do not require :5055
```

### Legacy: separate feedback HTTP agent

Only if you set `FEEDBACK_DELEGATE_HTTP=true` and `FEEDBACK_AGENT_BASE_URL=http://127.0.0.1:5055`:

```bash
make setup-agents
PYTHONPATH=app uvicorn agents.feedback.main:app --host 0.0.0.0 --port 5055
# or: make run-feedback
```

## Run with Docker (same `app/backendapi/.env` + optional repo-root `.env`)

1) Copy templates:

```bash
cp app/backendapi/.env.example app/backendapi/.env
cp .env.example .env
```

Edit **`app/backendapi/.env`** with OAuth, secrets, and Docker URLs (`REDIS_URL`, `DATABASE_URL`). Keep `FEEDBACK_AGENT_VERSION=v1` unless you have implemented another agent.

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
| `FEEDBACK_AGENT_VERSION` | No | `v1` (default). Set `v2` after implementing `agents/feedback_v2`. See `docs/feedback-agents-yolo-pose.md`. |
| `FEEDBACK_AGENT_BASE_URL` | No | Only for legacy HTTP delegate (`FEEDBACK_DELEGATE_HTTP=true`), e.g. `http://127.0.0.1:5055` |
| `FEEDBACK_DELEGATE_HTTP` | No | `false` (default). In-process feedback does not need `:5055`. |
| `FEEDBACK_AGENT_HTTP_TIMEOUT_SECONDS` | No | `30` when using HTTP delegate |

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

### Feedback agent (in-process v1 / optional HTTP)

OpenAI and video settings for **in-process** feedback live in **`app/backendapi/.env`** (same process as the worker). For the **legacy** standalone HTTP agent (`make run-feedback`), use the same variables in the environment or a local `.env` next to where you start uvicorn.

| Variable | Required | Default / notes |
|----------|----------|-----------------|
| `OPENAI_API_KEY` | **Yes** (for reviews) | Used by `openai_service` |
| `OPENAI_MODEL` | No | `gpt-4.1-mini` |
| `FEEDBACK_AGENT_VERSION` | No | `v1` — see `docs/feedback-agents-yolo-pose.md` |
| `DATA_DIR` | No | Defaults to `app/agents/feedback/data` |
| `FRONTEND_BASE_URL` | Recommended | Next.js origin for share/review links |

**System dependency:** `ffmpeg` must be installed on the host (or worker image) for video frame extraction.

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
