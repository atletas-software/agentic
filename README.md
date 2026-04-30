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

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Run worker (separate process):

```bash
python -m app.workers.run_worker
```

## Run with Docker (Production)

1) Prepare production env file:

```bash
cp .env.docker.example .env.docker
```

2) Update `.env.docker` with strong production values:

- `POSTGRES_PASSWORD` -> set a strong password
- `DATABASE_URL_DOCKER` -> keep aligned with Postgres values
- `REDIS_URL_DOCKER` -> usually `redis://redis:6379/0`
- `GOOGLE_CREDENTIALS_FILE_HOST` -> host path to your service-account JSON
- `WEB_CONCURRENCY` -> tune based on CPU (start with `2`)

3) Ensure your app env values are present in `.env` (OAuth, destination sheet, JWT, etc.).

4) Start the full stack:

```bash
docker compose --env-file .env.docker up -d --build
```

This starts:
- `api` (FastAPI app)
- `worker` (`python -m app.workers.run_worker`)
- `redis`
- `postgres`

5) Verify deployment:

```bash
docker compose ps
docker compose logs -f api
docker compose logs -f worker
```

Health check endpoint:
- [http://localhost:8000/health](http://localhost:8000/health)

Stop stack:

```bash
docker compose --env-file .env.docker down
```

### Production safety notes

- Do not commit real `.env`, `.env.docker`, or `credentials.json`.
- `credentials.json` is mounted read-only into containers at `/run/secrets/google-credentials.json`.
- Use a managed Postgres/Redis and a secret manager in cloud production when possible.

Open backend UI pages:
- [http://localhost:8000/app/connect](http://localhost:8000/app/connect)
- [http://localhost:8000/app/sheets](http://localhost:8000/app/sheets)
- [http://localhost:8000/app/sheets/details?spreadsheet_id=YOUR_ID](http://localhost:8000/app/sheets/details?spreadsheet_id=YOUR_ID)

## Environment variables

- `DATABASE_URL` (Postgres recommended in production)
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_REDIRECT_URI`
- `SESSION_TTL_DAYS` (default: `30`)
- `SYNC_POLL_ENABLED` (default: `true`)
- `SYNC_POLL_TICK_SECONDS` (default: `30`, scheduler heartbeat)
- `REDIS_URL` (default: `redis://localhost:6379/0`)
- `SYNC_QUEUE_NAME` (default: `sheet-sync`)
- `SYNC_QUOTA_BACKOFF_BASE_MINUTES` (default: `1`)
- `SYNC_QUOTA_BACKOFF_MAX_MINUTES` (default: `60`)
- `DESTINATION_GOOGLE_CREDENTIALS_FILE` (default: `credentials.json`)
- `DESTINATION_SPREADSHEET_ID` (required for polling sync)
- `DESTINATION_USER_SHEET_PREFIX` (default: `user`, destination tab name becomes `<prefix>_<user_id_sanitized>`)
- `SOURCE_FIELD_MAP` (optional JSON map for raw source-row headers -> normalized record fields)
- `SOURCE_SHEET_ID` (optional; if missing, status updates run in mock mode)
- `SOURCE_SHEET_NAME` (default: `Sheet1`)
- Optional source status column names:
  - `SOURCE_STATUS_COLUMN`
  - `SOURCE_ERROR_COLUMN`
  - `SOURCE_ATTEMPTS_COLUMN`
  - `SOURCE_LAST_PROCESSED_COLUMN`
  - `SOURCE_FEEDBACK_LINK_COLUMN`
  - `SOURCE_JOB_ID_COLUMN`

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
