# Personal and shared context

How **personal** and **shared** player-memory context are stored, retrieved, and injected into the feedback agent.

Also see [`feedback-agents-yolo-pose.md`](./feedback-agents-yolo-pose.md) for switching YOLO / pose_api and plugging in feedback agent versions.

## Two scopes

Defined in `app/backendapi/services/context_scope.py`:

| Scope | Value | Player key | Meaning |
|-------|--------|------------|---------|
| Personal | `personal` | Sportal user id (string) | History / style for **one athlete** |
| Shared | `shared` | `__shared__` (`SHARED_PLAYER_KEY`) | Org-wide coaching rubric / vocabulary |

They live in **separate Firestore collections** and are formatted as **separate prompt blocks**. They are never merged into one blob before the LLM (except a legacy admin “test retrieval” helper that concatenates them for debugging).

---

## End-to-end flow

```
INGEST
  Personal  → Firestore collection (default: player_personal_context)
  Shared    → Firestore collection (default: shared_context)

RETRIEVE (worker, before the feedback agent runs)
  retrieve_feedback_context()
    → payload["player_memory_context"]   # personal text
    → payload["shared_context"]          # shared text

PROMPT (selected feedback agent → openai_service)
  --- PLAYER MEMORY ... ---              # personal only
  --- SHARED CLUB RUBRIC ... ---         # shared only

OPTIONAL WRITE-BACK
  embed completed review → personal chunks (source_type=feedback_review)
```

Agent Lab / feedback RAG uses the **admin memory workspace** (`user_id="0"`), not each coach’s personal workspace.

---

## 1. Where it is stored

### Firestore (vector store of record)

| Scope | Default collection | Config |
|-------|-------------------|--------|
| Personal | `player_personal_context` | `FIRESTORE_COLLECTION_PERSONAL` / settings `vector_collection_personal` |
| Shared | `shared_context` | `FIRESTORE_COLLECTION_SHARED` / settings `vector_collection_shared` |

Implementation:

- `app/backendapi/services/gcp_firestore_vector_store.py` — `_collection_name()` / search / insert
- `app/backendapi/services/vector_store.py` — `get_vector_store()`
- `app/backendapi/services/player_memory_service.py` — `insert_chunks`, `search_similar_chunks`, `list_chunks`

### Personal — how chunks get in

| Source | `source_type` | Code |
|--------|---------------|------|
| Sportal MySQL SQL sync | `sql_sync` | `sql_player_sync.py` → `structured_chunks_from_player_row()` (`player_context_chunker.py`) |
| Admin personal document upsert / reindex | `sql_sync` | `personal_context_store.py` — `upsert_personal_record()`, `reindex_personal_vectors()` |
| Manual admin note | `manual` | `admin.py` — personal manual endpoint |
| Completed feedback embed | `feedback_review` | `feedback_review_embed.py` — `embed_completed_feedback_review()` |
| Destination sheet snapshot rows | `sheet_row` | `snapshot_embed.py` |

Admin overlay (Postgres, **not** vectors): table `player_personal_context_overlays` (`models/player_memory.py`).  
`merged_document_for_player()` = Sportal SQL row + overlay → used for admin CRUD and reindex.

### Shared — how chunks get in

| Source | `source_type` | Code |
|--------|---------------|------|
| Google Sheet sync | `shared_sheet` | `shared_context_embed.py` — `sync_shared_context_from_sheet()` |
| Manual admin note | `manual` | `admin.py` — shared manual endpoint |

Sheet reader: `shared_feedback_context_sheet.py`  
Schema / embed text: `shared_context_schema.py` (`shared_context_v2`)  
Env: `FEEDBACK_SHARED_CONTEXT_SPREADSHEET_ID`, `FEEDBACK_SHARED_CONTEXT_SHEET_GID`, `DESTINATION_GOOGLE_CREDENTIALS_FILE`.

There is a non-vector helper `fetch_shared_feedback_context_text()` for full-sheet text; **feedback generation uses Firestore RAG**, not live sheet injection.

---

## 2. Where it is retrieved for feedback

### Core: `retrieve_feedback_context()`

File: `app/backendapi/services/feedback_memory.py`

1. Build query text from the job payload (`build_feedback_query_text` / `build_shared_context_query_text`).
2. Embed query (`embed_single_query`).
3. Personal search: `search_similar_chunks(..., player_key=<id>, context_scope=personal, top_k)`.
4. Shared search: `search_similar_chunks(..., player_key=__shared__, context_scope=shared, shared_top_k)`.
5. Format separately:
   - `format_retrieval_context()` → personal block
   - `format_shared_retrieval_context()` → shared block
6. Return `(personal_text, shared_text, debug_dict)`.

Legacy combine for admin testing: `retrieve_player_memory_context()` (shared then personal in one string).

### Attached on the feedback job

`_build_feedback_delegate_body()` in `app/backendapi/workers/workspace_worker.py`:

- Resolves `player_key` (payload or name lookup).
- Uses admin memory workspace `user_id="0"`.
- Sets:
  - `body["player_memory_context"]` ← **personal**
  - `body["shared_context"]` ← **shared**
  - `body["player_memory_retrieval_debug"]`
  - `body["shared_context_retrieval_debug"]` (agent paths also accept `shared_context_sheet_debug` for generation_debug)

Then either:

- **In-process (default):** `run_feedback_review_inprocess()` → pluggable agent (`FEEDBACK_AGENT_VERSION`)
- **HTTP (legacy):** `FEEDBACK_DELEGATE_HTTP=true` → POST to feedback agent `/api/reviews`

Agent Lab RAG chat (separate from job generation): `rag_chat.py` — `retrieve_knowledge_for_query()` / `run_rag_chat()` with `include_shared`.

---

## 3. How the feedback agent uses it

Handoff:

1. Worker attaches the two strings on the payload.
2. Selected agent (`agents.feedback` v1 today) reads:
   - `player_memory_context`
   - `shared_context`
3. `review_agent.py` (`build_review`, `build_review_from_pose_json`, …) forwards both into `openai_service.py`.
4. Prompts keep them as **distinct sections**:

| Prompt role | Personal | Shared |
|-------------|----------|--------|
| Moment selection / vision / letter | `PLAYER MEMORY` — this athlete’s continuity / style | `SHARED CLUB RUBRIC` — org standards / vocabulary |
| Conflict rule | Prefer **video evidence** when memory conflicts with what is on screen | Do **not** invent events from the rubric |

Debug is stored on the review under `generation_debug.player_memory_vector_retrieval` and `generation_debug.shared_context_sheet`.

In-process entry: `agents/feedback/agent_entry.py` (`FeedbackAgentV1`) via `agents.registry.get_feedback_agent()`.  
HTTP twin: `agents/feedback/job_runners.py` (same fields).

---

## 4. Admin UI / Agent Lab surfaces

### Frontend (`athlete-agent-frontend`)

| Page | Path | Role |
|------|------|------|
| Personal | `src/app/admin/player-memory/personal/page.tsx` | Players, chunks, SQL sync, manual personal text |
| Shared | `src/app/admin/player-memory/shared/page.tsx` | Shared chunks, sheet sync, manual shared text |
| Settings | `src/app/admin/player-memory/settings/page.tsx` | SQL, collections, top_k, GCP |
| Agent Lab | `src/app/admin/agents-lab/page.tsx` + `rag-chat-panel.tsx` | Run feedback + RAG chat (`include_shared`) |
| API client | `src/lib/api/admin.ts` | `adminApi.playerMemory.*`, agents-lab embed/RAG |

### Backend admin API

`app/backendapi/api/routes/admin.py` — settings, test SQL/vector/retrieval, chunks CRUD with `context_scope`, personal records, shared sheet sync, Agent Lab feedback jobs + `embed-to-player-memory`.

Token agent API: `app/backendapi/api/routes/player_memory.py` — `/agents/player-memory/*`.

---

## 5. Key env / settings

| Variable | Purpose |
|----------|---------|
| `PLAYER_MEMORY_VECTOR_BACKEND` | Must be `firestore` |
| `GCP_PROJECT_ID` / `GCP_FIRESTORE_DATABASE` | Firestore target |
| `FIRESTORE_COLLECTION_PERSONAL` / `_SHARED` | Collection names |
| `PLAYER_MEMORY_EMBEDDING_MODEL` / `_DIM` | Embeddings |
| `PLAYER_MEMORY_TOP_K` | Personal RAG k (default 12) |
| `PLAYER_MEMORY_SHARED_TOP_K` | Shared RAG k (default 6) |
| `PLAYER_MEMORY_CONTEXT_MAX_CHARS` / `_SHARED_CONTEXT_MAX_CHARS` | Prompt block caps |
| `PLAYER_CONTEXT_DATABASE_URL` / `PLAYER_CONTEXT_SQL` | Sportal personal sync |
| `FEEDBACK_SHARED_CONTEXT_SPREADSHEET_ID` / `_SHEET_GID` | Shared sheet source |
| `PLAYER_MEMORY_SETTINGS_MASTER_KEY` | Encrypt DB settings row |

Runtime overrides also live in encrypted `player_memory_settings` (Admin → Player Memory → Settings).

---

## Quick mental model

- **Personal** = “what do we already know about **this player**?”
- **Shared** = “what does **the club** call good play / what rubric applies?”
- Both are retrieved as vectors, attached on the job payload, and injected as separate sections into the feedback agent prompts. Swapping the feedback agent version (`FEEDBACK_AGENT_VERSION`) does not change this retrieval step — only how the agent consumes the payload.
