from __future__ import annotations

CONTEXT_SCOPE_PERSONAL = "personal"
CONTEXT_SCOPE_SHARED = "shared"
SHARED_PLAYER_KEY = "__shared__"

VALID_CONTEXT_SCOPES = frozenset({CONTEXT_SCOPE_PERSONAL, CONTEXT_SCOPE_SHARED})


def normalize_context_scope(raw: str | None, *, default: str = CONTEXT_SCOPE_PERSONAL) -> str:
    s = (raw or default).strip().lower()
    return s if s in VALID_CONTEXT_SCOPES else default
