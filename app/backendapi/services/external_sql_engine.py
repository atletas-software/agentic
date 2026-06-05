from __future__ import annotations

from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def normalize_external_db_url(raw_db_url: str) -> str:
    url = (raw_db_url or "").strip()
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    if url.startswith("mysql://"):
        return url.replace("mysql://", "mysql+pymysql://", 1)
    return url


@lru_cache(maxsize=8)
def get_external_sql_engine(database_url: str) -> Engine | None:
    """Shared pooled engine for Sportal MySQL — avoids 'Too many connections' per request."""
    normalized = normalize_external_db_url(database_url)
    if not normalized:
        return None
    return create_engine(
        normalized,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=300,
    )
