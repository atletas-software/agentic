from __future__ import annotations

import hashlib
import os
from typing import Sequence

from openai import OpenAI


def embedding_dimensions() -> int:
    return int(os.getenv("PLAYER_MEMORY_EMBEDDING_DIM", "3072"))


def embedding_model_name() -> str:
    return os.getenv("PLAYER_MEMORY_EMBEDDING_MODEL", "text-embedding-3-large")


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Batch embedding via OpenAI; raises if API key missing."""
    if not texts:
        return []
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for player memory embeddings.")
    client = OpenAI(api_key=api_key)
    model = embedding_model_name()
    dim = embedding_dimensions()
    kwargs: dict[str, object] = {"model": model, "input": texts}
    # text-embedding-3-* supports an explicit dimensions parameter (must match DB vector column).
    kwargs["dimensions"] = dim
    resp = client.embeddings.create(**kwargs)
    out: list[list[float]] = []
    data_list = sorted(resp.data, key=lambda d: d.index)
    for item in data_list:
        vec = list(item.embedding)
        if len(vec) > dim:
            vec = vec[:dim]
        out.append(vec)
    return out


def embed_single_query(text: str) -> list[float]:
    rows = embed_texts([text])
    return rows[0] if rows else []
