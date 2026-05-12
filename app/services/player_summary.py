from __future__ import annotations

import os

from openai import OpenAI


def summarize_player_context(*, player_key: str, raw_context: str) -> str:
    text = (raw_context or "").strip()
    if not text:
        return ""
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return text
    model = (os.getenv("PLAYER_MEMORY_SUMMARY_MODEL") or "gpt-4.1-mini").strip()
    max_chars = int(os.getenv("PLAYER_MEMORY_SUMMARY_INPUT_MAX_CHARS", "18000"))
    trimmed = text[:max_chars]
    prompt = (
        "You are summarizing a player's historical profile for coaching feedback generation.\n"
        "Return concise factual bullet points only. Include identity, club context, evaluations, videos, "
        "recent updates, and notable patterns. Do not invent details.\n\n"
        f"player_key: {player_key}\n"
        f"raw_data:\n{trimmed}"
    )
    client = OpenAI(api_key=api_key)
    resp = client.responses.create(
        model=model,
        input=prompt,
        max_output_tokens=900,
    )
    summary = (resp.output_text or "").strip()
    return summary or trimmed
