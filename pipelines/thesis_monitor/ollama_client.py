"""Thin wrapper for the Studio Ollama daemon.

Default model: glm-4.7-flash:latest (verified on Studio).
"""
from __future__ import annotations

import logging
import os

import httpx

log = logging.getLogger(__name__)

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
DEFAULT_MODEL = os.getenv("THESIS_MONITOR_MODEL", "glm-4.7-flash:latest")
TIMEOUT_S = 180


def summarize(system_prompt: str, user_content: str, *, model: str = DEFAULT_MODEL) -> str:
    """Call Ollama /api/chat. Returns the assistant text."""
    payload = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "options": {
            "temperature": 0.3,
            "num_ctx": 8192,
        },
    }
    try:
        resp = httpx.post(f"{OLLAMA_HOST}/api/chat", json=payload, timeout=TIMEOUT_S)
        resp.raise_for_status()
        data = resp.json()
        return data.get("message", {}).get("content", "").strip()
    except httpx.HTTPError as exc:
        log.error("Ollama call failed: %s", exc)
        return f"[LLM summarization failed: {exc}. Raw data follows.]"
