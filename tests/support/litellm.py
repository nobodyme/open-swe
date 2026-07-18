"""Opt-in access to the locally running LiteLLM proxy for LLM-in-the-loop tests.

Reads ``LITELLM_BASE_URL`` / ``LITELLM_API_KEY`` / ``LITELLM_MODEL`` from the
environment, falling back to the repo ``.env`` (which no ``agent/`` code
consumes — verified in docs/fast-api-migration/phase-0.md task 4c). The key is
never logged or echoed. Tests using this module carry ``@pytest.mark.litellm``
and are excluded from the default run by ``pyproject.toml`` ``addopts`` —
they must never call paid cloud LLM APIs.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

_ENV_FILE = Path(__file__).resolve().parents[2] / ".env"
_KEYS = ("LITELLM_BASE_URL", "LITELLM_API_KEY", "LITELLM_MODEL")


def _read_env_file() -> dict[str, str]:
    values: dict[str, str] = {}
    if not _ENV_FILE.exists():
        return values
    for line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip("'\"")
    return values


def litellm_settings() -> dict[str, str]:
    """Resolve the three LiteLLM settings or skip the calling test."""
    file_values = _read_env_file()
    settings = {key: os.environ.get(key) or file_values.get(key, "") for key in _KEYS}
    missing = [key for key, value in settings.items() if not value]
    if missing:
        pytest.skip(f"LiteLLM proxy not configured (missing {', '.join(missing)})")
    return settings


def litellm_chat_model():
    """A real chat model pointed at the local LiteLLM proxy (OpenAI wire)."""
    from langchain_openai import ChatOpenAI
    from pydantic import SecretStr

    settings = litellm_settings()
    base_url = settings["LITELLM_BASE_URL"].rstrip("/")
    if not base_url.endswith("/v1"):
        # LiteLLM proxies serve the OpenAI-compatible surface under /v1.
        base_url = f"{base_url}/v1"
    return ChatOpenAI(
        model=settings["LITELLM_MODEL"],
        base_url=base_url,
        api_key=SecretStr(settings["LITELLM_API_KEY"]),
        temperature=0,
        timeout=120,
        max_retries=1,
    )
