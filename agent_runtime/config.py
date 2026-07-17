"""Runtime configuration (env-driven; see phase-1.md D4 for the names)."""

from __future__ import annotations

import os
from pathlib import Path


def database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is required (postgresql://... for the runtime's Postgres)")
    return url


def runtime_config_path() -> Path:
    """Path to the langgraph.json-shaped config declaring graphs + http.app."""
    return Path(os.environ.get("AGENT_RUNTIME_CONFIG", "langgraph.json")).resolve()
