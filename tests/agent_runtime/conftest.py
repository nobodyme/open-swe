"""tests/agent_runtime fixtures (phase-1.md T1/D5).

The whole package skips cleanly when Postgres is unavailable (no
``TEST_POSTGRES_DSN`` and no Docker) — ``make test`` with Docker stopped
stays green. With Postgres, a real uvicorn server serves the runtime app on
an ephemeral port so tests drive it through the real ``langgraph_sdk``
client and real SSE (wire mistakes can't hide behind an ASGI shortcut).
"""

from __future__ import annotations

import asyncio
import os
import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import psycopg
import pytest

from tests.support.postgres import postgres_dsn  # noqa: F401  (session fixture re-export)

RUNTIME_DIR = Path(__file__).parent
REPO_ROOT = RUNTIME_DIR.parents[1]

_TRUNCATE_TABLES = [
    "rt_thread_event",
    "rt_run",
    "rt_cron",
    "rt_thread",
    "checkpoint_writes",
    "checkpoint_blobs",
    "checkpoints",
    "store",
]


@pytest.fixture(scope="session", autouse=True)
def runtime_env(postgres_dsn: str) -> Iterator[str]:  # noqa: F811 - pytest fixture-by-name
    """Session env: DATABASE_URL + test graph registry, no webapp mount.

    Depending on ``postgres_dsn`` makes the whole package skip when Postgres
    is unavailable (the fixture skips itself) — the D5 convention.
    """
    saved = {
        key: os.environ.get(key)
        for key in ("DATABASE_URL", "AGENT_RUNTIME_CONFIG", "AGENT_RUNTIME_NO_WEBAPP")
    }
    os.environ["DATABASE_URL"] = postgres_dsn
    os.environ["AGENT_RUNTIME_CONFIG"] = str(RUNTIME_DIR / "runtime.test.json")
    os.environ["AGENT_RUNTIME_NO_WEBAPP"] = "1"
    yield postgres_dsn
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


class RuntimeServer:
    def __init__(self, base_url: str, dsn: str) -> None:
        self.base_url = base_url
        self.dsn = dsn


@pytest.fixture(scope="session")
def runtime_server(runtime_env: str) -> Iterator[RuntimeServer]:
    """Real uvicorn serving agent_runtime.app on an ephemeral port."""
    import uvicorn

    from agent_runtime.app import create_app

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    config = uvicorn.Config(
        create_app(), host="127.0.0.1", port=port, log_level="warning", lifespan="on"
    )
    server = uvicorn.Server(config)

    def _serve() -> None:
        asyncio.run(server.serve())

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 60
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{base_url}/ok", timeout=2.0).status_code == 200:
                break
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(0.2)
    else:
        raise RuntimeError(f"agent_runtime server did not become ready: {last}")

    yield RuntimeServer(base_url, runtime_env)

    server.should_exit = True
    thread.join(timeout=15)


@pytest.fixture(autouse=True)
def clean_tables(runtime_env: str) -> Iterator[None]:
    """Per-test isolation: truncate runtime + checkpoint + store tables."""
    yield
    with psycopg.connect(runtime_env) as conn:
        existing = {
            row[0]
            for row in conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
            ).fetchall()
        }
        targets = [t for t in _TRUNCATE_TABLES if t in existing]
        if targets:
            from agent_runtime.db import query as _q

            conn.execute(_q(f"TRUNCATE {', '.join(targets)} CASCADE"))  # noqa: S608
        conn.commit()


@pytest.fixture()
def sdk_client(runtime_server: RuntimeServer) -> Any:
    """Real langgraph_sdk client against the runtime (T7 requirement)."""
    from langgraph_sdk import get_client

    return get_client(url=runtime_server.base_url)
