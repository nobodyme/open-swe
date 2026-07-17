"""Chaos-suite fixtures (phase-2.md T3, decisions D4/D5).

Gated twice: skipped without Docker/TEST_POSTGRES_DSN (the Phase 1
convention) AND without an explicit ``RUN_CHAOS=1`` opt-in — chaos runs are
slow and process-killing; they never ride ``make test``. The runtime under
chaos runs as a SUBPROCESS so SIGKILL is real.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from tests.support.postgres import postgres_dsn  # noqa: F401  (session fixture re-export)

CHAOS_DIR = Path(__file__).parent
REPO_ROOT = CHAOS_DIR.parents[1]


@pytest.fixture(scope="session", autouse=True)
def chaos_gate() -> None:
    if os.environ.get("RUN_CHAOS") != "1":
        pytest.skip("chaos suite is opt-in: RUN_CHAOS=1 uv run pytest tests/chaos/")


class ChaosRuntime:
    """A killable agent_runtime subprocess over a dedicated database."""

    def __init__(self, dsn: str, port: int, log_path: Path) -> None:
        self.dsn = dsn
        self.port = port
        self.base_url = f"http://127.0.0.1:{port}"
        self.log_path = log_path
        self._proc: subprocess.Popen[bytes] | None = None

    def start(self, *, extra_env: dict[str, str] | None = None) -> None:
        env = dict(os.environ)
        env.update(
            {
                "DATABASE_URL": self.dsn,
                "AGENT_RUNTIME_CONFIG": str(CHAOS_DIR / "chaos.config.json"),
                "AGENT_RUNTIME_NO_WEBAPP": "1",
                "AGENT_RUNTIME_PICKUP_DELAY_MS": "0",
                "LANGSMITH_TRACING": "false",
                "LANGCHAIN_TRACING_V2": "false",
                **(extra_env or {}),
            }
        )
        log_file = self.log_path.open("ab")
        self._proc = subprocess.Popen(
            [
                "uv",
                "run",
                "--project",
                str(REPO_ROOT),
                "uvicorn",
                "agent_runtime.app:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-level",
                "warning",
            ],
            cwd=REPO_ROOT,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            # Own process group: sigkill() uses killpg to take uv's uvicorn
            # child down too — without this it would kill pytest itself.
            start_new_session=True,
        )
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if self._proc.poll() is not None:
                raise RuntimeError(
                    f"chaos runtime died during boot:\n"
                    f"{self.log_path.read_text(errors='replace')[-3000:]}"
                )
            try:
                if httpx.get(f"{self.base_url}/ok", timeout=1.0).status_code == 200:
                    return
            except Exception:  # noqa: BLE001, S110
                pass
            time.sleep(0.2)
        raise RuntimeError("chaos runtime not ready in 90s")

    def sigkill(self) -> None:
        """The whole point: no graceful shutdown, no lifespan teardown."""
        assert self._proc is not None
        # uv run may wrap uvicorn: kill the process group to take the worker
        # down with it.
        try:
            os.killpg(os.getpgid(self._proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            self._proc.kill()
        self._proc.wait(timeout=15)
        self._proc = None
        # The advisory single-worker lock is held by the dead backend until
        # Postgres notices; give restarts a beat.
        time.sleep(1.0)

    def stop(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        self._proc = None


@pytest.fixture()
def chaos_runtime(postgres_dsn: str, tmp_path: Path) -> Iterator[ChaosRuntime]:  # noqa: F811
    import uuid

    import psycopg

    db_name = f"chaos_{uuid.uuid4().hex[:10]}"
    from agent_runtime.db import query

    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute(query(f'CREATE DATABASE "{db_name}"'))
    base, _, _ = postgres_dsn.rpartition("/")

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    runtime = ChaosRuntime(f"{base}/{db_name}", port, tmp_path / "chaos-runtime.log")
    runtime.start()
    try:
        yield runtime
    finally:
        runtime.stop()
        # Don't leak databases on a long-lived TEST_POSTGRES_DSN Postgres.
        with psycopg.connect(postgres_dsn, autocommit=True) as conn:
            conn.execute(query(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'))
