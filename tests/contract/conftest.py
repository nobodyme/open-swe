"""Contract-suite fixtures.

Everything in tests/contract/ carries the ``contract`` marker and is excluded
from the default run by ``pyproject.toml``'s ``addopts``. Run via
``make contract-test``.

``contract_server`` boots the golden baseline — ``langgraph dev`` serving the
deterministic contract graph (tests/contract/contract_graph.py) — once per
session on an ephemeral port. It deliberately does NOT use tests/e2e/harness.py
(server semantics need no webapp) and does NOT import tests/e2e modules
(docs/fast-api-migration/phase-0.md task 4a).
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

from tests.support.postgres import postgres_dsn  # noqa: F401  (session fixture re-export)

_CONTRACT_DIR = Path(__file__).parent
REPO_ROOT = _CONTRACT_DIR.parents[1]
_BOOT_TIMEOUT_S = 120.0


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # This hook fires for the whole session even from a nested conftest, so
    # scope the marker to items that actually live under tests/contract/.
    for item in items:
        if _CONTRACT_DIR in Path(item.fspath).parents:
            item.add_marker(pytest.mark.contract)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@dataclass(frozen=True)
class ContractServer:
    base_url: str
    port: int


def _fresh_database(admin_dsn: str) -> str:
    """CREATE DATABASE contract_<rand> on the compose Postgres; returns its DSN."""
    from typing import TYPE_CHECKING, cast

    import psycopg

    if TYPE_CHECKING:
        from psycopg.abc import QueryNoTemplate

    name = f"contract_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        conn.execute(cast("QueryNoTemplate", f'CREATE DATABASE "{name}"'))
    base, _, _ = admin_dsn.rpartition("/")
    return f"{base}/{name}"


@pytest.fixture(scope="session")
def contract_server(
    tmp_path_factory: pytest.TempPathFactory, request: pytest.FixtureRequest
) -> Iterator[ContractServer]:
    """The server under contract test.

    ``CONTRACT_RUNTIME=platform`` (default): ``langgraph dev`` — the golden
    baseline, exactly as Phase 0 shipped it.
    ``CONTRACT_RUNTIME=embedded``: ``uvicorn agent_runtime.app:app`` over a
    fresh Postgres database (phase-1.md T11) — must match the same goldens.

    Boots with cwd = a session tmpdir, because the inmem runtime resolves BOTH
    the graph paths and its persistence dir (``.langgraph_api``) against cwd.
    A fresh tmpdir per session gives an empty golden baseline without touching
    the developer's repo-root ``.langgraph_api`` (a concurrently running
    ``make dev`` keeps its state; goldens keep their fresh-server assumption).
    The generated config therefore uses absolute paths.
    """
    runtime = os.environ.get("CONTRACT_RUNTIME", "platform")
    workdir = tmp_path_factory.mktemp("contract-server")
    config_path = workdir / "langgraph.contract.json"
    config_path.write_text(
        json.dumps(
            {
                "python_version": "3.12",
                "dependencies": [str(REPO_ROOT)],
                "graphs": {"agent": f"{_CONTRACT_DIR / 'contract_graph.py'}:graph"},
            }
        )
    )
    port = _free_port()
    env = dict(os.environ)
    # The baseline must stay hermetic: no tracing/telemetry side channels.
    env.update(
        {
            "LANGSMITH_TRACING": "false",
            "LANGCHAIN_TRACING_V2": "false",
            "LANGGRAPH_CLI_NO_ANALYTICS": "1",
        }
    )
    if runtime == "embedded":
        admin_dsn = request.getfixturevalue("postgres_dsn")
        env.update(
            {
                "DATABASE_URL": _fresh_database(admin_dsn),
                "AGENT_RUNTIME_CONFIG": str(config_path),
                "AGENT_RUNTIME_NO_WEBAPP": "1",
                # Contract timing matches dev's fast inmem pickup; the queue
                # delay is pinned by its own runtime test.
                "AGENT_RUNTIME_PICKUP_DELAY_MS": "0",
            }
        )
        command = [
            "uv",
            "run",
            "--project",
            str(REPO_ROOT),
            "uvicorn",
            "agent_runtime.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ]
    else:
        command = [
            "uv",
            "run",
            "--project",
            str(REPO_ROOT),
            "langgraph",
            "dev",
            "--config",
            str(config_path),
            "--port",
            str(port),
            "--no-browser",
            "--allow-blocking",
            "--no-reload",
        ]
    # Server output goes to a file, not a PIPE: nothing drains a pipe during
    # the session, and a full pipe buffer would block the server mid-suite.
    log_path = workdir / "contract-server.log"
    with log_path.open("wb") as log_file:
        proc = subprocess.Popen(
            command,
            cwd=workdir,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    base_url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + _BOOT_TIMEOUT_S
    last_error: Exception | None = None
    try:
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"langgraph dev exited with {proc.returncode} during boot:\n"
                    f"{log_path.read_text(errors='replace')[-4000:]}"
                )
            try:
                response = httpx.get(f"{base_url}/ok", timeout=2.0)
                if response.status_code == 200:
                    break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
            time.sleep(0.5)
        else:
            proc.terminate()
            raise RuntimeError(
                f"langgraph dev not ready after {_BOOT_TIMEOUT_S}s: {last_error}\n"
                f"{log_path.read_text(errors='replace')[-4000:]}"
            )
        yield ContractServer(base_url=base_url, port=port)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=15)


@pytest.fixture()
def contract_client(contract_server: ContractServer):
    """SDK client against the golden server — the transport all of agent/ uses.

    Function-scoped on purpose: each test runs in its own event loop
    (``asyncio_mode = "auto"``), and a shared client would keep pooled
    connections bound to a closed loop.
    """
    from langgraph_sdk import get_client

    return get_client(url=contract_server.base_url)
