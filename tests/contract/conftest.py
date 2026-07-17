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


@pytest.fixture(scope="session")
def contract_server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[ContractServer]:
    """``langgraph dev`` serving the contract graph on an ephemeral port.

    Boots with cwd = a session tmpdir, because the inmem runtime resolves BOTH
    the graph paths and its persistence dir (``.langgraph_api``) against cwd.
    A fresh tmpdir per session gives an empty golden baseline without touching
    the developer's repo-root ``.langgraph_api`` (a concurrently running
    ``make dev`` keeps its state; goldens keep their fresh-server assumption).
    The generated config therefore uses absolute paths.
    """
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
    # Server output goes to a file, not a PIPE: nothing drains a pipe during
    # the session, and a full pipe buffer would block the server mid-suite.
    log_path = workdir / "langgraph-dev.log"
    with log_path.open("wb") as log_file:
        proc = subprocess.Popen(
            [
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
            ],
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
