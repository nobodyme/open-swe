"""The runtime executes the REAL deep-agent factory (phase-1.md T12).

Boots ``fake_boundary_app.py`` in a subprocess (e2e patches are process-
poisonous — Phase 0 ledger), drives one scripted run over HTTP through the
real ``agent.graphs.agent:traced_agent`` entrypoint, and asserts the deep-
agent loop actually ran: scripted tool calls in state, terminal success,
checkpoints served by the Postgres saver.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def boundary_app(postgres_dsn: str) -> Any:
    import psycopg

    db_name = f"boundary_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        from agent_runtime.db import query as _q

        conn.execute(_q(f'CREATE DATABASE "{db_name}"'))
    base, _, _ = postgres_dsn.rpartition("/")

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    env = dict(os.environ)
    env.update(
        {
            "BOUNDARY_APP_PORT": str(port),
            "DATABASE_URL": f"{base}/{db_name}",
            "LANGSMITH_TRACING": "false",
            "LANGCHAIN_TRACING_V2": "false",
        }
    )
    log_path = Path(os.environ.get("TMPDIR", "/tmp")) / f"boundary-app-{port}.log"
    with log_path.open("wb") as log_file:
        proc = subprocess.Popen(
            [sys.executable, str(Path(__file__).parent / "fake_boundary_app.py")],
            cwd=REPO_ROOT,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    base_url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 120
    try:
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"boundary app died during boot:\n{log_path.read_text(errors='replace')[-4000:]}"
                )
            try:
                if httpx.get(f"{base_url}/ok", timeout=2.0).status_code == 200:
                    break
            except Exception:  # noqa: BLE001, S110
                pass
            time.sleep(0.5)
        else:
            proc.terminate()
            raise RuntimeError(
                f"boundary app not ready:\n{log_path.read_text(errors='replace')[-4000:]}"
            )
        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()


async def test_real_agent_factory_via_registry(boundary_app: str) -> None:
    from langgraph_sdk import get_client

    client: Any = get_client(url=boundary_app)
    thread_id = str(uuid.uuid4())
    await client.threads.create(thread_id=thread_id, metadata={"source": "runtime-test"})

    run_config: Any = {
        "configurable": {
            "__is_for_execution__": True,
            # Minimal auth context (resolve_github_token requires a
            # source; the patched boundary supplies the dummy tokens).
            "source": "slack",
            "user_email": "dev@example.com",
        }
    }
    run = await client.runs.create(
        thread_id,
        "agent",
        input={
            "messages": [{"role": "user", "content": "Please add a greet() helper to the repo."}]
        },
        config=run_config,
        if_not_exists="create",
        multitask_strategy="interrupt",
        durability="sync",
    )
    await client.runs.join(thread_id, run["run_id"])

    final = await client.runs.get(thread_id, run["run_id"])
    assert final["status"] == "success", final

    state = await client.threads.get_state(thread_id)
    values: Any = state["values"]
    messages = values["messages"]
    assert len(messages) >= 4  # human + scripted tool loop + final reply
    tool_calls = [
        call["name"]
        for m in messages
        if m.get("type") == "ai"
        for call in (m.get("tool_calls") or [])
    ]
    # The e2e fake model's implement script drives real tools through the
    # real deepagents loop (slack ack → execute → open PR → reply).
    assert "slack_thread_reply" in tool_calls, tool_calls
    assert "execute" in tool_calls, tool_calls

    # The webapp is mounted per D1 (real langgraph.json http.app): a webapp
    # route answers on the same origin (unauthenticated → 401, not 404).
    async with httpx.AsyncClient(base_url=boundary_app, timeout=10.0) as http:
        me = await http.get("/dashboard/api/me")
        assert me.status_code in (200, 401, 403), me.status_code
