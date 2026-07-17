"""Startup sweep (phase-1.md T6 step 6, decision D2).

Orphans (runs left pending/running by a dead process) are marked ``error`` —
NOT ``interrupted`` — so agent/completion.py posts the failure reply
(``_TERMINAL_FAILURE_STATUSES``). Both sides of the split are pinned here.
"""

from __future__ import annotations

import asyncio
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import psycopg
import pytest


class _SweepReceiver(BaseHTTPRequestHandler):
    received: list[dict[str, Any]] = []

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        type(self).received.append(json.loads(self.rfile.read(length) or b"{}"))
        self.send_response(200)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass


@pytest.fixture()
def sweep_receiver() -> Any:
    _SweepReceiver.received = []
    server = HTTPServer(("127.0.0.1", 0), _SweepReceiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)


async def test_startup_sweep_marks_orphans_error_and_fires_webhook(
    runtime_env: str, runtime_server: Any, sweep_receiver: Any
) -> None:
    """Seed a 'running' rt_run (as a dead process would leave it), run the
    sweep as boot would, assert error + exactly one failure webhook."""
    thread_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    hook = f"http://127.0.0.1:{sweep_receiver.server_address[1]}/webhooks/run-complete?token=s"
    with psycopg.connect(runtime_env) as conn:
        conn.execute("INSERT INTO rt_thread (thread_id, status) VALUES (%s, 'busy')", (thread_id,))
        conn.execute(
            "INSERT INTO rt_run (run_id, thread_id, assistant_id, status, kwargs) "
            "VALUES (%s, %s, 'echo', 'running', %s::jsonb)",
            (run_id, thread_id, json.dumps({"webhook": hook})),
        )
        conn.commit()

    # Run the sweep the same way the lifespan does, against a fresh executor
    # wired to the same database (a second boot of the runtime).
    from agent_runtime import db as runtime_db
    from agent_runtime.config import runtime_config_path
    from agent_runtime.executor import RunExecutor
    from agent_runtime.registry import GraphRegistry
    from agent_runtime.runs_repo import RunsRepo
    from agent_runtime.threads_repo import ThreadsRepo

    pool = runtime_db.create_pool(runtime_env)
    await pool.open(wait=True)
    try:
        executor = RunExecutor(
            pool=pool,
            saver=None,
            registry=GraphRegistry(runtime_config_path()),
            threads=ThreadsRepo(pool),
            runs=RunsRepo(pool),
        )
        swept = await executor.sweep_orphans_on_boot()
        assert swept == 1
    finally:
        await pool.close()

    with psycopg.connect(runtime_env) as conn:
        run_row = conn.execute("SELECT status FROM rt_run WHERE run_id = %s", (run_id,)).fetchone()
        thread_row = conn.execute(
            "SELECT status FROM rt_thread WHERE thread_id = %s", (thread_id,)
        ).fetchone()
    assert run_row is not None and thread_row is not None
    run_status = run_row[0]
    thread_status = thread_row[0]
    assert run_status == "error"
    assert thread_status == "error"

    for _ in range(50):
        if _SweepReceiver.received:
            break
        await asyncio.sleep(0.1)
    assert len(_SweepReceiver.received) == 1
    payload = _SweepReceiver.received[0]
    assert payload["run_id"] == run_id
    assert payload["status"] == "error"


async def test_completion_receiver_ignores_interrupted() -> None:
    """The other side of D2: agent/completion.py treats 'interrupted' as the
    healthy multitask path and must NOT post a failure reply for it."""
    from agent import completion

    assert "interrupted" not in completion._TERMINAL_FAILURE_STATUSES
    assert {"error", "timeout"} == set(completion._TERMINAL_FAILURE_STATUSES)

    result = await completion.handle_run_completion(
        {"run_id": str(uuid.uuid4()), "thread_id": str(uuid.uuid4()), "status": "interrupted"}
    )
    # Exactly "ignored" — "ok" would mean a failure reply was POSTED, which
    # is the regression this test exists to catch (adversarial finding 15c).
    assert result.get("status") == "ignored", result
    assert "non-failure" in result.get("reason", ""), result
