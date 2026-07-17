"""Runs API + executor semantics (phase-1.md T6/T7), via the real SDK client."""

from __future__ import annotations

import asyncio
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest


def _tid() -> str:
    return str(uuid.uuid4())


class _CaptureReceiver(BaseHTTPRequestHandler):
    received: list[dict[str, Any]] = []

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length) or b"{}")
        type(self).received.append({"path": self.path, "payload": payload})
        self.send_response(200)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass


@pytest.fixture()
def webhook_capture() -> Any:
    _CaptureReceiver.received = []
    server = HTTPServer(("127.0.0.1", 0), _CaptureReceiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)


def _hook_url(server: Any) -> str:
    return f"http://127.0.0.1:{server.server_address[1]}/webhooks/run-complete?token=test-secret"


async def _wait_webhooks(count: int, timeout: float = 10.0) -> list[dict[str, Any]]:
    for _ in range(int(timeout / 0.1)):
        if len(_CaptureReceiver.received) >= count:
            return _CaptureReceiver.received
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"expected {count} webhook deliveries, got {len(_CaptureReceiver.received)}"
    )


async def test_run_success_fires_webhook_with_values(sdk_client: Any, webhook_capture: Any) -> None:
    thread_id = _tid()
    run = await sdk_client.runs.create(
        thread_id,
        "echo",
        input={"messages": [{"role": "user", "content": "hook me"}]},
        if_not_exists="create",
        multitask_strategy="interrupt",
        stream_resumable=True,
        durability="sync",
        webhook=_hook_url(webhook_capture),
    )
    await sdk_client.runs.join(thread_id, run["run_id"])

    deliveries = await _wait_webhooks(1)
    payload = deliveries[0]["payload"]
    assert deliveries[0]["path"].endswith("?token=test-secret")
    assert payload["run_id"] == run["run_id"]
    assert payload["status"] == "success"
    assert payload["values"]["messages"][-1]["content"] == "echo: hook me"


async def test_if_not_exists_reject_404s(sdk_client: Any) -> None:
    with pytest.raises(Exception, match="not found|404"):
        await sdk_client.runs.create(
            _tid(),
            "echo",
            input={"messages": [{"role": "user", "content": "no thread"}]},
        )


async def test_interrupt_arbitration_cancels_inflight(
    sdk_client: Any, webhook_capture: Any
) -> None:
    """Double dispatch: first run interrupted at a step boundary, second
    succeeds, BOTH webhooks fire, and the first never emits its final echo."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    first = await sdk_client.runs.create(
        thread_id,
        "slow_busy",
        input={"messages": [{"role": "user", "content": "long task"}], "busy_seconds": 1.5},
        multitask_strategy="interrupt",
        webhook=_hook_url(webhook_capture),
    )
    for _ in range(40):
        current = await sdk_client.runs.get(thread_id, first["run_id"])
        if current["status"] == "running":
            break
        await asyncio.sleep(0.1)

    second = await sdk_client.runs.create(
        thread_id,
        "echo",
        input={"messages": [{"role": "user", "content": "supersede"}]},
        multitask_strategy="interrupt",
        webhook=_hook_url(webhook_capture),
    )
    result = await sdk_client.runs.join(thread_id, second["run_id"])
    assert result["messages"][-1]["content"] == "echo: supersede"

    first_final = await sdk_client.runs.get(thread_id, first["run_id"])
    second_final = await sdk_client.runs.get(thread_id, second["run_id"])
    assert first_final["status"] == "interrupted"
    assert second_final["status"] == "success"

    state = await sdk_client.threads.get_state(thread_id)
    contents = [m.get("content") for m in state["values"]["messages"]]
    assert "echo: long task" not in contents

    deliveries = await _wait_webhooks(2)
    statuses = {d["payload"]["run_id"]: d["payload"]["status"] for d in deliveries}
    assert statuses[first["run_id"]] == "interrupted"
    assert statuses[second["run_id"]] == "success"
    # EXACTLY once per run — a duplicate-delivery regression must not hide
    # behind the dict collapse (adversarial finding 1/15a).
    await asyncio.sleep(0.5)
    per_run: dict[str, int] = {}
    for d in _CaptureReceiver.received:
        per_run[d["payload"]["run_id"]] = per_run.get(d["payload"]["run_id"], 0) + 1
    assert per_run == {first["run_id"]: 1, second["run_id"]: 1}, per_run


async def test_interrupt_preserves_checkpoint(sdk_client: Any) -> None:
    """The durability=sync floor: steps checkpointed before the interrupt
    survive it (what a re-trigger resumes from — D3)."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    first = await sdk_client.runs.create(
        thread_id,
        "slow_busy",
        input={"messages": [{"role": "user", "content": "checkpointed"}], "busy_seconds": 0.8},
        multitask_strategy="interrupt",
        durability="sync",
    )
    # Let at least one step checkpoint land.
    await asyncio.sleep(1.2)
    await sdk_client.runs.cancel(thread_id, first["run_id"], action="interrupt")
    final = await sdk_client.runs.get(thread_id, first["run_id"])
    for _ in range(40):
        final = await sdk_client.runs.get(thread_id, first["run_id"])
        if final["status"] != "running":
            break
        await asyncio.sleep(0.1)
    assert final is not None and final["status"] == "interrupted"

    state = await sdk_client.threads.get_state(thread_id)
    assert int(state["values"].get("steps_done") or 0) >= 1


async def test_runs_list_status_filter_and_cancel_many(sdk_client: Any) -> None:
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    run = await sdk_client.runs.create(
        thread_id,
        "slow_busy",
        input={"messages": [{"role": "user", "content": "cancel me"}], "busy_seconds": 5},
    )
    active = [
        r["run_id"]
        for status in ("pending", "running")
        for r in await sdk_client.runs.list(thread_id, status=status)
    ]
    assert run["run_id"] in active
    errored = await sdk_client.runs.list(thread_id, status="error")
    assert run["run_id"] not in [r["run_id"] for r in errored]

    # reconcile.py's exact call: cancel_many(action="interrupt") — action is
    # a QUERY param; a body-reading server would use the default silently.
    await sdk_client.runs.cancel_many(
        thread_id=thread_id, run_ids=[run["run_id"]], action="interrupt"
    )
    final = await sdk_client.runs.get(thread_id, run["run_id"])
    for _ in range(40):
        final = await sdk_client.runs.get(thread_id, run["run_id"])
        if final["status"] not in ("pending", "running"):
            break
        await asyncio.sleep(0.1)
    assert final["status"] == "interrupted"


async def test_cancel_delivers_exactly_one_webhook_and_lifecycle(
    sdk_client: Any, webhook_capture: Any, runtime_env: str
) -> None:
    """Regression pin for the double-finalize blocker: one cancel → exactly
    one webhook delivery and exactly one `interrupted` lifecycle row."""
    import psycopg

    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    run = await sdk_client.runs.create(
        thread_id,
        "slow_busy",
        input={"messages": [{"role": "user", "content": "cancel once"}], "busy_seconds": 3},
        webhook=_hook_url(webhook_capture),
        stream_resumable=True,
    )
    for _ in range(40):
        current = await sdk_client.runs.get(thread_id, run["run_id"])
        if current["status"] == "running":
            break
        await asyncio.sleep(0.1)

    await sdk_client.runs.cancel(thread_id, run["run_id"], action="interrupt")
    deliveries = await _wait_webhooks(1)
    await asyncio.sleep(0.8)  # give any duplicate time to arrive
    assert len(_CaptureReceiver.received) == 1, _CaptureReceiver.received
    assert deliveries[0]["payload"]["status"] == "interrupted"

    with psycopg.connect(runtime_env) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM rt_thread_event "
            "WHERE thread_id = %s AND event = 'lifecycle' AND data LIKE '%%interrupted%%'",
            (thread_id,),
        ).fetchone()
    assert row is not None and row[0] == 1, f"expected 1 interrupted lifecycle row, got {row}"


async def test_v2_seq_survives_process_restart(runtime_env: str, runtime_server: Any) -> None:
    """Regression pin for the seq-restart blocker: a fresh executor over the
    same database continues the per-thread seq, never reissuing one."""
    import uuid as _uuid

    from agent_runtime import db as runtime_db
    from agent_runtime.config import runtime_config_path
    from agent_runtime.executor import RunExecutor
    from agent_runtime.registry import GraphRegistry
    from agent_runtime.runs_repo import RunsRepo
    from agent_runtime.streams import WireEvent
    from agent_runtime.threads_repo import ThreadsRepo

    thread_id = str(_uuid.uuid4())
    run_id = str(_uuid.uuid4())

    pool = runtime_db.create_pool(runtime_env)
    await pool.open(wait=True)
    try:
        threads = ThreadsRepo(pool)
        runs = RunsRepo(pool)
        await threads.create(thread_id, {})
        await runs.insert(
            run_id=run_id,
            thread_id=thread_id,
            assistant_id="echo",
            multitask_strategy="reject",
            kwargs={},
            metadata={},
        )

        def fresh_executor() -> RunExecutor:
            return RunExecutor(
                pool=pool,
                saver=None,
                registry=GraphRegistry(runtime_config_path()),
                threads=threads,
                runs=runs,
            )

        first = fresh_executor()
        await first._emit(thread_id, run_id, WireEvent("values", {"n": 1}))
        await first._emit(thread_id, run_id, WireEvent("values", {"n": 2}))

        # "Restart": a brand-new executor with cold in-memory counters.
        second = fresh_executor()
        await second._emit(thread_id, run_id, WireEvent("values", {"n": 3}))

        async with pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT seq FROM rt_thread_event WHERE thread_id = %s ORDER BY id",
                    (thread_id,),
                )
            ).fetchall()
        seqs = [r["seq"] for r in rows]
        assert seqs == [1, 2, 3], f"seq must continue across restarts, got {seqs}"
    finally:
        await pool.close()


async def test_rollback_is_rejected(sdk_client: Any) -> None:
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    run = await sdk_client.runs.create(
        thread_id,
        "slow_busy",
        input={"messages": [{"role": "user", "content": "x"}], "busy_seconds": 3},
    )
    with pytest.raises(Exception, match="rollback|400"):
        await sdk_client.runs.cancel(thread_id, run["run_id"], action="rollback")
    await sdk_client.runs.cancel(thread_id, run["run_id"], action="interrupt")


async def test_interrupting_graph_marks_run_and_thread_interrupted(sdk_client: Any) -> None:
    """A graph that calls interrupt() leaves run + thread 'interrupted', and
    a resume command completes it (the /commands path resumes via run.start
    with a command body — T9's test drives that wire)."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    run = await sdk_client.runs.create(
        thread_id,
        "interrupting",
        input={"messages": [{"role": "user", "content": "need approval"}]},
    )
    final = await sdk_client.runs.get(thread_id, run["run_id"])
    for _ in range(50):
        final = await sdk_client.runs.get(thread_id, run["run_id"])
        if final["status"] not in ("pending", "running"):
            break
        await asyncio.sleep(0.1)
    assert final["status"] == "interrupted"
    thread = await sdk_client.threads.get(thread_id)
    assert thread["status"] == "interrupted"

    state = await sdk_client.threads.get_state(thread_id)
    assert state["interrupts"], "pending interrupt missing from state"
    # Structured, not a repr string (adversarial finding 3): the dashboard
    # resume flows read interrupt["value"].
    interrupt = state["interrupts"][0]
    assert isinstance(interrupt, dict), interrupt
    assert interrupt["value"] == {"question": "approve?", "prompt": "need approval"}
    assert interrupt.get("id")

    resume = await sdk_client.runs.create(
        thread_id,
        "interrupting",
        command={"resume": True},
        multitask_strategy="interrupt",
    )
    result = await sdk_client.runs.join(thread_id, resume["run_id"])
    assert result["messages"][-1]["content"] == "resolution: approved"
