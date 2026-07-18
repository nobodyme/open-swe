"""SIGKILL chaos suite (phase-2.md T3): the D1 durability floor.

Pinned floor — exactly what durability="sync" + Phase 1's queue decision
promise, nothing more: a consistent checkpoint PREFIX (never corruption),
no rt_run left running after the boot sweep (orphans → error, D2, with
exactly one valid failure webhook), the thread freed by the reconcile
sweep, and NO silent re-execution.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from typing import Any

import httpx
import psycopg

from tests.chaos.receiver import WebhookCapture


async def _start_slow_run(
    base_url: str, *, webhook: str | None = None, thread_id: str | None = None
) -> tuple[str, str]:
    thread_id = thread_id or str(uuid.uuid4())
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as http:
        body: dict[str, Any] = {
            "assistant_id": "slow",
            "input": {"messages": [{"role": "user", "content": "chaos"}], "steps": []},
            "if_not_exists": "create",
            "multitask_strategy": "interrupt",
            "durability": "sync",
            "stream_resumable": True,
        }
        if webhook:
            body["webhook"] = webhook
        response = await http.post(f"/threads/{thread_id}/runs", json=body)
        response.raise_for_status()
        return thread_id, response.json()["run_id"]


async def _state_steps(base_url: str, thread_id: str) -> list[int]:
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as http:
        response = await http.get(f"/threads/{thread_id}/state")
        response.raise_for_status()
        values = response.json().get("values") or {}
        return list(values.get("steps") or [])


def _assert_floor(runtime: Any, thread_id: str, pre_kill_steps: list[int]) -> list[int]:
    """The invariant every kill must satisfy after restart."""
    with psycopg.connect(runtime.dsn) as conn:
        rows = conn.execute(
            "SELECT run_id, status FROM rt_run WHERE thread_id = %s", (thread_id,)
        ).fetchall()
    assert rows, "run row lost"
    for _run_id, status in rows:
        assert status not in ("pending", "running"), f"boot sweep left an active run: {rows}"

    steps = asyncio.run(_state_steps(runtime.base_url, thread_id))
    # Consistent prefix: 0..n-1 with no gaps, no duplicates, no corruption.
    assert steps == list(range(len(steps))), f"corrupt step sequence: {steps}"
    # No silent re-execution: the persisted prefix may exceed the pre-kill
    # observation by at most the read-to-kill gap (one step boundary plus one
    # in-flight write = 2).
    assert len(steps) <= len(pre_kill_steps) + 2, (
        f"steps grew after the kill without a new run: {len(pre_kill_steps)} -> {len(steps)}"
    )
    return steps


def test_sigkill_mid_run_no_corruption(chaos_runtime: Any) -> None:
    with WebhookCapture() as capture:
        thread_id, run_id = asyncio.run(
            _start_slow_run(chaos_runtime.base_url, webhook=capture.url)
        )
        # ~40% through the 30-step run.
        import time

        time.sleep(6.0)
        pre_kill = asyncio.run(_state_steps(chaos_runtime.base_url, thread_id))
        assert pre_kill, "no steps checkpointed before the kill — graph never started?"
        chaos_runtime.sigkill()

        chaos_runtime.start()
        steps = _assert_floor(chaos_runtime, thread_id, pre_kill)
        assert len(steps) < 30, "run completed despite the kill?"

        # D2: the orphan was swept to error and exactly one valid failure
        # webhook fired (agent/completion.py posts the user-facing reply on
        # exactly this signal).
        with psycopg.connect(chaos_runtime.dsn) as conn:
            row = conn.execute("SELECT status FROM rt_run WHERE run_id = %s", (run_id,)).fetchone()
        assert row is not None and row[0] == "error"
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not capture.received:
            time.sleep(0.2)
        errors = [d for d in capture.received if d["payload"].get("status") == "error"]
        assert len(errors) == 1, f"expected exactly 1 failure webhook, got {capture.received}"
        assert errors[0]["token_valid"]


def test_reconcile_frees_stuck_busy_thread(chaos_runtime: Any, monkeypatch: Any) -> None:
    thread_id, _run_id = asyncio.run(_start_slow_run(chaos_runtime.base_url))
    import time

    time.sleep(3.0)
    chaos_runtime.sigkill()
    chaos_runtime.start()

    # Simulate the stuck-busy shape reconcile exists for: a pending run the
    # sweep-to-error didn't cover (e.g. written by a dying process after the
    # sweep read); seed one directly.
    stale_run = str(uuid.uuid4())
    with psycopg.connect(chaos_runtime.dsn) as conn:
        conn.execute(
            "INSERT INTO rt_run (run_id, thread_id, assistant_id, status, created_at) "
            "VALUES (%s, %s, 'slow', 'pending', now() - interval '1 hour')",
            (stale_run, thread_id),
        )
        conn.execute("UPDATE rt_thread SET status = 'busy' WHERE thread_id = %s", (thread_id,))
        conn.commit()

    # Drive the REAL sweep the scheduler graph runs (agent/reconcile.py).
    monkeypatch.setenv("LANGGRAPH_URL", chaos_runtime.base_url)
    from agent import reconcile

    counts = asyncio.run(reconcile.reconcile_stale_runs(max_age_seconds=0))
    assert counts["cancelled"] >= 1, counts

    async def thread_status() -> str:
        async with httpx.AsyncClient(base_url=chaos_runtime.base_url, timeout=30.0) as http:
            return (await http.get(f"/threads/{thread_id}")).json()["status"]

    import time as _time

    for _ in range(20):
        if asyncio.run(thread_status()) != "busy":
            break
        _time.sleep(0.25)
    assert asyncio.run(thread_status()) != "busy"


def test_kill_during_checkpoint_write(chaos_runtime: Any) -> None:
    """Three kills at randomized offsets near step boundaries; the floor must
    hold every time."""
    import time

    rng = random.Random(42)
    for iteration in range(3):
        thread_id, _run_id = asyncio.run(_start_slow_run(chaos_runtime.base_url))
        time.sleep(2.0 + rng.uniform(0.3, 0.8))
        pre_kill = asyncio.run(_state_steps(chaos_runtime.base_url, thread_id))
        assert pre_kill, "no steps checkpointed before the kill — graph never started?"
        chaos_runtime.sigkill()
        chaos_runtime.start()
        steps = _assert_floor(chaos_runtime, thread_id, pre_kill)
        # Loud context on any failure.
        print(f"iteration {iteration}: pre-kill {len(pre_kill)} steps, post {len(steps)}")
