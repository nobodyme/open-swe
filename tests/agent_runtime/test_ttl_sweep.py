"""Checkpoint TTL sweep tests (phase-3.md T2) — four pins, no more."""

from __future__ import annotations

import uuid
from typing import Any

import psycopg
import pytest

from agent_runtime.db import query


@pytest.fixture()
async def sweep_env(runtime_env: str, sdk_client: Any) -> dict[str, Any]:
    """A thread with real checkpoints + events, made to look 30 days idle."""
    import asyncio

    thread_id = str(uuid.uuid4())
    await sdk_client.threads.create(thread_id=thread_id, metadata={"sandbox_id": "sb-keep"})
    run = await sdk_client.runs.create(
        thread_id,
        "echo",
        input={"messages": [{"role": "user", "content": "expire me"}]},
        stream_resumable=True,
    )
    await sdk_client.runs.join(thread_id, run["run_id"])
    # _finalize's recompute_status is the LAST updated_at writer and lands
    # after join returns; wait it out so _age_thread can't be clobbered.
    for _ in range(50):
        thread = await sdk_client.threads.get(thread_id)
        if thread["status"] != "busy":
            break
        await asyncio.sleep(0.1)
    await asyncio.sleep(0.2)
    return {"thread_id": thread_id, "dsn": runtime_env}


def _age_thread(dsn: str, thread_id: str) -> None:
    with psycopg.connect(dsn) as conn:
        conn.execute(
            query(
                "UPDATE rt_thread SET updated_at = now() - interval '31 days' WHERE thread_id = %s"
            ),
            (thread_id,),
        )
        conn.commit()


async def _run_sweep(dsn: str) -> int:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    from agent_runtime import db as runtime_db
    from agent_runtime.ttl_sweep import sweep_expired_checkpoints

    pool = runtime_db.create_pool(dsn)
    await pool.open(wait=True)
    try:
        saver = AsyncPostgresSaver(pool)  # type: ignore[arg-type]
        return await sweep_expired_checkpoints(pool, saver)
    finally:
        await pool.close()


def _counts(dsn: str, thread_id: str) -> dict[str, Any]:
    with psycopg.connect(dsn) as conn:
        checkpoints = conn.execute(
            query("SELECT COUNT(*) FROM checkpoints WHERE thread_id = %s"), (thread_id,)
        ).fetchone()
        events = conn.execute(
            query("SELECT COUNT(*) FROM rt_thread_event WHERE thread_id = %s"), (thread_id,)
        ).fetchone()
        thread = conn.execute(
            query("SELECT metadata FROM rt_thread WHERE thread_id = %s"), (thread_id,)
        ).fetchone()
        runs = conn.execute(
            query("SELECT COUNT(*) FROM rt_run WHERE thread_id = %s"), (thread_id,)
        ).fetchone()
    assert checkpoints is not None and events is not None and runs is not None
    return {
        "checkpoints": checkpoints[0],
        "events": events[0],
        "runs": runs[0],
        "metadata": thread[0] if thread else None,
    }


async def test_expired_idle_thread_is_swept_but_thread_row_kept(
    sweep_env: dict[str, Any], sdk_client: Any
) -> None:
    dsn, thread_id = sweep_env["dsn"], sweep_env["thread_id"]
    before = _counts(dsn, thread_id)
    assert before["checkpoints"] > 0 and before["events"] > 0

    _age_thread(dsn, thread_id)
    assert await _run_sweep(dsn) == 1

    after = _counts(dsn, thread_id)
    assert after["checkpoints"] == 0
    assert after["events"] == 0
    # The divergence pin: thread row (metadata!) and run history stay.
    assert after["metadata"] == {"sandbox_id": "sb-keep"}
    assert after["runs"] == before["runs"]

    # get_state on a swept thread reads like a never-run thread.
    state = await sdk_client.threads.get_state(thread_id)
    assert not (state.get("values") or {}).get("messages")


async def test_thread_with_inflight_run_is_untouched(
    sweep_env: dict[str, Any], sdk_client: Any
) -> None:
    dsn, thread_id = sweep_env["dsn"], sweep_env["thread_id"]
    run = await sdk_client.runs.create(
        thread_id,
        "slow_busy",
        input={"messages": [{"role": "user", "content": "hold"}], "busy_seconds": 3},
    )
    _age_thread(dsn, thread_id)
    assert await _run_sweep(dsn) == 0
    assert _counts(dsn, thread_id)["checkpoints"] > 0
    # Pin the exclusion, not incidental freshness: the run must still be
    # in flight when the sweep declined the thread.
    current = await sdk_client.runs.get(thread_id, run["run_id"])
    assert current["status"] in ("pending", "running"), current["status"]
    await sdk_client.runs.cancel(thread_id, run["run_id"], action="interrupt")


async def test_fresh_thread_is_untouched(sweep_env: dict[str, Any]) -> None:
    dsn, thread_id = sweep_env["dsn"], sweep_env["thread_id"]
    assert await _run_sweep(dsn) == 0
    assert _counts(dsn, thread_id)["checkpoints"] > 0


async def test_sweep_is_idempotent(sweep_env: dict[str, Any]) -> None:
    dsn, thread_id = sweep_env["dsn"], sweep_env["thread_id"]
    _age_thread(dsn, thread_id)
    assert await _run_sweep(dsn) == 1
    assert await _run_sweep(dsn) == 0
