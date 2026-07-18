"""Crons + APScheduler firing loop (phase-1.md T10)."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg


def _tid() -> str:
    return str(uuid.uuid4())


def _next_minute_cron(offset_minutes: int = 1) -> str:
    """A 5-field crontab hitting the next minute boundary (UTC)."""
    fire_at = datetime.now(UTC) + timedelta(minutes=offset_minutes)
    return f"{fire_at.minute} {fire_at.hour} * * *"


async def test_create_search_delete_wire_shapes(sdk_client: Any) -> None:
    cron = await sdk_client.crons.create(
        "echo",
        schedule="0 4 * * *",
        input={"messages": [{"role": "user", "content": "nightly"}]},
    )
    assert set(cron) >= {"cron_id", "assistant_id", "schedule", "thread_id", "end_time"}
    assert cron["thread_id"] is None
    assert cron["next_run_date"] is not None

    searched = await sdk_client.crons.search()
    assert any(c["cron_id"] == cron["cron_id"] for c in searched)

    await sdk_client.crons.delete(cron["cron_id"])
    remaining = await sdk_client.crons.search()
    assert all(c["cron_id"] != cron["cron_id"] for c in remaining)


async def test_create_for_thread_with_end_time_and_timezone(sdk_client: Any) -> None:
    """The exact schedule_thread_wakeup.py call shape."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    cron = await sdk_client.crons.create_for_thread(
        thread_id,
        "echo",
        schedule="30 7 * * *",
        input={"messages": [{"role": "user", "content": "wakeup"}]},
        end_time=datetime.now(UTC) + timedelta(days=1),
        timezone="America/Los_Angeles",
    )
    assert cron["thread_id"] == thread_id
    assert cron["end_time"] is not None
    assert cron["timezone"] == "America/Los_Angeles"

    by_thread = await sdk_client.crons.search(thread_id=thread_id)
    assert [c["cron_id"] for c in by_thread] == [cron["cron_id"]]
    await sdk_client.crons.delete(cron["cron_id"])


async def test_end_time_in_past_never_fires(sdk_client: Any, runtime_env: str) -> None:
    """end_time MUST stop re-fires — the wakeup tool's one-shot contract.
    A cron whose end_time already passed gets no next fire and never runs."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    cron = await sdk_client.crons.create_for_thread(
        thread_id,
        "echo",
        schedule="* * * * *",  # would fire every minute...
        input={"messages": [{"role": "user", "content": "never"}]},
        end_time=datetime.now(UTC) - timedelta(minutes=1),  # ...but ended already
    )
    assert cron["next_run_date"] is None

    # Wait past a minute boundary: no run may appear on the thread.
    seconds_to_boundary = 61 - datetime.now(UTC).second
    await asyncio.sleep(min(seconds_to_boundary, 65))
    runs = await sdk_client.runs.list(thread_id, limit=10)
    assert runs == []
    await sdk_client.crons.delete(cron["cron_id"])


async def test_every_minute_cron_fires_on_thread(sdk_client: Any) -> None:
    """A live fire: '* * * * *' creates a run on the bound thread within
    the next minute boundary (arbitration applies — interrupt strategy)."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    cron = await sdk_client.crons.create_for_thread(
        thread_id,
        "echo",
        schedule="* * * * *",
        input={"messages": [{"role": "user", "content": "tick"}]},
        end_time=datetime.now(UTC) + timedelta(minutes=2),
    )
    try:
        runs: list[dict[str, Any]] = []
        for _ in range(140):  # up to ~70s: the next minute boundary
            runs = await sdk_client.runs.list(thread_id, limit=10)
            if runs:
                break
            await asyncio.sleep(0.5)
        assert runs, "cron never fired within the minute boundary"
        await sdk_client.runs.join(thread_id, runs[0]["run_id"])
        thread = await sdk_client.threads.get(thread_id)
        assert thread["values"]["messages"][-1]["content"] == "echo: tick"
    finally:
        await sdk_client.crons.delete(cron["cron_id"])


async def test_boot_reloads_rt_cron_rows(runtime_env: str) -> None:
    """rt_cron is the source of truth: a fresh scheduler projects rows into
    jobs on start (as a rebooted runtime would)."""
    import json

    from agent_runtime import db as runtime_db
    from agent_runtime.config import runtime_config_path
    from agent_runtime.cron_scheduler import CronScheduler
    from agent_runtime.executor import RunExecutor
    from agent_runtime.registry import GraphRegistry
    from agent_runtime.runs_repo import RunsRepo
    from agent_runtime.threads_repo import ThreadsRepo

    cron_id = str(uuid.uuid4())
    with psycopg.connect(runtime_env) as conn:
        conn.execute(
            "INSERT INTO rt_cron (cron_id, assistant_id, schedule, timezone, payload) "
            "VALUES (%s, 'echo', '0 4 * * *', 'UTC', %s::jsonb)",
            (cron_id, json.dumps({"input": {"messages": []}})),
        )
        conn.commit()

    pool = runtime_db.create_pool(runtime_env)
    await pool.open(wait=True)
    try:
        threads = ThreadsRepo(pool)
        executor = RunExecutor(
            pool=pool,
            saver=None,
            registry=GraphRegistry(runtime_config_path()),
            threads=threads,
            runs=RunsRepo(pool),
        )
        scheduler = CronScheduler(pool=pool, executor=executor, threads=threads)
        await scheduler.start()
        try:
            job = scheduler._scheduler.get_job(cron_id)
            assert job is not None, "boot did not project the rt_cron row into a job"
        finally:
            await scheduler.shutdown()
    finally:
        await pool.close()
