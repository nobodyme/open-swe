"""Threads API tests (phase-1.md T4) — driven through the real SDK client."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest


def _tid() -> str:
    return str(uuid.uuid4())


async def test_create_if_exists_do_nothing_idempotent(sdk_client: Any) -> None:
    thread_id = _tid()
    first = await sdk_client.threads.create(thread_id=thread_id, metadata={"origin": "first"})
    assert first["thread_id"] == thread_id
    second = await sdk_client.threads.create(
        thread_id=thread_id, metadata={"origin": "second"}, if_exists="do_nothing"
    )
    assert second["thread_id"] == thread_id
    fetched = await sdk_client.threads.get(thread_id)
    assert fetched["metadata"]["origin"] == "first"

    with pytest.raises(Exception, match="already exists|409"):
        await sdk_client.threads.create(thread_id=thread_id)


async def test_update_merges_metadata_and_delete_404s(sdk_client: Any) -> None:
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id, metadata={"a": 1})
    await sdk_client.threads.update(thread_id=thread_id, metadata={"b": 2})
    fetched = await sdk_client.threads.get(thread_id)
    assert fetched["metadata"] == {"a": 1, "b": 2}

    await sdk_client.threads.delete(thread_id)
    with pytest.raises(Exception, match="not found|404"):
        await sdk_client.threads.get(thread_id)


async def test_search_sort_select_and_nested_filters(sdk_client: Any) -> None:
    batch = f"batch-{_tid()[:8]}"
    ids = [_tid() for _ in range(3)]
    for index, thread_id in enumerate(ids):
        await sdk_client.threads.create(
            thread_id=thread_id,
            metadata={
                "batch": batch,
                "index": index,
                "source_context": {"kind": "slack", "channel": f"C{index}"},
            },
        )
        await asyncio.sleep(0.02)
    await sdk_client.threads.update(thread_id=ids[0], metadata={"touched": True})

    ascending = await sdk_client.threads.search(
        metadata={"batch": batch}, sort_by="created_at", sort_order="asc", limit=10
    )
    assert [t["thread_id"] for t in ascending] == ids

    by_updated = await sdk_client.threads.search(
        metadata={"batch": batch}, sort_by="updated_at", sort_order="desc", limit=10
    )
    assert by_updated[0]["thread_id"] == ids[0]

    nested = await sdk_client.threads.search(
        metadata={"batch": batch, "source_context": {"kind": "slack", "channel": "C1"}},
        limit=10,
    )
    assert [t["metadata"]["index"] for t in nested] == [1]

    subset = await sdk_client.threads.search(
        metadata={"batch": batch}, select=["thread_id", "status", "metadata"], limit=1
    )
    assert set(subset[0].keys()) == {"thread_id", "status", "metadata"}

    page_two = await sdk_client.threads.search(
        metadata={"batch": batch}, sort_by="created_at", sort_order="asc", limit=2, offset=2
    )
    assert [t["thread_id"] for t in page_two] == [ids[2]]


async def test_run_lifecycle_state_and_status(sdk_client: Any) -> None:
    """echo run: created → success; thread busy while running, idle after;
    get_state returns the final values through the real checkpointer."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id, metadata={"probe": thread_id})

    run = await sdk_client.runs.create(
        thread_id,
        "echo",
        input={"messages": [{"role": "user", "content": "hello runtime"}]},
        if_not_exists="create",
    )
    result = await sdk_client.runs.join(thread_id, run["run_id"])
    assert result["messages"][-1]["content"] == "echo: hello runtime"

    final = await sdk_client.runs.get(thread_id, run["run_id"])
    assert final["status"] == "success"
    thread = await sdk_client.threads.get(thread_id)
    assert thread["status"] == "idle"
    assert thread["values"]["messages"][-1]["content"] == "echo: hello runtime"

    state = await sdk_client.threads.get_state(thread_id)
    assert state["values"]["messages"][-1]["content"] == "echo: hello runtime"
    assert state["checkpoint"]["thread_id"] == thread_id
    assert state["next"] == []


async def test_search_status_busy_during_run(sdk_client: Any) -> None:
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id, metadata={"busy_probe": thread_id})
    run = await sdk_client.runs.create(
        thread_id,
        "slow_busy",
        input={"messages": [{"role": "user", "content": "hold"}], "busy_seconds": 0.4},
    )
    busy: list[dict[str, Any]] = []
    for _ in range(20):
        busy = await sdk_client.threads.search(
            metadata={"busy_probe": thread_id}, status="busy", limit=5
        )
        if busy:
            break
        await asyncio.sleep(0.1)
    assert [t["thread_id"] for t in busy] == [thread_id]

    await sdk_client.runs.join(thread_id, run["run_id"])
    after = await sdk_client.threads.search(
        metadata={"busy_probe": thread_id}, status="busy", limit=5
    )
    assert after == []


async def test_history_returns_checkpoint_snapshots(sdk_client: Any) -> None:
    import httpx

    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    run = await sdk_client.runs.create(
        thread_id,
        "echo",
        input={"messages": [{"role": "user", "content": "history probe"}]},
    )
    await sdk_client.runs.join(thread_id, run["run_id"])

    base_url = sdk_client.http.client.base_url
    async with httpx.AsyncClient(base_url=str(base_url), timeout=30.0) as http:
        response = await http.post(f"/threads/{thread_id}/history", json={"limit": 10})
    assert response.status_code == 200
    snapshots = response.json()
    assert len(snapshots) >= 2  # input checkpoint + final
    assert snapshots[0]["values"]["messages"][-1]["content"] == "echo: history probe"

    raw_state = await http_get_state(str(base_url), thread_id)
    assert raw_state["values"]["messages"][-1]["content"] == "echo: history probe"


async def http_get_state(base_url: str, thread_id: str) -> dict[str, Any]:
    import httpx

    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as http:
        response = await http.get(f"/threads/{thread_id}/state")
    assert response.status_code == 200
    return response.json()
