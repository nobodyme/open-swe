"""Streaming + commands wire tests (phase-1.md T8/T9)."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import httpx

from tests.contract.normalize import parse_sse


def _tid() -> str:
    return str(uuid.uuid4())


async def test_runs_stream_emits_metadata_then_mode_events(
    sdk_client: Any, runtime_server: Any
) -> None:
    """SDK runs.stream: metadata first, then values events, run completes."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    parts = []
    async for part in sdk_client.runs.stream(
        thread_id,
        "echo",
        input={"messages": [{"role": "user", "content": "stream me"}]},
        stream_mode="values",
    ):
        parts.append(part)
    assert parts[0].event == "metadata"
    assert "run_id" in parts[0].data
    values = [p for p in parts if p.event == "values"]
    assert values, [p.event for p in parts]
    assert values[-1].data["messages"][-1]["content"] == "echo: stream me"


async def test_threads_join_stream_live_tail_and_last_event_id_resume(
    sdk_client: Any,
) -> None:
    """Dashboard live tail + reconnect: no loss, no duplication."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    run = await sdk_client.runs.create(
        thread_id,
        "slow_busy",
        input={"messages": [{"role": "user", "content": "tail me"}], "busy_seconds": 0.5},
        stream_resumable=True,
    )

    head: list[Any] = []

    async def read_head() -> None:
        stream = await sdk_client.threads.join_stream(thread_id)
        async for part in stream:
            if part.event and part.event.startswith("lifecycle"):
                continue
            head.append(part)
            if len(head) >= 1 and part.id:
                return

    await asyncio.wait_for(read_head(), timeout=30)
    last_seen = next(p.id for p in reversed(head) if p.id)

    tail: list[Any] = []

    async def read_tail() -> None:
        stream = await sdk_client.threads.join_stream(thread_id, last_event_id=last_seen)
        async for part in stream:
            tail.append(part)
            if part.event == "values" and isinstance(part.data, dict):
                messages = part.data.get("messages") or []
                if messages and messages[-1].get("content") == "echo: tail me":
                    return

    await asyncio.wait_for(read_tail(), timeout=30)
    await sdk_client.runs.join(thread_id, run["run_id"])

    head_ids = [p.id for p in head if p.id]
    tail_ids = [p.id for p in tail if p.id]
    assert last_seen not in tail_ids
    assert not set(head_ids) & set(tail_ids)


async def test_stream_events_v2_live_and_since_resume(sdk_client: Any, runtime_server: Any) -> None:
    """The v2 wire: envelope shape, lifecycle events, seq contiguity across
    a since-based reconnect (the resumable-SSE contract)."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)

    async with httpx.AsyncClient(base_url=runtime_server.base_url, timeout=60.0) as http:
        connected = asyncio.Event()
        collected = bytearray()

        async def consume(body: dict[str, Any], stop_at: str) -> list[dict[str, Any]]:
            local = bytearray()
            async with http.stream(
                "POST",
                f"/threads/{thread_id}/stream/events",
                json=body,
                headers={"Accept": "text/event-stream"},
            ) as response:
                assert response.status_code == 200
                connected.set()
                async for chunk in response.aiter_bytes():
                    local.extend(chunk)
                    events = parse_sse(bytes(local))
                    if any(
                        isinstance(e["data"], dict)
                        and e["data"].get("method") == "lifecycle"
                        and (e["data"].get("params") or {}).get("data", {}).get("event") == stop_at
                        for e in events
                    ):
                        break
            collected.extend(local)
            return parse_sse(bytes(local))

        async def start_run() -> str:
            await connected.wait()
            # No "tools"/"events" here: the SDK run endpoints 422 them (dev
            # parity) — only the /commands run.start path accepts the full
            # dashboard list.
            run = await sdk_client.runs.create(
                thread_id,
                "echo",
                input={"messages": [{"role": "user", "content": "v2 probe"}]},
                stream_mode=["values", "updates", "messages", "messages-tuple", "checkpoints"],
                stream_resumable=True,
            )
            return run["run_id"]

        starter = asyncio.create_task(start_run())
        events = await asyncio.wait_for(
            consume(
                {"channels": ["values", "updates", "messages", "tools", "lifecycle"]}, "completed"
            ),
            timeout=30,
        )
        run_id = await starter
        await sdk_client.runs.join(thread_id, run_id)

        # Envelope shape per the golden.
        for event in events:
            data = event["data"]
            assert data["type"] == "event"
            assert set(data) >= {"event_id", "seq", "method", "params"}
            assert str(data["seq"]) == event["id"]
        lifecycles = [
            (e["data"]["params"]["data"]["event"])
            for e in events
            if e["data"]["method"] == "lifecycle"
        ]
        assert lifecycles[0] == "running" and lifecycles[-1] == "completed"
        values = [e for e in events if e["data"]["method"] == "values"]
        assert values
        seqs = [int(e["id"]) for e in events]
        assert seqs == sorted(seqs)

        # since-resume: replay strictly after the cut, contiguous.
        cut = seqs[0]
        connected = asyncio.Event()
        resumed_raw = bytearray()
        async with http.stream(
            "POST",
            f"/threads/{thread_id}/stream/events",
            json={
                "channels": ["values", "updates", "messages", "tools", "lifecycle"],
                "since": cut,
            },
            headers={"Accept": "text/event-stream"},
        ) as response:
            assert response.status_code == 200
            async for chunk in response.aiter_bytes():
                resumed_raw.extend(chunk)
                resumed = parse_sse(bytes(resumed_raw))
                if len(resumed) >= len(seqs) - 1:
                    break
        resumed_seqs = [int(e["id"]) for e in parse_sse(bytes(resumed_raw))]
        assert resumed_seqs == seqs[1:]


async def test_stream_events_v2_failed_run_lifecycle_and_error(
    sdk_client: Any, runtime_server: Any
) -> None:
    """Failure path: a live v2 subscriber AND a post-mortem replay both end
    with lifecycle "failed" — the only error name the SDK's lifecycleReason()
    resolves — and the run record carries the exception text."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)

    async with httpx.AsyncClient(base_url=runtime_server.base_url, timeout=60.0) as http:

        async def consume(connected: asyncio.Event) -> list[dict[str, Any]]:
            local = bytearray()
            async with http.stream(
                "POST",
                f"/threads/{thread_id}/stream/events",
                json={"channels": ["values", "lifecycle"]},
                headers={"Accept": "text/event-stream"},
            ) as response:
                assert response.status_code == 200
                connected.set()
                async for chunk in response.aiter_bytes():
                    local.extend(chunk)
                    if any(
                        isinstance(e["data"], dict)
                        and e["data"].get("method") == "lifecycle"
                        and (e["data"].get("params") or {}).get("data", {}).get("event") == "failed"
                        for e in parse_sse(bytes(local))
                    ):
                        break
            return parse_sse(bytes(local))

        connected = asyncio.Event()

        async def start_run() -> str:
            await connected.wait()
            run = await sdk_client.runs.create(
                thread_id,
                "failing",
                input={"messages": [{"role": "user", "content": "boom please"}]},
                stream_mode=["values"],
                stream_resumable=True,
            )
            return run["run_id"]

        starter = asyncio.create_task(start_run())
        live = await asyncio.wait_for(consume(connected), timeout=30)
        run_id = await starter

        lifecycles = [
            e["data"]["params"]["data"]["event"] for e in live if e["data"]["method"] == "lifecycle"
        ]
        assert lifecycles[0] == "running" and lifecycles[-1] == "failed", lifecycles

        run: dict[str, Any] | None = None
        for _ in range(50):
            run = await sdk_client.runs.get(thread_id, run_id)
            if run is not None and run["status"] not in ("pending", "running"):
                break
            await asyncio.sleep(0.1)
        assert run is not None and run["status"] == "error"
        assert run["error"] == "RuntimeError: deterministic boom"

        # A fresh subscriber's replay (page refresh / rejoin) also sees the end.
        replayed = await asyncio.wait_for(consume(asyncio.Event()), timeout=30)
        failed = [
            e
            for e in replayed
            if e["data"]["method"] == "lifecycle"
            and e["data"]["params"]["data"]["event"] == "failed"
        ]
        assert failed, [e["data"] for e in replayed]
        assert failed[-1]["data"]["event_id"] == f"synth:{run_id}:lc||failed"


async def test_join_run_stream_after_completion_ends_immediately(sdk_client: Any) -> None:
    """Dev parity: finished run replays nothing on runs.join_stream."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)
    run = await sdk_client.runs.create(
        thread_id,
        "echo",
        input={"messages": [{"role": "user", "content": "done already"}]},
        stream_resumable=True,
    )
    await sdk_client.runs.join(thread_id, run["run_id"])

    events = []
    async with asyncio.timeout(10):
        async for part in sdk_client.runs.join_stream(
            thread_id, run["run_id"], stream_mode="values"
        ):
            events.append(part.event)
    assert events == []


async def test_commands_run_start_and_interrupt_resume(
    sdk_client: Any, runtime_server: Any
) -> None:
    """T9: run.start envelope per golden; a second run.start with a resume
    command completes an interrupting graph."""
    thread_id = _tid()
    await sdk_client.threads.create(thread_id=thread_id)

    run: dict[str, Any] | None = None
    async with httpx.AsyncClient(base_url=runtime_server.base_url, timeout=30.0) as http:
        start = await http.post(
            f"/threads/{thread_id}/commands",
            json={
                "id": 1,
                "method": "run.start",
                "params": {
                    "assistant_id": "interrupting",
                    "input": {"messages": [{"role": "user", "content": "approve this"}]},
                    "stream_mode": ["values"],
                    "stream_resumable": True,
                },
            },
        )
        assert start.status_code == 200
        payload = start.json()
        assert payload["type"] == "success"
        assert payload["id"] == 1
        assert set(payload["meta"]) == {"applied_through_seq"}
        run_id = payload["result"]["run_id"]

        for _ in range(50):
            run = await sdk_client.runs.get(thread_id, run_id)
            if run is not None and run["status"] not in ("pending", "running"):
                break
            await asyncio.sleep(0.1)
        assert run is not None and run["status"] == "interrupted"

        resume = await http.post(
            f"/threads/{thread_id}/commands",
            json={
                "id": 2,
                "method": "run.start",
                "params": {
                    "assistant_id": "interrupting",
                    "command": {"resume": True},
                },
            },
        )
        assert resume.status_code == 200
        resume_run_id = resume.json()["result"]["run_id"]
        # Dev parity: join on a /commands-started run returns a v2 protocol
        # envelope, not values (divergence ledger item 4) — read the state.
        joined = await sdk_client.runs.join(thread_id, resume_run_id)
        assert set(joined) >= {"type", "method", "params"}, joined
        state = await sdk_client.threads.get_state(thread_id)
        assert state["values"]["messages"][-1]["content"] == "resolution: approved"

        malformed = await http.post(f"/threads/{thread_id}/commands", json={"id": "x", "method": 5})
        assert malformed.status_code == 400
        assert malformed.json()["error"] == "invalid_argument"
