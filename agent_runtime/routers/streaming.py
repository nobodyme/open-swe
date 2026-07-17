"""Streaming routes (phase-1.md T8): SDK run/thread streams + the v2 wire.

SSE encodings:

* SDK streams (``POST /threads/{id}/runs/stream``, ``GET .../runs/{id}/stream``,
  ``GET /threads/{id}/stream``): ``event:`` = wire event name, ``id:`` = the
  durable redis-style event id (``Last-Event-ID`` resumes against it).
* v2 (``POST /threads/{id}/stream/events``): ``event:`` = channel, ``data:`` =
  the envelope, ``id:`` = session seq; body ``since`` resumes.

All streams subscribe FIRST, then replay ``rt_thread_event``, then tail the
live queue deduping on seq — replay-then-tail with no gap and no duplicate
(MIGRATION §3; the resume goldens pin it).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

from agent_runtime.models import RunCreateBody
from agent_runtime.streams import WireEvent, to_v2_envelope

router = APIRouter(tags=["streaming"])

_HEARTBEAT_SECONDS = 15.0


def _sse(event: str, data: Any, event_id: str | None = None) -> bytes:
    lines = [f"event: {event}", f"data: {json.dumps(data, default=str)}"]
    if event_id is not None:
        lines.append(f"id: {event_id}")
    return ("\n".join(lines) + "\n\n").encode()


def _encode(
    event: WireEvent,
    *,
    as_v2: bool,
    channels: set[str] | None,
    include_lifecycle: bool,
) -> bytes | None:
    if as_v2:
        envelope = to_v2_envelope(event)
        if envelope is None:
            return None
        if channels is not None and envelope["method"] not in channels:
            return None
        return _sse(envelope["method"], envelope, event_id=str(event.seq))
    if event.event == "lifecycle" and not include_lifecycle:
        return None
    return _sse(event.event, event.data, event_id=event.event_id)


async def _stream_thread_events(
    request: Request,
    thread_id: str,
    *,
    after_seq: int | None = None,
    after_event_id: str | None = None,
    only_run: str | None = None,
    stop_at_run_end: bool = False,
    include_lifecycle: bool = False,
    as_v2: bool = False,
    channels: set[str] | None = None,
) -> AsyncIterator[bytes]:
    """Subscribe → replay → tail, deduping on the global ``ord`` (no gap, no
    duplicate). v2 mode replays only seq-carrying rows (``since`` semantics)."""
    state = request.app.state
    executor = state.executor
    queue = executor.subscribe(thread_id)
    try:
        replayed = await executor.replay_events(
            thread_id,
            after_seq=after_seq,
            after_event_id=after_event_id,
            v2_only=as_v2,
        )
        last_ord = 0
        for event in replayed:
            last_ord = max(last_ord, event.ord)
            if only_run is not None and event.run_id != only_run:
                continue
            chunk = _encode(
                event, as_v2=as_v2, channels=channels, include_lifecycle=include_lifecycle
            )
            if chunk:
                yield chunk

        def _deliver(event: WireEvent) -> bytes | None:
            nonlocal last_ord
            if event.ord <= last_ord:
                return None  # already replayed
            last_ord = event.ord
            if only_run is not None and event.run_id != only_run:
                return None
            if as_v2 and after_seq is not None and 0 < event.seq <= after_seq:
                return None
            return _encode(
                event, as_v2=as_v2, channels=channels, include_lifecycle=include_lifecycle
            )

        while True:
            if stop_at_run_end and only_run is not None:
                run = await state.runs.get(thread_id, only_run)
                if run is None or run["status"] not in ("pending", "running"):
                    # Drain anything already queued for this run, then end.
                    while not queue.empty():
                        event = queue.get_nowait()
                        if event is None:
                            return
                        chunk = _deliver(event)
                        if chunk:
                            yield chunk
                    return
            try:
                event = await asyncio.wait_for(
                    queue.get(), timeout=0.25 if stop_at_run_end else _HEARTBEAT_SECONDS
                )
            except TimeoutError:
                if not stop_at_run_end:
                    yield b": heartbeat\n\n"
                continue
            if event is None:
                return
            chunk = _deliver(event)
            if chunk:
                yield chunk
    finally:
        executor.unsubscribe(thread_id, queue)


@router.post("/threads/{thread_id}/runs/stream")
async def create_run_and_stream(
    thread_id: str, body: RunCreateBody, request: Request
) -> StreamingResponse:
    """SDK runs.stream: create the run, emit metadata, stream its events."""
    from agent_runtime.routers.runs import _create_run

    # Create BEFORE the response starts streaming so validation errors (e.g.
    # stream_mode="tools" → 422) surface as real HTTP errors, not a dead SSE.
    run = await _create_run(request, thread_id, body)

    async def stream() -> AsyncIterator[bytes]:
        yield _sse("metadata", {"run_id": run["run_id"], "attempt": 1})
        # Replay covers any event emitted between run start and subscription;
        # seq-dedup inside prevents double-delivery of queued overlap.
        async for chunk in _stream_thread_events(
            request,
            thread_id,
            only_run=run["run_id"],
            stop_at_run_end=True,
        ):
            yield chunk

    return StreamingResponse(stream(), media_type="text/event-stream")


@router.get("/threads/{thread_id}/runs/{run_id}/stream")
async def join_run_stream(
    thread_id: str,
    run_id: str,
    request: Request,
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
) -> StreamingResponse:
    """SDK runs.join_stream. Finished run → the stream ends immediately
    (dev behavior; join_stream_after_completion.json golden)."""
    state = request.app.state
    run = await state.runs.get(thread_id, run_id)
    if run is None:
        raise HTTPException(404, f"Run {run_id} not found")

    async def stream() -> AsyncIterator[bytes]:
        current = await state.runs.get(thread_id, run_id)
        if current is None or current["status"] not in ("pending", "running"):
            return
        async for chunk in _stream_thread_events(
            request,
            thread_id,
            after_event_id=last_event_id,
            only_run=run_id,
            stop_at_run_end=True,
        ):
            yield chunk

    return StreamingResponse(stream(), media_type="text/event-stream")


@router.get("/threads/{thread_id}/stream")
async def join_thread_stream(
    thread_id: str,
    request: Request,
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
) -> StreamingResponse:
    """SDK threads.join_stream — the dashboard live tail (thread_api.py:2016)."""
    state = request.app.state
    thread = await state.threads.get(thread_id)
    if thread is None:
        raise HTTPException(404, f"Thread with ID {thread_id} not found")

    async def stream() -> AsyncIterator[bytes]:
        # Live tail: without a Last-Event-ID cursor, start from now (dev
        # behavior — no history replay for finished runs).
        after_seq = None
        if not last_event_id:
            after_seq = await state.executor.load_thread_seq(thread_id)
        async for chunk in _stream_thread_events(
            request,
            thread_id,
            after_seq=after_seq,
            after_event_id=last_event_id,
        ):
            yield chunk

    return StreamingResponse(stream(), media_type="text/event-stream")


@router.post("/threads/{thread_id}/stream/events")
async def thread_events_v2(thread_id: str, request: Request) -> StreamingResponse:
    """The v2 dashboard wire (thread_api.py:1835 proxies it verbatim)."""
    try:
        body = json.loads(await request.body())
    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON body") from None
    channels = body.get("channels") if isinstance(body, dict) else None
    if not isinstance(channels, list) or not channels:
        raise HTTPException(400, "channels is required and must be a non-empty array")
    raw_since = body.get("since")
    since = (
        raw_since
        if isinstance(raw_since, int) and not isinstance(raw_since, bool) and raw_since >= 0
        else None
    )

    async def stream() -> AsyncIterator[bytes]:
        async for chunk in _stream_thread_events(
            request,
            thread_id,
            after_seq=since if since is not None else 0,
            include_lifecycle=True,
            as_v2=True,
            channels=set(channels),
        ):
            yield chunk

    return StreamingResponse(stream(), media_type="text/event-stream")
