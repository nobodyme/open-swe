"""Runs router (phase-1.md T7). Wire paths from langgraph_sdk/_async/runs.py.

``cancel_many``'s ``action`` is a QUERY parameter — the SDK sends only
thread_id/run_ids/status in the body (folded finding). ``join`` exists because
the Phase 0 contract suite (the T11 parity gate) drives runs through
``runs.join``; it returns the final thread values exactly as dev does.
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from agent_runtime.executor import (
    ThreadBusyError,
    ThreadMissingError,
    UnknownAssistantError,
    UnsupportedStrategyError,
)
from agent_runtime.models import CancelManyBody, RunCreateBody

router = APIRouter(tags=["runs"])


async def _create_run(request: Request, thread_id: str, body: RunCreateBody) -> dict[str, Any]:
    # Dev's run schema rejects "tools" as a run-level StreamMode (it is only
    # a v2 event-stream channel) — run_stream_mode_tools.json pins the 422.
    # The /commands path deliberately bypasses this (the dashboard's run.start
    # list includes "tools" and dev accepts it there).
    modes = body.stream_mode if isinstance(body.stream_mode, list) else [body.stream_mode]
    if "tools" in [m for m in modes if m]:
        raise HTTPException(422, '"tools" is not a valid run stream_mode')
    try:
        return await request.app.state.executor.create_run(
            thread_id,
            assistant_id=body.assistant_id,
            body=body.model_dump(),
        )
    except ThreadMissingError as exc:
        raise HTTPException(404, f"Thread with ID {thread_id} not found") from exc
    except UnknownAssistantError as exc:
        raise HTTPException(404, f"Assistant {body.assistant_id} not found") from exc
    except ThreadBusyError as exc:
        raise HTTPException(409, "Thread is already running a task") from exc
    except UnsupportedStrategyError as exc:
        # App callers only send interrupt/reject (D6).
        raise HTTPException(400, f"multitask_strategy {exc} is not supported") from exc


@router.post("/threads/{thread_id}/runs")
async def create_run(thread_id: str, body: RunCreateBody, request: Request) -> dict[str, Any]:
    return await _create_run(request, thread_id, body)


@router.get("/threads/{thread_id}/runs")
async def list_runs(
    thread_id: str,
    request: Request,
    status: str | None = None,
    limit: int = 10,
    offset: int = 0,
) -> list[dict[str, Any]]:
    return await request.app.state.runs.list(thread_id, status=status, limit=limit, offset=offset)


@router.get("/threads/{thread_id}/runs/{run_id}")
async def get_run(thread_id: str, run_id: str, request: Request) -> dict[str, Any]:
    run = await request.app.state.runs.get(thread_id, run_id)
    if run is None:
        raise HTTPException(404, f"Run {run_id} not found")
    return run


@router.get("/threads/{thread_id}/runs/{run_id}/join")
async def join_run(thread_id: str, run_id: str, request: Request) -> JSONResponse:
    state = request.app.state
    run = await state.runs.get(thread_id, run_id)
    if run is None:
        raise HTTPException(404, f"Run {run_id} not found")
    while run["status"] in ("pending", "running"):
        await state.executor.wait(run_id)
        await asyncio.sleep(0.05)
        run = await state.runs.get(thread_id, run_id)
        if run is None:
            raise HTTPException(404, f"Run {run_id} not found")

    # Dev quirk, deliberately preserved (divergence ledger item 4): joining a
    # /commands-started run returns the last v2 protocol envelope, not values.
    raw_kwargs = await state.runs.get_raw_kwargs(thread_id, run_id) or {}
    if raw_kwargs.get("__transport__") == "commands":
        from agent_runtime.streams import to_v2_envelope

        events = await state.executor.replay_events(thread_id, v2_only=True)
        run_values = [e for e in events if e.run_id == run_id and e.event == "values"]
        if run_values:
            return JSONResponse(to_v2_envelope(run_values[-1]))

    thread = await state.threads.get(thread_id)
    return JSONResponse((thread or {}).get("values"))


async def _cancel(
    request: Request, thread_id: str, run_id: str, *, wait: str, action: str
) -> Response:
    if action == "rollback":
        # App callers only send interrupt (or the default) — D6.
        raise HTTPException(400, "action=rollback is not supported by this runtime")
    state = request.app.state
    cancelled = await state.executor.cancel_run(thread_id, run_id, action=action)
    if not cancelled:
        raise HTTPException(404, f"Run {run_id} not found")
    if wait in ("1", "true", "True"):
        await state.executor.wait(run_id)
    return Response(status_code=202)


@router.get("/threads/{thread_id}/runs/{run_id}/cancel")
async def cancel_run_get(
    thread_id: str,
    run_id: str,
    request: Request,
    wait: str = "0",
    action: str = "interrupt",
) -> Response:
    return await _cancel(request, thread_id, run_id, wait=wait, action=action)


@router.post("/threads/{thread_id}/runs/{run_id}/cancel")
async def cancel_run_post(
    thread_id: str,
    run_id: str,
    request: Request,
    wait: str = "0",
    action: str = "interrupt",
) -> Response:
    return await _cancel(request, thread_id, run_id, wait=wait, action=action)


@router.post("/runs/cancel")
async def cancel_many(
    body: CancelManyBody,
    request: Request,
    action: str = "interrupt",
) -> JSONResponse:
    if action == "rollback":
        raise HTTPException(400, "action=rollback is not supported by this runtime")
    state = request.app.state
    thread_id = body.thread_id
    run_ids = body.run_ids or []
    if thread_id and not run_ids:
        status = body.status or "all"
        runs = await state.runs.list(
            thread_id, status=None if status == "all" else status, limit=1000
        )
        run_ids = [r["run_id"] for r in runs if r["status"] in ("pending", "running")]
    for run_id in run_ids:
        if thread_id:
            await state.executor.cancel_run(thread_id, run_id, action=action)
    # Dev returns null here (runs_cancel_many_response.json golden).
    return JSONResponse(None)
