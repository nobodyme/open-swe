"""Threads router (phase-1.md T4). Every route cites an app caller."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from agent_runtime.models import ThreadCreateBody, ThreadSearchBody, ThreadUpdateBody
from agent_runtime.serializers import serialize_state_snapshot
from agent_runtime.threads_repo import ThreadExistsError, ThreadNotFoundError

router = APIRouter(tags=["threads"])


def _state(request: Request) -> Any:
    return request.app.state


def _validate_thread_id(thread_id: str) -> str:
    try:
        return str(uuid.UUID(thread_id))
    except ValueError as exc:
        raise HTTPException(422, "Invalid thread ID: must be a UUID") from exc


@router.post("/threads")
async def create_thread(body: ThreadCreateBody, request: Request) -> dict[str, Any]:
    state = _state(request)
    thread_id = _validate_thread_id(body.thread_id) if body.thread_id else str(uuid.uuid4())
    try:
        thread, _created = await state.threads.create(
            thread_id, body.metadata, if_exists=body.if_exists
        )
    except ThreadExistsError as exc:
        raise HTTPException(409, f"Thread with ID {thread_id} already exists") from exc
    return thread


@router.get("/threads/{thread_id}")
async def get_thread(thread_id: str, request: Request) -> dict[str, Any]:
    thread_id = _validate_thread_id(thread_id)
    thread = await _state(request).threads.get(thread_id)
    if thread is None:
        raise HTTPException(404, f"Thread with ID {thread_id} not found")
    return thread


@router.patch("/threads/{thread_id}")
async def update_thread(thread_id: str, body: ThreadUpdateBody, request: Request) -> dict[str, Any]:
    thread_id = _validate_thread_id(thread_id)
    try:
        return await _state(request).threads.update_metadata(thread_id, body.metadata)
    except ThreadNotFoundError as exc:
        raise HTTPException(404, f"Thread with ID {thread_id} not found") from exc


@router.delete("/threads/{thread_id}")
async def delete_thread(thread_id: str, request: Request) -> dict[str, Any]:
    thread_id = _validate_thread_id(thread_id)
    state = _state(request)
    # Stop in-flight work first: a still-executing task would keep writing
    # checkpoints/events for a row that no longer exists.
    for run in await state.runs.list(thread_id, status="all", limit=1000):
        if run["status"] in ("pending", "running"):
            await state.executor.cancel_run(thread_id, run["run_id"], action="interrupt")
    deleted = await state.threads.delete(thread_id)
    if not deleted:
        raise HTTPException(404, f"Thread with ID {thread_id} not found")
    # Checkpoints go with the thread; store data is namespace-keyed app data
    # and is deliberately NOT implied (phase-1.md T4).
    await state.saver.adelete_thread(thread_id)
    return {"ok": True}


@router.post("/threads/search")
async def search_threads(body: ThreadSearchBody, request: Request) -> list[dict[str, Any]]:
    return await _state(request).threads.search(
        metadata=body.metadata,
        status=body.status,
        limit=body.limit,
        offset=body.offset,
        sort_by=body.sort_by,
        sort_order=body.sort_order,
        select=body.select,
    )


async def _latest_snapshot(request: Request, thread_id: str) -> Any:
    state = _state(request)
    thread = await state.threads.get(thread_id)
    if thread is None:
        raise HTTPException(404, f"Thread with ID {thread_id} not found")
    return await state.executor.get_state_snapshot(thread_id)


@router.get("/threads/{thread_id}/state")
async def get_thread_state(thread_id: str, request: Request) -> dict[str, Any]:
    thread_id = _validate_thread_id(thread_id)
    snapshot = await _latest_snapshot(request, thread_id)
    return serialize_state_snapshot(snapshot, thread_id=thread_id)


@router.post("/threads/{thread_id}/history")
async def get_thread_history(
    thread_id: str, request: Request, body: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    thread_id = _validate_thread_id(thread_id)
    state = _state(request)
    thread = await state.threads.get(thread_id)
    if thread is None:
        raise HTTPException(404, f"Thread with ID {thread_id} not found")
    body = body or {}
    limit = int(body.get("limit") or 10)
    snapshots = await state.executor.get_state_history(thread_id, limit=limit)
    return [serialize_state_snapshot(s, thread_id=thread_id) for s in snapshots]
