"""Crons router (phase-1.md T10). Wire paths from langgraph_sdk/_async/cron.py."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from agent_runtime.models import CronCreateBody, CronSearchBody

router = APIRouter(tags=["crons"])


def _payload(body: CronCreateBody) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if body.input is not None:
        payload["input"] = body.input
    if body.config is not None:
        payload["config"] = body.config
    if body.multitask_strategy is not None:
        payload["multitask_strategy"] = body.multitask_strategy
    if body.webhook is not None:
        payload["webhook"] = body.webhook
    return payload


@router.post("/runs/crons")
async def create_cron(body: CronCreateBody, request: Request) -> dict[str, Any]:
    """Schedule cron: fresh thread per fire (analyzer_cron.py:42, schedules.py:236)."""
    return await request.app.state.crons.create(
        assistant_id=body.assistant_id,
        schedule=body.schedule,
        thread_id=None,
        end_time=body.end_time,
        timezone=str(body.timezone) if body.timezone else None,
        payload=_payload(body),
        metadata=body.metadata,
    )


@router.post("/threads/{thread_id}/runs/crons")
async def create_cron_for_thread(
    thread_id: str, body: CronCreateBody, request: Request
) -> dict[str, Any]:
    """Thread cron with end_time one-shot semantics (schedule_thread_wakeup.py:129)."""
    state = request.app.state
    if await state.threads.get(thread_id) is None:
        raise HTTPException(404, f"Thread with ID {thread_id} not found")
    return await state.crons.create(
        assistant_id=body.assistant_id,
        schedule=body.schedule,
        thread_id=thread_id,
        end_time=body.end_time,
        timezone=str(body.timezone) if body.timezone else None,
        payload=_payload(body),
        metadata=body.metadata,
    )


@router.post("/runs/crons/search")
async def search_crons(body: CronSearchBody, request: Request) -> list[dict[str, Any]]:
    return await request.app.state.crons.search(
        assistant_id=body.assistant_id,
        thread_id=body.thread_id,
        limit=body.limit,
        offset=body.offset,
    )


@router.delete("/runs/crons/{cron_id}")
async def delete_cron(cron_id: str, request: Request) -> dict[str, Any]:
    deleted = await request.app.state.crons.delete(cron_id)
    if not deleted:
        raise HTTPException(404, f"Cron {cron_id} not found")
    return {"ok": True}
