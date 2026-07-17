"""v2 commands endpoint (phase-1.md T9).

``POST /threads/{id}/commands`` — stateless JSON command transport. Callers:
the dashboard proxies at thread_api.py:1901 and review_chat_api.py:471. The
response envelope is pinned by commands_run_start_response.json:
``{"type": "success", "id": <id>, "result": {"run_id": ...},
"meta": {"applied_through_seq": <seq>}}``; malformed commands get the
``invalid_argument`` error envelope (langgraph_api/api/event_streaming.py).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from agent_runtime.executor import (
    ThreadBusyError,
    ThreadMissingError,
    UnknownAssistantError,
    UnsupportedStrategyError,
)
from agent_runtime.models import RunCreateBody

router = APIRouter(tags=["commands"])


def _error(command_id: Any, error: str, message: str, *, status_code: int = 400) -> JSONResponse:
    return JSONResponse(
        {"type": "error", "id": command_id, "error": error, "message": message},
        status_code=status_code,
    )


@router.post("/threads/{thread_id}/commands")
async def thread_command(thread_id: str, request: Request) -> JSONResponse:
    import json

    try:
        payload = json.loads(await request.body())
    except json.JSONDecodeError:
        return JSONResponse({"detail": "Invalid JSON body"}, status_code=400)
    if (
        not isinstance(payload, dict)
        or not isinstance(payload.get("id"), int)
        or not isinstance(payload.get("method"), str)
    ):
        return _error(
            payload.get("id") if isinstance(payload, dict) else None,
            "invalid_argument",
            "Protocol commands must include an integer id and string method.",
        )

    command_id = payload["id"]
    method = payload["method"]
    raw_params = payload.get("params")
    params: dict[str, Any] = raw_params if isinstance(raw_params, dict) else {}

    if method != "run.start":
        return _error(command_id, "invalid_argument", f"Unsupported command method: {method}")

    state = request.app.state
    body = RunCreateBody(
        assistant_id=str(params.get("assistant_id") or "agent"),
        input=params.get("input"),
        command=params.get("command"),
        config=params.get("config"),
        metadata=params.get("metadata") or {},
        multitask_strategy=params.get("multitask_strategy") or "interrupt",
        if_not_exists="create",
        stream_mode=params.get("stream_mode"),
        stream_resumable=bool(params.get("stream_resumable")),
        durability=params.get("durability"),
        webhook=params.get("webhook"),
    )
    # Snapshot the seq BEFORE the run starts emitting (golden pins 0 on a
    # fresh thread).
    applied_through_seq = await state.executor.load_thread_seq(thread_id)
    body_dump = body.model_dump()
    # Internal marker (stripped from wire kwargs): dev's runs.join returns a
    # v2 protocol envelope — not final values — for /commands-started runs
    # (divergence datum pinned by commands_run_start_response flow).
    body_dump["__transport__"] = "commands"
    try:
        run = await state.executor.create_run(
            thread_id, assistant_id=body.assistant_id, body=body_dump
        )
    except ThreadMissingError:
        return _error(command_id, "not_found", "thread not found", status_code=404)
    except UnknownAssistantError:
        return _error(command_id, "not_found", "assistant not found", status_code=404)
    except ThreadBusyError:
        return _error(command_id, "failed_precondition", "thread is busy", status_code=409)
    except UnsupportedStrategyError as exc:
        return _error(command_id, "invalid_argument", f"unsupported strategy: {exc}")
    return JSONResponse(
        {
            "type": "success",
            "id": command_id,
            "result": {"run_id": run["run_id"]},
            "meta": {"applied_through_seq": applied_through_seq},
        }
    )
