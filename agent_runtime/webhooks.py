"""Completion-webhook sender.

Payload matches langgraph-api's delivery shape (run dict + status/values/
timestamps; ``error`` on failure). The URL comes verbatim from
``rt_run.kwargs["webhook"]`` — open-swe's dispatch already appended the
``?token=<RUN_COMPLETE_WEBHOOK_SECRET>`` auth, and completion.py verifies it;
the runtime adds no signing of its own (phase-1.md T7).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_ATTEMPTS = 3
_TIMEOUT = httpx.Timeout(10.0, connect=5.0)


async def send_completion_webhook(
    run: dict[str, Any],
    *,
    status: str,
    values: Any = None,
    exception: BaseException | None = None,
) -> bool:
    url = (run.get("kwargs") or {}).get("webhook")
    if not url:
        return False
    now = datetime.now(UTC).isoformat()
    payload: dict[str, Any] = {
        **run,
        "status": status,
        "run_started_at": run.get("created_at"),
        "run_ended_at": now,
        "webhook_sent_at": now,
        "values": values,
    }
    if exception is not None:
        payload["error"] = f"{type(exception).__name__}: {exception}"

    delay = 1.0
    for attempt in range(1, _ATTEMPTS + 1):
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                response = await client.post(url, json=payload)
            if response.status_code < 500:
                return True
            logger.warning(
                "Completion webhook for run %s got %s (attempt %d)",
                run.get("run_id"),
                response.status_code,
                attempt,
            )
        except Exception:  # noqa: BLE001
            logger.warning(
                "Completion webhook for run %s failed (attempt %d)",
                run.get("run_id"),
                attempt,
                exc_info=True,
            )
        if attempt < _ATTEMPTS:
            await asyncio.sleep(delay)
            delay *= 2
    return False
