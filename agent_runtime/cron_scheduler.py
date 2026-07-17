"""Cron firing loop (phase-1.md T10, D4 name).

``rt_cron`` is the source of truth; the APScheduler ``AsyncIOScheduler`` is a
projection of it (rebuilt on boot). ``end_time`` MUST stop re-fires — the
wakeup tool's one-shot semantics depend on it (schedule_thread_wakeup.py:94) —
so the trigger is built via ``from_crontab`` and then given ``end_date``
explicitly (3.x's ``from_crontab`` accepts no ``end_date`` kwarg).
"""

from __future__ import annotations

import contextlib
import logging
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from agent_runtime.db import query

if TYPE_CHECKING:
    from agent_runtime.db import DictPool
    from agent_runtime.executor import RunExecutor
    from agent_runtime.threads_repo import ThreadsRepo

logger = logging.getLogger(__name__)

_MISFIRE_GRACE_SECONDS = 60


def _build_trigger(schedule: str, timezone: str, end_time: datetime | None) -> CronTrigger:
    trigger = CronTrigger.from_crontab(schedule, timezone=ZoneInfo(timezone or "UTC"))
    if end_time is not None:
        trigger.end_date = (
            end_time.astimezone(trigger.timezone)
            if end_time.tzinfo
            else end_time.replace(tzinfo=trigger.timezone)
        )
    return trigger


class CronScheduler:
    def __init__(
        self,
        *,
        pool: DictPool,
        executor: RunExecutor,
        threads: ThreadsRepo,
    ) -> None:
        self._pool = pool
        self._executor = executor
        self._threads = threads
        self._scheduler = AsyncIOScheduler()

    async def start(self) -> None:
        self._scheduler.start()
        import json as _json  # noqa: F401

        async with self._pool.connection() as conn:
            rows = await (await conn.execute("SELECT * FROM rt_cron")).fetchall()
        for row in rows:
            try:
                self._add_job(self._row_to_cron(row))
            except Exception:  # noqa: BLE001
                logger.exception("Failed to schedule cron %s on boot", row.get("cron_id"))

    async def shutdown(self) -> None:
        self._scheduler.shutdown(wait=False)

    @staticmethod
    def _row_to_cron(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "cron_id": str(row["cron_id"]),
            "assistant_id": row["assistant_id"],
            "thread_id": str(row["thread_id"]) if row.get("thread_id") else None,
            "schedule": row["schedule"],
            "timezone": row["timezone"],
            "end_time": row["end_time"].isoformat() if row.get("end_time") else None,
            "payload": row["payload"],
            "metadata": row["metadata"],
            "next_run_date": (
                row["next_run_date"].isoformat() if row.get("next_run_date") else None
            ),
            "created_at": row["created_at"].isoformat(),
            "updated_at": row["updated_at"].isoformat(),
        }

    async def create(
        self,
        *,
        assistant_id: str,
        schedule: str,
        thread_id: str | None,
        end_time: datetime | None,
        timezone: str | None,
        payload: dict[str, Any],
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        import json

        tz = timezone or "UTC"
        trigger = _build_trigger(schedule, tz, end_time)  # validates early
        cron_id = str(uuid.uuid4())
        next_fire = trigger.get_next_fire_time(None, datetime.now(trigger.timezone))
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    INSERT INTO rt_cron
                      (cron_id, assistant_id, thread_id, schedule, timezone, end_time,
                       payload, metadata, next_run_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                    RETURNING *
                    """,
                    (
                        cron_id,
                        assistant_id,
                        thread_id,
                        schedule,
                        tz,
                        end_time,
                        json.dumps(payload),
                        json.dumps(metadata),
                        next_fire,
                    ),
                )
            ).fetchone()
        assert row is not None
        cron = self._row_to_cron(row)
        self._add_job(cron)
        return cron

    def _add_job(self, cron: dict[str, Any]) -> None:
        end_time = datetime.fromisoformat(cron["end_time"]) if cron.get("end_time") else None
        trigger = _build_trigger(cron["schedule"], cron["timezone"], end_time)
        # Boot misfire semantics (T10): a tick missed by less than the grace
        # window (e.g. the process was restarting) fires immediately; older
        # ones are skipped (best-effort posture).
        kwargs: dict[str, Any] = {}
        stored_next = (
            datetime.fromisoformat(cron["next_run_date"]) if cron.get("next_run_date") else None
        )
        if stored_next is not None:
            now = datetime.now(stored_next.tzinfo or trigger.timezone)
            missed_by = (now - stored_next).total_seconds()
            if 0 < missed_by <= _MISFIRE_GRACE_SECONDS:
                kwargs["next_run_time"] = now
        self._scheduler.add_job(
            self._fire,
            trigger=trigger,
            id=cron["cron_id"],
            args=[cron],
            misfire_grace_time=_MISFIRE_GRACE_SECONDS,
            replace_existing=True,
            **kwargs,
        )

    async def _persist_next_run(self, cron_id: str) -> None:
        """Keep rt_cron.next_run_date in sync with the live job so
        crons.search never serves a stale schedule."""
        job = self._scheduler.get_job(cron_id)
        next_run = getattr(job, "next_run_time", None) if job is not None else None
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE rt_cron SET next_run_date = %s, updated_at = now() WHERE cron_id = %s",
                (next_run, cron_id),
            )

    async def _fire(self, cron: dict[str, Any]) -> None:
        from agent_runtime.models import RunCreateBody

        payload = cron.get("payload") or {}
        thread_id = cron.get("thread_id") or str(uuid.uuid4())  # fresh thread per fire
        body = RunCreateBody(
            assistant_id=cron["assistant_id"],
            input=payload.get("input"),
            config=payload.get("config"),
            metadata=cron.get("metadata") or {},
            multitask_strategy=str(payload.get("multitask_strategy") or "interrupt"),  # type: ignore[arg-type]
            if_not_exists="create",
            webhook=payload.get("webhook"),
        )
        try:
            await self._executor.create_run(
                thread_id, assistant_id=cron["assistant_id"], body=body.model_dump()
            )
            logger.info("Cron %s fired on thread %s", cron["cron_id"], thread_id)
        except Exception:  # noqa: BLE001
            logger.exception("Cron %s failed to fire", cron["cron_id"])
        finally:
            with contextlib.suppress(Exception):
                await self._persist_next_run(cron["cron_id"])

    async def search(
        self,
        *,
        assistant_id: str | None = None,
        thread_id: str | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if assistant_id:
            clauses.append("assistant_id = %s")
            params.append(assistant_id)
        if thread_id:
            clauses.append("thread_id = %s")
            params.append(thread_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    query(f"SELECT * FROM rt_cron {where} ORDER BY created_at LIMIT %s OFFSET %s"),  # noqa: S608
                    params,
                )
            ).fetchall()
        return [self._row_to_cron(row) for row in rows]

    async def delete(self, cron_id: str) -> bool:
        async with self._pool.connection() as conn:
            result = await conn.execute("DELETE FROM rt_cron WHERE cron_id = %s", (cron_id,))
        try:
            self._scheduler.remove_job(cron_id)
        except Exception:  # noqa: BLE001
            pass
        return result.rowcount > 0
