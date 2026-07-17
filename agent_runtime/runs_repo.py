"""All rt_run SQL. Response shape matches the run_create.json golden."""

from __future__ import annotations

import json
from typing import Any

from agent_runtime.db import DictPool, query


def _serialize(row: dict[str, Any]) -> dict[str, Any]:
    kwargs = row["kwargs"]
    if isinstance(kwargs, dict):
        # Dunder keys are runtime-internal (e.g. __transport__), never wire.
        kwargs = {k: v for k, v in kwargs.items() if not k.startswith("__")}
    return {
        "run_id": str(row["run_id"]),
        "thread_id": str(row["thread_id"]),
        "assistant_id": row["assistant_id"],
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
        "metadata": row["metadata"],
        "status": row["status"],
        "kwargs": kwargs,
        "multitask_strategy": row["multitask_strategy"],
    }


class RunsRepo:
    def __init__(self, pool: DictPool) -> None:
        self._pool = pool

    async def insert(
        self,
        *,
        run_id: str,
        thread_id: str,
        assistant_id: str,
        multitask_strategy: str,
        kwargs: dict[str, Any],
        metadata: dict[str, Any],
        status: str = "pending",
    ) -> dict[str, Any]:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    INSERT INTO rt_run
                      (run_id, thread_id, assistant_id, status, multitask_strategy,
                       kwargs, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                    RETURNING *
                    """,
                    (
                        run_id,
                        thread_id,
                        assistant_id,
                        status,
                        multitask_strategy,
                        json.dumps(kwargs),
                        json.dumps(metadata),
                    ),
                )
            ).fetchone()
        assert row is not None
        return _serialize(row)

    async def get(self, thread_id: str, run_id: str) -> dict[str, Any] | None:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM rt_run WHERE thread_id = %s AND run_id = %s",
                    (thread_id, run_id),
                )
            ).fetchone()
        return _serialize(row) if row else None

    async def get_raw_kwargs(self, thread_id: str, run_id: str) -> dict[str, Any] | None:
        """Unstripped kwargs (internal dunder keys included)."""
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT kwargs FROM rt_run WHERE thread_id = %s AND run_id = %s",
                    (thread_id, run_id),
                )
            ).fetchone()
        return row["kwargs"] if row else None

    async def list(
        self,
        thread_id: str,
        *,
        status: str | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        clauses = ["thread_id = %s"]
        params: list[Any] = [thread_id]
        if status and status != "all":
            clauses.append("status = %s")
            params.append(status)
        params.extend([limit, offset])
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    query(  # noqa: S608
                        f"SELECT * FROM rt_run WHERE {' AND '.join(clauses)} "
                        "ORDER BY created_at DESC, run_id DESC LIMIT %s OFFSET %s"
                    ),
                    params,
                )
            ).fetchall()
        return [_serialize(row) for row in rows]

    async def active_on_thread(self, thread_id: str) -> list[dict[str, Any]]:
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT * FROM rt_run WHERE thread_id = %s "
                    "AND status IN ('pending','running') ORDER BY created_at",
                    (thread_id,),
                )
            ).fetchall()
        return [_serialize(row) for row in rows]

    async def set_status(self, run_id: str, status: str) -> dict[str, Any] | None:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "UPDATE rt_run SET status = %s, updated_at = now() "
                    "WHERE run_id = %s RETURNING *",
                    (status, run_id),
                )
            ).fetchone()
        return _serialize(row) if row else None

    async def start_if_pending(self, run_id: str) -> dict[str, Any] | None:
        """pending → running, or None if something terminal-ized it first
        (a cancel landing in the insert-before-lock window must never be
        resurrected)."""
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "UPDATE rt_run SET status = 'running', updated_at = now() "
                    "WHERE run_id = %s AND status = 'pending' RETURNING *",
                    (run_id,),
                )
            ).fetchone()
        return _serialize(row) if row else None

    async def finish_if_active(self, run_id: str, status: str) -> dict[str, Any] | None:
        """Terminal transition, exactly once: only from pending/running.
        Returns None when the run was already finalized — the caller must
        then skip webhook/lifecycle emission (double-delivery guard)."""
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "UPDATE rt_run SET status = %s, updated_at = now() "
                    "WHERE run_id = %s AND status IN ('pending','running') RETURNING *",
                    (status, run_id),
                )
            ).fetchone()
        return _serialize(row) if row else None

    async def sweep_orphans(self) -> list[dict[str, Any]]:
        """Mark runs left pending/running by a dead process as error (D2)."""
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "UPDATE rt_run SET status = 'error', updated_at = now() "
                    "WHERE status IN ('pending','running') RETURNING *"
                )
            ).fetchall()
        return [_serialize(row) for row in rows]
