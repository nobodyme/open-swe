"""All rt_thread SQL + the single thread-status recompute function.

Status transitions happen ONLY in ``recompute_status`` — reconcile.py's
``status="busy"`` search and the dashboard busy pill both key off it, and the
Phase 0 contract goldens pin the transitions (phase-1.md T4).
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from agent_runtime.db import DictPool, query

_ALLOWED_SORT = {"thread_id", "status", "created_at", "updated_at"}
_ALLOWED_SELECT = {
    "thread_id",
    "status",
    "metadata",
    "values",
    "created_at",
    "updated_at",
    "config",
    "state_updated_at",
}
# Full wire shape pinned by thread_create.json: config is always {} (the app
# never writes thread config) and state_updated_at tracks the values column.
_DEFAULT_FIELDS = [
    "thread_id",
    "created_at",
    "updated_at",
    "metadata",
    "status",
    "values",
    "config",
    "state_updated_at",
]


def _serialize(row: dict[str, Any], select: list[str] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in select or _DEFAULT_FIELDS:
        if field == "config":
            out[field] = {}
            continue
        if field == "state_updated_at":
            value = row.get("updated_at")
            out[field] = value.isoformat() if value is not None else None
            continue
        value = row.get(field)
        if field == "thread_id":
            value = str(value)
        elif field in ("created_at", "updated_at") and value is not None:
            value = value.isoformat()
        out[field] = value
    return out


class ThreadsRepo:
    def __init__(self, pool: DictPool) -> None:
        self._pool = pool

    async def create(
        self,
        thread_id: str,
        metadata: dict[str, Any],
        *,
        if_exists: str = "raise",
    ) -> tuple[dict[str, Any], bool]:
        """Returns (thread, created). ``do_nothing`` keeps the FIRST create's
        metadata untouched (pinned by thread_create_if_exists_do_nothing golden)."""
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    INSERT INTO rt_thread (thread_id, metadata)
                    VALUES (%s, %s::jsonb)
                    ON CONFLICT (thread_id) DO NOTHING
                    RETURNING *
                    """,
                    (thread_id, json.dumps(metadata)),
                )
            ).fetchone()
            if row is not None:
                return _serialize(row), True
            if if_exists != "do_nothing":
                raise ThreadExistsError(thread_id)
            existing = await self.get(thread_id)
            assert existing is not None
            return existing, False

    async def get(self, thread_id: str) -> dict[str, Any] | None:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute("SELECT * FROM rt_thread WHERE thread_id = %s", (thread_id,))
            ).fetchone()
        return _serialize(row) if row else None

    async def update_metadata(self, thread_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Shallow JSONB merge — the semantics threads.update callers rely on."""
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    UPDATE rt_thread
                    SET metadata = metadata || %s::jsonb, updated_at = now()
                    WHERE thread_id = %s
                    RETURNING *
                    """,
                    (json.dumps(metadata), thread_id),
                )
            ).fetchone()
        if row is None:
            raise ThreadNotFoundError(thread_id)
        return _serialize(row)

    async def set_values(self, thread_id: str, values: Any) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                'UPDATE rt_thread SET "values" = %s::jsonb, updated_at = now() '
                "WHERE thread_id = %s",
                (json.dumps(values), thread_id),
            )

    async def delete(self, thread_id: str) -> bool:
        async with self._pool.connection() as conn:
            result = await conn.execute("DELETE FROM rt_thread WHERE thread_id = %s", (thread_id,))
        return result.rowcount > 0

    async def search(
        self,
        *,
        metadata: dict[str, Any] | None = None,
        status: str | None = None,
        limit: int = 10,
        offset: int = 0,
        sort_by: str | None = None,
        sort_order: str | None = None,
        select: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if metadata:
            # JSONB containment covers nested-dict filters.
            clauses.append("metadata @> %s::jsonb")
            params.append(json.dumps(metadata))
        if status:
            clauses.append("status = %s")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        order_col = sort_by if sort_by in _ALLOWED_SORT else "created_at"
        order_dir = "DESC" if (sort_order or "asc").lower() == "desc" else "ASC"

        selected: list[str] | None = None
        if select:
            selected = [f for f in select if f in _ALLOWED_SELECT]

        params.extend([limit, offset])
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    query(  # noqa: S608 - identifiers allowlisted above
                        f"SELECT * FROM rt_thread {where} "
                        f"ORDER BY {order_col} {order_dir} LIMIT %s OFFSET %s"
                    ),
                    params,
                )
            ).fetchall()
        return [_serialize(row, selected) for row in rows]

    async def recompute_status(self, thread_id: str) -> str:
        """busy if any pending/running run; else latest run's terminal shade."""
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    """
                    SELECT status FROM rt_run WHERE thread_id = %s
                    ORDER BY created_at DESC, run_id DESC
                    """,
                    (thread_id,),
                )
            ).fetchall()
            statuses = [r["status"] for r in rows]
            if any(s in ("pending", "running") for s in statuses):
                new_status = "busy"
            elif statuses and statuses[0] == "interrupted":
                new_status = "interrupted"
            elif statuses and statuses[0] in ("error", "timeout"):
                new_status = "error"
            else:
                new_status = "idle"
            await conn.execute(
                "UPDATE rt_thread SET status = %s, updated_at = now() WHERE thread_id = %s",
                (new_status, thread_id),
            )
        return new_status


class ThreadExistsError(Exception):
    pass


class ThreadNotFoundError(Exception):
    pass


def ensure_uuid(value: str) -> str:
    try:
        return str(UUID(value))
    except ValueError as exc:
        raise InvalidThreadIdError(value) from exc


class InvalidThreadIdError(Exception):
    pass
