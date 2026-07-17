"""Postgres wiring: one shared AsyncConnectionPool for the whole runtime.

The saver, the store, and the rt_* repositories all draw from this pool, so
the in-process ``get_store()`` a graph node sees and the Store router's
``AsyncPostgresStore`` are one instance over one pool (MIGRATION §1's
store-consistency constraint, D1).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

if TYPE_CHECKING:
    from psycopg.abc import QueryNoTemplate

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# The whole runtime works in dict rows.
DictPool = AsyncConnectionPool[AsyncConnection[DictRow]]


def query(text: str) -> QueryNoTemplate:
    """Dynamic SQL built from allowlisted identifiers (psycopg types queries
    as LiteralString; every dynamic piece here comes from a literal allowlist,
    never from request input)."""
    return cast("QueryNoTemplate", text)


def create_pool(database_url: str) -> DictPool:
    # autocommit: the checkpoint/store libraries manage their own transactions
    # and their .setup() runs CREATE INDEX CONCURRENTLY-style statements that
    # cannot run inside one.
    return AsyncConnectionPool(
        database_url,
        min_size=1,
        max_size=10,
        open=False,
        connection_class=AsyncConnection[DictRow],
        kwargs={"autocommit": True, "row_factory": dict_row},
    )


async def apply_schema(pool: DictPool) -> None:
    ddl = _SCHEMA_PATH.read_text(encoding="utf-8")
    async with pool.connection() as conn:
        await conn.execute(query(ddl))
