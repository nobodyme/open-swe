"""Checkpoint TTL sweep (phase-3.md T2).

Enforces what langgraph.json's ``checkpointer.ttl`` block always INTENDED
(default_ttl=43200 minutes = 30 days; the inmem dev runtime's sweep is an
explicit no-op, so this is new enforcement of the config's platform intent).

Deliberate divergence from platform ``strategy="delete"`` (recorded in the
contract divergence ledger): the THREAD is kept. ``rt_thread.metadata`` is
load-bearing app state (sandbox id, encrypted GitHub token, Slack/PR links)
and ``rt_run`` rows feed usage/history — only checkpoint data and the
run-event log are deleted. ``get_state`` on a swept thread reads like a
never-run thread.

Race note (accepted): a run created between victim SELECT and delete loses
30-day-old checkpoints it was never going to read — it starts from empty
state exactly as it would post-sweep; not worth locking.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

from agent_runtime.db import query

if TYPE_CHECKING:
    from agent_runtime.db import DictPool

logger = logging.getLogger(__name__)


def ttl_minutes() -> int:
    """0 disables the sweep entirely."""
    try:
        return max(0, int(os.environ.get("CHECKPOINT_TTL_MINUTES", "43200")))
    except ValueError:
        return 43200


def sweep_interval_minutes() -> int:
    try:
        return max(1, int(os.environ.get("CHECKPOINT_SWEEP_INTERVAL_MINUTES", "60")))
    except ValueError:
        return 60


def sweep_limit() -> int:
    try:
        return max(1, int(os.environ.get("CHECKPOINT_SWEEP_LIMIT", "500")))
    except ValueError:
        return 500


async def sweep_expired_checkpoints(pool: DictPool, saver: Any) -> int:
    """Delete checkpoint data + event log for threads idle past the TTL.

    Victim selection runs against OUR schema only; deletion of checkpoint
    rows goes through the checkpoint package's supported cross-table delete
    (``adelete_thread``), never hand-rolled SQL against its tables.
    """
    ttl = ttl_minutes()
    if ttl <= 0:
        return 0
    async with pool.connection() as conn:
        rows = await (
            await conn.execute(
                """
                SELECT t.thread_id FROM rt_thread t
                WHERE t.updated_at < now() - make_interval(mins => %(ttl)s)
                  AND NOT EXISTS (SELECT 1 FROM rt_run r
                                  WHERE r.thread_id = t.thread_id
                                    AND r.status IN ('pending', 'running'))
                  -- Idempotence marker within OUR schema: every executed run
                  -- leaves lifecycle events; the sweep deletes them, so an
                  -- already-swept (or never-run) thread is never a victim.
                  -- INVARIANT: any future checkpoint writer (e.g. a platform-
                  -- parity POST /threads/{id}/state endpoint) must also write
                  -- rt_thread_event or extend this SELECT, else its
                  -- checkpoints become permanently unsweepable.
                  AND EXISTS (SELECT 1 FROM rt_thread_event e
                              WHERE e.thread_id = t.thread_id)
                ORDER BY t.updated_at ASC
                LIMIT %(limit)s
                """,
                {"ttl": ttl, "limit": sweep_limit()},
            )
        ).fetchall()
    swept = 0
    failed = 0
    for row in rows:
        thread_id = str(row["thread_id"])
        try:
            await saver.adelete_thread(thread_id)
            async with pool.connection() as conn:
                await conn.execute(
                    query("DELETE FROM rt_thread_event WHERE thread_id = %s"), (thread_id,)
                )
            swept += 1
        except Exception:  # noqa: BLE001
            failed += 1
            logger.exception("TTL sweep failed for thread %s", thread_id)
    if swept:
        logger.info("TTL sweep removed checkpoint data for %d thread(s)", swept)
    if failed:
        # Failed victims stay candidates (their events survive) and, with
        # ORDER BY updated_at, keep their place in line — surface it.
        logger.warning("TTL sweep failed for %d thread(s); they will be retried", failed)
    return swept
