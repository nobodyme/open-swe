"""The runtime FastAPI app (phase-1.md T1/D1).

Single process: runtime routers first, then the existing webapp mounted at
``/`` as a catch-all — Starlette matches in registration order, so runtime
paths win and everything else (webhooks, dashboard, OAuth) falls through to
the webapp, preserving today's topology where the webapp lives inside the
server process and ``LANGGRAPH_URL`` points back at the same socket.

Boot order in the lifespan: pool → checkpoint/store ``setup()`` → owned
schema → startup sweep (D2: orphans → ``error`` + completion webhooks) →
cron scheduler.
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from agent_runtime import config as runtime_config
from agent_runtime import db
from agent_runtime.cron_scheduler import CronScheduler
from agent_runtime.executor import RunExecutor
from agent_runtime.registry import GraphRegistry, load_webapp
from agent_runtime.routers import commands, crons, runs, store, streaming, threads
from agent_runtime.runs_repo import RunsRepo
from agent_runtime.threads_repo import ThreadsRepo

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    from langgraph.store.postgres.aio import AsyncPostgresStore

    pool = db.create_pool(runtime_config.database_url())
    await pool.open(wait=True)

    # D1 is a hard single-process assumption (per-thread arbitration locks,
    # seq counters, and live fanout are all in-process). A second worker
    # against the same database silently corrupts arbitration and streams —
    # fail its boot instead. The advisory lock lives on a dedicated
    # connection held for the process lifetime.
    from psycopg import AsyncConnection

    guard_conn = await AsyncConnection.connect(runtime_config.database_url(), autocommit=True)
    # Bounded retry: after a SIGKILL the dead holder's lock releases when
    # Postgres notices the closed socket — usually instant, occasionally a
    # few seconds (chaos restarts hit exactly this window).
    import asyncio as _asyncio

    acquired = False
    for _ in range(20):
        guard_row = await (
            await guard_conn.execute("SELECT pg_try_advisory_lock(hashtext('agent_runtime'))")
        ).fetchone()
        if guard_row and guard_row[0]:
            acquired = True
            break
        await _asyncio.sleep(0.5)
    if not acquired:
        await guard_conn.close()
        await pool.close()
        raise RuntimeError(
            "another agent_runtime process already serves this database — "
            "multiple workers are unsupported (phase-1.md D1)"
        )

    saver = AsyncPostgresSaver(pool)  # type: ignore[arg-type]
    store_instance = AsyncPostgresStore(pool)  # type: ignore[arg-type]
    await saver.setup()
    await store_instance.setup()
    await db.apply_schema(pool)

    registry = GraphRegistry(
        runtime_config.runtime_config_path(), store=store_instance, checkpointer=saver
    )
    threads_repo = ThreadsRepo(pool)
    runs_repo = RunsRepo(pool)
    executor = RunExecutor(
        pool=pool,
        saver=saver,
        registry=registry,
        threads=threads_repo,
        runs=runs_repo,
    )
    cron_scheduler = CronScheduler(pool=pool, executor=executor, threads=threads_repo, saver=saver)

    app.state.pool = pool
    app.state.saver = saver
    app.state.store = store_instance
    app.state.registry = registry
    app.state.threads = threads_repo
    app.state.runs = runs_repo
    app.state.executor = executor
    app.state.crons = cron_scheduler

    await executor.sweep_orphans_on_boot()
    await cron_scheduler.start()

    # Starlette does not run mounted sub-apps' lifespans; the webapp registers
    # its routers in its startup hook, so forward it explicitly (D1 mount).
    from contextlib import AsyncExitStack

    from starlette.routing import Mount

    async with AsyncExitStack() as stack:
        for route in app.routes:
            if isinstance(route, Mount) and hasattr(route.app, "router"):
                await stack.enter_async_context(
                    route.app.router.lifespan_context(route.app)  # type: ignore[attr-defined]
                )
        logger.info("agent_runtime ready (graphs: %s)", ", ".join(registry.assistant_ids))
        try:
            yield
        finally:
            await cron_scheduler.shutdown()
            await guard_conn.close()
            await pool.close()


def create_app() -> FastAPI:
    # uvicorn configures only its own loggers; app loggers (agent.*,
    # agent_runtime.*) need a root handler to be visible in dev.
    if os.environ.get("AGENT_RUNTIME_LOG_LEVEL"):
        logging.basicConfig(
            level=os.environ["AGENT_RUNTIME_LOG_LEVEL"].upper(),
            format="%(asctime)s.%(msecs)03d %(name)s %(message)s",
            datefmt="%H:%M:%S",
        )

    # Load the config's "env" file before the webapp import reads its
    # configuration. DELIBERATE divergence from langgraph dev: dev's .env
    # OVERRIDES the shell; here the shell wins (override=False) — make dev's
    # exported DATABASE_URL must beat a stale .env value, and shell-wins is
    # the less surprising rule. Documented in CLAUDE.md.
    try:
        env_registry = GraphRegistry(runtime_config.runtime_config_path())
    except Exception:  # noqa: BLE001
        logger.exception(
            "Could not read %s; webapp mount and env file skipped",
            runtime_config.runtime_config_path(),
        )
        env_registry = None
    if env_registry is not None and env_registry.env_file:
        from dotenv import load_dotenv

        load_dotenv(
            runtime_config.runtime_config_path().parent / env_registry.env_file,
            override=False,
        )

    app = FastAPI(title="agent_runtime", lifespan=_lifespan)

    @app.get("/ok")
    async def ok() -> dict[str, bool]:
        return {"ok": True}

    app.include_router(threads.router)
    app.include_router(runs.router)
    app.include_router(streaming.router)
    app.include_router(commands.router)
    app.include_router(store.router)
    app.include_router(crons.router)

    # D1: mount the existing webapp last so runtime routes win and everything
    # else (webhooks, dashboard, OAuth, harness control routes) falls through.
    if os.environ.get("AGENT_RUNTIME_NO_WEBAPP") != "1" and env_registry is not None:
        try:
            webapp = load_webapp(env_registry.webapp_path)
            if webapp is not None:
                app.mount("/", webapp)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to mount webapp; serving runtime routes only")

    return app


app = create_app()
