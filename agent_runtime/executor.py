"""Run executor: arbitration, execution, event log, webhooks (phase-1.md T6).

Single-process by design (D1): per-thread ``asyncio.Lock``s serialize
arbitration, so multitask races cannot span processes. Runs execute as
``asyncio.create_task`` — no durable queue, no auto-resume (D3); the
``durability`` kwarg is forwarded to MIT ``astream`` so checkpoints land
before each step, and the startup sweep (D2) marks orphans ``error``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING, Any, cast

from agent_runtime.db import DictPool, query

if TYPE_CHECKING:
    from langchain_core.runnables import RunnableConfig

from agent_runtime import webhooks
from agent_runtime.registry import GraphRegistry
from agent_runtime.runs_repo import RunsRepo
from agent_runtime.streams import (
    EventIdAllocator,
    StreamNormalizer,
    WireEvent,
    lifecycle_event,
    mit_modes_for,
)
from agent_runtime.threads_repo import ThreadsRepo

logger = logging.getLogger(__name__)


def _langgraph_version() -> str:
    try:
        from importlib.metadata import version

        return version("langgraph")
    except Exception:  # noqa: BLE001
        return "unknown"


def _runtime_version() -> str:
    return "agent-runtime/0.1"


class ThreadBusyError(Exception):
    pass


class UnsupportedStrategyError(Exception):
    pass


class ThreadMissingError(Exception):
    pass


class UnknownAssistantError(Exception):
    pass


class RunExecutor:
    def __init__(
        self,
        *,
        pool: DictPool,
        saver: Any,
        registry: GraphRegistry,
        threads: ThreadsRepo,
        runs: RunsRepo,
    ) -> None:
        self._pool = pool
        self._saver = saver
        self._registry = registry
        self._threads = threads
        self._runs = runs
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._webhook_tasks: set[asyncio.Task[Any]] = set()
        self._ids = EventIdAllocator()
        # Per-thread live fanout queues and per-thread seq counters.
        self._subscribers: dict[str, set[asyncio.Queue[WireEvent | None]]] = defaultdict(set)
        self._thread_seq: dict[str, int] = defaultdict(int)

    # -- subscriptions -----------------------------------------------------

    def subscribe(self, thread_id: str) -> asyncio.Queue[WireEvent | None]:
        queue: asyncio.Queue[WireEvent | None] = asyncio.Queue()
        self._subscribers[thread_id].add(queue)
        return queue

    def unsubscribe(self, thread_id: str, queue: asyncio.Queue[WireEvent | None]) -> None:
        subscribers = self._subscribers.get(thread_id)
        if subscribers is not None:
            subscribers.discard(queue)
            if not subscribers:
                del self._subscribers[thread_id]

    # -- event log ----------------------------------------------------------

    async def _emit(self, thread_id: str, run_id: str, event: WireEvent) -> None:
        from agent_runtime.streams import v2_eligible

        if v2_eligible(event.event):
            if thread_id not in self._thread_seq:
                # First emit for this thread in this process: seed from the
                # persisted max so a restart never reissues seqs (a reissued
                # seq breaks `since` resume — the crash/recycle scenario).
                await self.load_thread_seq(thread_id)
            self._thread_seq[thread_id] += 1
            event.seq = self._thread_seq[thread_id]
        event.run_id = run_id
        if not event.event_id:
            event.event_id = self._ids.next_id()
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "INSERT INTO rt_thread_event "
                    "(run_id, thread_id, seq, event_id, event, data) "
                    "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
                    (
                        run_id,
                        thread_id,
                        event.seq or None,
                        event.event_id,
                        event.event,
                        json.dumps(event.data, default=str),
                    ),
                )
            ).fetchone()
        event.ord = int(row["id"]) if row else 0
        for queue in list(self._subscribers.get(thread_id, ())):
            queue.put_nowait(event)

    async def load_thread_seq(self, thread_id: str) -> int:
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM rt_thread_event "
                    "WHERE thread_id = %s",
                    (thread_id,),
                )
            ).fetchone()
        seq = int(row["max_seq"]) if row else 0
        # Keep the in-process counter ahead of anything already persisted
        # (e.g. after a restart).
        self._thread_seq[thread_id] = max(self._thread_seq[thread_id], seq)
        return seq

    async def replay_events(
        self,
        thread_id: str,
        *,
        after_seq: int | None = None,
        after_event_id: str | None = None,
        v2_only: bool = False,
    ) -> list[WireEvent]:
        clauses = ["thread_id = %s"]
        params: list[Any] = [thread_id]
        if v2_only:
            clauses.append("seq IS NOT NULL")
        if after_seq is not None:
            clauses.append("COALESCE(seq, 0) > %s")
            params.append(after_seq)
        if after_event_id is not None:
            clauses.append(
                "id > COALESCE((SELECT id FROM rt_thread_event "
                "WHERE thread_id = %s AND event_id = %s ORDER BY id LIMIT 1), 0)"
            )
            params.extend([thread_id, after_event_id])
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    query(  # noqa: S608
                        f"SELECT id, run_id, seq, event_id, event, data FROM rt_thread_event "
                        f"WHERE {' AND '.join(clauses)} ORDER BY id"
                    ),
                    params,
                )
            ).fetchall()
        return [
            WireEvent(
                event=row["event"],
                data=json.loads(row["data"]),
                event_id=row["event_id"],
                seq=row["seq"] or 0,
                run_id=str(row["run_id"]),
                ord=int(row["id"]),
            )
            for row in rows
        ]

    # -- run lifecycle -------------------------------------------------------

    def _assistant_ids(self, assistant_id: str) -> tuple[str, str]:
        """(graph name, assistant uuid). Accepts either form — the SDK sends
        the graph name; wire responses carry a deterministic uuid5, matching
        how langgraph-api mints system-assistant ids."""
        for name in self._registry.assistant_ids:
            candidate = str(uuid.uuid5(uuid.NAMESPACE_DNS, name))
            if assistant_id in (name, candidate):
                return name, candidate
        # Unknown graph: keep the raw id; resolution fails later with a clear error.
        return assistant_id, str(uuid.uuid5(uuid.NAMESPACE_DNS, assistant_id))

    async def create_run(
        self,
        thread_id: str,
        *,
        assistant_id: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        """Insert + arbitrate + start. ``body`` is the RunCreateBody dump."""
        if_not_exists = body.get("if_not_exists") or "reject"
        thread = await self._threads.get(thread_id)
        if thread is None:
            if if_not_exists != "create":
                raise ThreadMissingError(thread_id)
            await self._threads.create(thread_id, {}, if_exists="do_nothing")

        graph_name, assistant_uuid = self._assistant_ids(assistant_id)
        if graph_name not in self._registry.assistant_ids:
            # Reject at create like dev does — a typo'd graph id must be a
            # loud 4xx, not an async error-webhook loop.
            raise UnknownAssistantError(assistant_id)
        multitask = body.get("multitask_strategy") or "reject"
        run_id = str(uuid.uuid4())
        requested_modes = body.get("stream_mode") or ["values"]
        if isinstance(requested_modes, str):
            requested_modes = [requested_modes]

        # kwargs shape pinned by run_create.json — the enriched config carries
        # the platform stamps dev writes (graph name, ids, auth placeholders).
        client_config = body.get("config") or {}
        client_configurable = dict(client_config.get("configurable") or {})
        client_configurable.update(
            {
                "__after_seconds__": 0,
                "__request_start_time_ms__": int(time.time() * 1000),
                "assistant_id": assistant_uuid,
                "graph_id": graph_name,
                "langgraph_auth_permissions": [],
                "langgraph_auth_user": None,
                "langgraph_auth_user_id": "",
                "langgraph_request_id": str(uuid.uuid4()),
                "run_id": run_id,
                "thread_id": thread_id,
                "user_id": "",
            }
        )
        # At CREATE time only these two; the worker-pickup stamps (env keys,
        # run_attempt, __pregel_node_finished) land in _enrich_kwargs_on_start
        # — run_create.json vs run_complete_webhook_payload.json pin the split.
        config_metadata = dict(client_config.get("metadata") or {})
        config_metadata.update({"assistant_id": assistant_uuid, "created_by": "system"})
        kwargs: dict[str, Any] = {
            "input": body.get("input"),
            "command": body.get("command"),
            "config": {
                **{k: v for k, v in client_config.items() if k not in ("configurable", "metadata")},
                "configurable": client_configurable,
                "metadata": config_metadata,
            },
            "context": {},
            "checkpoint_during": True,
            "durability": body.get("durability") or "async",
            "feedback_keys": None,
            "interrupt_after": None,
            "interrupt_before": None,
            "resumable": bool(body.get("stream_resumable")),
            "stream_mode": requested_modes,
            "subgraphs": False,
            "temporary": False,
            "webhook": body.get("webhook"),
        }
        if body.get("__transport__"):
            kwargs["__transport__"] = body["__transport__"]
        run_metadata = {
            **(body.get("metadata") or {}),
            "assistant_id": assistant_uuid,
            "created_by": "system",
        }
        run = await self._runs.insert(
            run_id=run_id,
            thread_id=thread_id,
            assistant_id=assistant_uuid,
            multitask_strategy=multitask,
            kwargs=kwargs,
            metadata=run_metadata,
        )

        async with self._locks[thread_id]:
            active = [
                r
                for r in await self._runs.active_on_thread(thread_id)
                if r["run_id"] != run["run_id"]
            ]
            if active:
                if multitask == "reject":
                    await self._runs.set_status(run["run_id"], "error")
                    raise ThreadBusyError(thread_id)
                if multitask != "interrupt":
                    await self._runs.set_status(run["run_id"], "error")
                    # No app caller sends enqueue/rollback (D6).
                    raise UnsupportedStrategyError(multitask)
                for stale in active:
                    await self._cancel_locked(stale, action="interrupt")
            # Conditional start: between the insert above and taking the lock,
            # a cancel/arbitration may have finalized this run already — never
            # resurrect a run that announced a terminal state.
            started = await self._runs.start_if_pending(run["run_id"])
            if started is None:
                current = await self._runs.get(thread_id, run["run_id"])
                return current or run
            await self._threads.recompute_status(thread_id)
            self._tasks[run["run_id"]] = asyncio.create_task(self._execute(started))
        return run

    async def cancel_run(self, thread_id: str, run_id: str, *, action: str = "interrupt") -> bool:
        run = await self._runs.get(thread_id, run_id)
        if run is None:
            return False
        async with self._locks[thread_id]:
            await self._cancel_locked(run, action=action)
        return True

    async def _cancel_locked(self, run: dict[str, Any], *, action: str) -> None:
        task = self._tasks.pop(run["run_id"], None)
        if task is not None and not task.done():
            task.cancel()
            try:
                # Bounded: a graph swallowing CancelledError must not wedge
                # the thread lock (and every later create/cancel) forever.
                await asyncio.wait_for(asyncio.shield(task), timeout=15)
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            # The task's own finally already finalized (webhook + lifecycle);
            # finalizing again here double-delivers. Only finalize below when
            # the task never got to run its finally.
        current = await self._runs.get(run["thread_id"], run["run_id"])
        if current is not None and current["status"] in ("pending", "running"):
            await self._finalize(current, "interrupted")

    async def wait(self, run_id: str) -> None:
        task = self._tasks.get(run_id)
        if task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.shield(task)

    # -- state reads ----------------------------------------------------------

    def state_config(self, thread_id: str) -> dict[str, Any]:
        # No CONFIG_KEY_CHECKPOINTER here: the registry attaches the saver as
        # the compiled checkpointer (config-key injection breaks DeltaChannel
        # replay in aget_state — see registry.resolve_managed).
        return {"configurable": {"thread_id": thread_id}}

    def graph_name_for_run(self, run: dict[str, Any]) -> str:
        """rt_run.assistant_id stores the wire uuid; the graph NAME rides in
        the enriched config's graph_id stamp."""
        raw_kwargs = run.get("kwargs")
        kwargs: dict[str, Any] = raw_kwargs if isinstance(raw_kwargs, dict) else {}
        config = kwargs.get("config") or {}
        configurable = config.get("configurable") or {}
        name = configurable.get("graph_id")
        if isinstance(name, str) and name:
            return name
        return run["assistant_id"]

    async def _state_graph(self, thread_id: str) -> tuple[Any, Any | None, dict[str, Any]]:
        """(graph, context-manager, config) for state reads.

        The factory is resolved with the LATEST RUN's stored configurable so
        factory graphs rebuild the same shape that wrote the checkpoints
        (e.g. get_agent's execution gate) — otherwise channel mapping comes
        back empty. CM factories are exited by the caller after the read.
        """
        runs = await self._runs.list(thread_id, limit=1)
        base_configurable: dict[str, Any] = {}
        graph_name = "agent"
        if runs:
            raw = await self._runs.get_raw_kwargs(thread_id, runs[0]["run_id"]) or {}
            raw_config = raw.get("config") or {}
            base_configurable = dict(raw_config.get("configurable") or {})
            graph_name = self.graph_name_for_run({**runs[0], "kwargs": raw})
        configurable = {**base_configurable, "thread_id": thread_id}
        config = {"configurable": configurable}
        graph, cm = await self._registry.resolve_managed(graph_name, config)
        return graph, cm, config

    async def get_state_snapshot(self, thread_id: str) -> Any:
        graph, cm, config = await self._state_graph(thread_id)
        try:
            return await graph.aget_state(config)
        finally:
            if cm is not None:
                with contextlib.suppress(Exception):
                    await cm.__aexit__(None, None, None)

    async def get_state_history(self, thread_id: str, *, limit: int) -> list[Any]:
        graph, cm, config = await self._state_graph(thread_id)
        try:
            snapshots = []
            async for snapshot in graph.aget_state_history(config, limit=limit):
                snapshots.append(snapshot)
            return snapshots
        finally:
            if cm is not None:
                with contextlib.suppress(Exception):
                    await cm.__aexit__(None, None, None)

    # -- execution -------------------------------------------------------------

    async def _enrich_kwargs_on_start(self, run: dict[str, Any]) -> dict[str, Any]:
        """Worker-pickup stamps dev writes into the stored run kwargs."""
        # Raw (unstripped) kwargs: internal dunder keys must survive the
        # rewrite (the serialized run dict strips them).
        raw = await self._runs.get_raw_kwargs(run["thread_id"], run["run_id"])
        kwargs = dict(raw) if isinstance(raw, dict) else {}
        config = dict(kwargs.get("config") or {})
        configurable = dict(config.get("configurable") or {})
        configurable.setdefault("__pregel_node_finished", None)
        metadata = dict(config.get("metadata") or {})
        metadata.update(
            {
                "langgraph_api_url": "self-hosted",
                "langgraph_api_version": _runtime_version(),
                "langgraph_host": "self-hosted",
                "langgraph_plan": "self-hosted",
                "langgraph_version": _langgraph_version(),
                "run_attempt": 1,
            }
        )
        kwargs["config"] = {**config, "configurable": configurable, "metadata": metadata}
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE rt_run SET kwargs = %s::jsonb, updated_at = now() WHERE run_id = %s",
                (json.dumps(kwargs), run["run_id"]),
            )
        return {**run, "kwargs": kwargs}

    def _run_config(self, run: dict[str, Any]) -> dict[str, Any]:
        base = (run["kwargs"].get("config") or {}) if isinstance(run["kwargs"], dict) else {}
        configurable = dict(base.get("configurable") or {})
        configurable.update(
            {
                "thread_id": run["thread_id"],
                "run_id": run["run_id"],
                "graph_id": self.graph_name_for_run(run),
            }
        )
        config = {**base, "configurable": configurable}
        config.setdefault("recursion_limit", base.get("recursion_limit", 100))
        return config

    async def _execute(self, run: dict[str, Any]) -> None:
        thread_id = run["thread_id"]
        run_id = run["run_id"]
        kwargs = run["kwargs"] if isinstance(run["kwargs"], dict) else {}
        requested = kwargs.get("stream_mode") or ["values"]
        if isinstance(requested, str):
            requested = [requested]
        normalizer = StreamNormalizer(requested_modes=requested)
        final_values: Any = None
        status = "success"
        error: BaseException | None = None
        graph_name = self.graph_name_for_run(run)
        graph_cm: Any | None = None
        try:
            run = await self._enrich_kwargs_on_start(run)
            config = self._run_config(run)
            graph, graph_cm = await self._registry.resolve_managed(graph_name, config)
            await self._emit(thread_id, run_id, lifecycle_event(run_id, "running", graph_name))
            run_input = self._resolve_input(kwargs)
            durability = kwargs.get("durability")
            astream_kwargs: dict[str, Any] = {
                "stream_mode": mit_modes_for(requested),
                "subgraphs": True,
            }
            if durability:
                astream_kwargs["durability"] = durability
            async for ns, mode, payload in graph.astream(
                run_input, cast("RunnableConfig", config), **astream_kwargs
            ):
                if mode == "values" and not ns:
                    final_values = payload
                for event in normalizer.normalize(tuple(ns), mode, payload):
                    await self._emit(thread_id, run_id, event)
            if await self._has_pending_interrupt(graph, config):
                status = "interrupted"
        except asyncio.CancelledError:
            status = "interrupted"
        except Exception as exc:  # noqa: BLE001
            logger.exception("Run %s failed", run_id)
            status = "error"
            error = exc
        finally:
            if graph_cm is not None:
                with contextlib.suppress(Exception):
                    await graph_cm.__aexit__(None, None, None)
            try:
                await self._finalize(run, status, values=final_values, exception=error)
            except Exception:
                # A swallowed finalize failure leaves the run stuck `running`
                # (busy thread, spinning join) until the next boot sweep —
                # make it loud even though we can't do better here.
                logger.exception("Failed to finalize run %s as %s", run_id, status)
            self._tasks.pop(run_id, None)

    def _resolve_input(self, kwargs: dict[str, Any]) -> Any:
        command = kwargs.get("command")
        if isinstance(command, dict) and command:
            from langgraph.types import Command

            if "resume" in command:
                return Command(resume=command["resume"])
            if "update" in command:
                return Command(update=command["update"])
            if "goto" in command:
                return Command(goto=command["goto"])
        return kwargs.get("input")

    async def _has_pending_interrupt(self, graph: Any, config: dict[str, Any]) -> bool:
        try:
            snapshot = await graph.aget_state(config)
        except Exception:  # noqa: BLE001
            return False
        return bool(getattr(snapshot, "next", None)) and any(
            getattr(task, "interrupts", None) for task in getattr(snapshot, "tasks", ())
        )

    async def _finalize(
        self,
        run: dict[str, Any],
        status: str,
        *,
        values: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        thread_id = run["thread_id"]
        run_id = run["run_id"]
        # Exactly-once: the conditional terminal transition is the gate for
        # webhook + lifecycle emission. A second finalizer (cancel racing the
        # task's own finally, timeout paths) finds the run already terminal
        # and emits nothing.
        updated = await self._runs.finish_if_active(run_id, status)
        if updated is None:
            return
        if values is not None:
            from agent_runtime.streams import _dump

            await self._threads.set_values(thread_id, _dump(values))
        await self._threads.recompute_status(thread_id)
        lifecycle_name = "completed" if status == "success" else status
        with contextlib.suppress(Exception):
            await self._emit(
                thread_id,
                run_id,
                lifecycle_event(run_id, lifecycle_name, self.graph_name_for_run(run)),
            )
        # Completion webhook fires for EVERY terminal state (locked decision).
        # Keep a strong reference: the loop holds tasks weakly, and a GC'd
        # webhook task is a silently dropped completion signal.
        from agent_runtime.streams import _dump as dump_values

        task = asyncio.get_running_loop().create_task(
            webhooks.send_completion_webhook(
                updated,
                status=status,
                values=dump_values(values) if values is not None else None,
                exception=exception,
            )
        )
        self._webhook_tasks.add(task)
        task.add_done_callback(self._webhook_tasks.discard)

    # -- startup sweep (D2) ---------------------------------------------------

    async def sweep_orphans_on_boot(self) -> int:
        orphans = await self._runs.sweep_orphans()
        for run in orphans:
            await self._threads.recompute_status(run["thread_id"])
            await webhooks.send_completion_webhook(run, status="error", exception=None)
        if orphans:
            logger.warning("Startup sweep marked %d orphaned run(s) as error", len(orphans))
        return len(orphans)
