"""Compiled scheduler-graph tests (docs/fast-api-migration/phase-0.md task 4b).

``tests/reviewer/test_reconcile_sweep.py`` pins reconcile *internals*; these
pin that the compiled ``scheduler`` graph wires and invokes them — the graph
``langgraph.json`` registers and Phase 1's runtime must execute. Client fakes
are modeled on that module's ``_FakeThreads``/``_FakeRuns`` pattern.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from agent import reconcile as reconcile_module
from agent import scheduler as scheduler_module
from agent.scheduler import get_scheduler


@dataclass
class _FakeThreads:
    pages: list[list[dict[str, Any]]] = field(default_factory=list)
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def search(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(kwargs)
        return self.pages.pop(0) if self.pages else []


@dataclass
class _FakeRuns:
    runs_by_thread: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    cancelled: list[dict[str, Any]] = field(default_factory=list)

    async def list(self, thread_id: str, *, status: str) -> list[dict[str, Any]]:
        assert status == "pending"
        return self.runs_by_thread.get(thread_id, [])

    async def cancel_many(self, *, thread_id: str, run_ids: list[str], action: str) -> None:
        self.cancelled.append({"thread_id": thread_id, "run_ids": run_ids, "action": action})


@dataclass
class _FakeClient:
    threads: _FakeThreads
    runs: _FakeRuns


async def test_reconcile_task_sweeps_stale_runs_through_the_compiled_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale_created = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    fresh_created = datetime.now(UTC).isoformat()
    fake = _FakeClient(
        threads=_FakeThreads(pages=[[{"thread_id": "t-busy"}]]),
        runs=_FakeRuns(
            runs_by_thread={
                "t-busy": [
                    {"run_id": "r-stale", "created_at": stale_created},
                    {"run_id": "r-fresh", "created_at": fresh_created},
                ]
            }
        ),
    )
    monkeypatch.setattr(reconcile_module, "langgraph_client", lambda: fake)

    graph = get_scheduler()
    result = await graph.ainvoke({"task": "reconcile"})

    assert result["result"] == {"threads_checked": 1, "stale_runs": 1, "cancelled": 1}
    assert fake.threads.calls[0]["status"] == "busy"
    assert fake.runs.cancelled == [
        {"thread_id": "t-busy", "run_ids": ["r-stale"], "action": "interrupt"}
    ]


async def test_schedule_tick_launches_the_scheduled_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launched: list[str] = []

    async def fake_launch(schedule_id: str) -> dict[str, Any]:
        launched.append(schedule_id)
        return {"status": "started", "schedule_id": schedule_id}

    monkeypatch.setattr(scheduler_module, "launch_scheduled_agent_run", fake_launch)

    graph = get_scheduler()
    result = await graph.ainvoke(
        {},
        config={"configurable": {"schedule_id": "sched-42"}},
    )

    assert launched == ["sched-42"]
    assert result["result"]["status"] == "started"


async def test_missing_schedule_id_is_reported_not_raised() -> None:
    graph = get_scheduler()
    result = await graph.ainvoke({})
    assert result["result"] == {"status": "missing_schedule_id"}
