"""Stub seam for invoking the real compiled agent graph with no server process.

Every patch below fakes an *external boundary* of ``agent.server.get_agent``;
the deepagents loop, the middleware stack, and the tools stay real. Each stub
is annotated with the ``server.py`` code it replaces so a factory change breaks
loudly here (docs/fast-api-migration/phase-0.md task 4a / risk 5).
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from deepagents.backends import LocalShellBackend
from langgraph.graph.state import RunnableConfig

from agent.utils import ttl_cache
from agent.utils.sandbox_state import SANDBOX_BACKENDS, set_sandbox_backend
from tests.agent_graph.fake_model import ScriptedChatModel

THREAD_ID = "agent-graph-test-thread"

TEAM_DEFAULT_MODEL = "anthropic:claude-sonnet-5"


def execution_config(thread_id: str = THREAD_ID) -> RunnableConfig:
    # __is_for_execution__ is what graph_loaded_for_execution() gates on
    # (agent/runtime/execution.py); without it get_agent returns the inert
    # no-sandbox agent used for graph registration.
    return {
        "configurable": {
            "thread_id": thread_id,
            "__is_for_execution__": True,
        }
    }


@dataclass
class FakeThreads:
    updates: list[dict[str, Any]] = field(default_factory=list)

    async def update(self, thread_id: str, *, metadata: dict[str, Any]) -> dict[str, Any]:
        self.updates.append({"thread_id": thread_id, "metadata": metadata})
        return {"thread_id": thread_id, "metadata": metadata}


@dataclass
class FakeLangGraphClient:
    threads: FakeThreads = field(default_factory=FakeThreads)


@dataclass
class AgentGraphHarness:
    """What a test gets back: the scripted model + recorders for assertions."""

    model: ScriptedChatModel
    client: FakeLangGraphClient
    usage_records: list[dict[str, Any]]
    work_dir: str


@pytest.fixture
def agent_graph_harness(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[AgentGraphHarness]:
    from agent import server

    sandbox_root = tmp_path / "sandbox"
    sandbox_root.mkdir()
    backend = LocalShellBackend(root_dir=str(sandbox_root), virtual_mode=True, inherit_env=True)

    model = ScriptedChatModel(script=[], seen_prompts=[])
    client = FakeLangGraphClient()
    usage_records: list[dict[str, Any]] = []

    # server.py:835-839 — the settings-loader gather.
    async def fake_team_defaults(_kind: str) -> Any:
        return ((TEAM_DEFAULT_MODEL, None), (TEAM_DEFAULT_MODEL, None))

    async def fake_gateway_enabled() -> bool:
        return False

    async def fake_profile(_login: str | None) -> None:
        return None

    async def fake_fable_enabled() -> bool:
        return False

    monkeypatch.setattr(server, "_cached_team_default_model_pair", fake_team_defaults)
    monkeypatch.setattr(server, "_cached_gateway_enabled", fake_gateway_enabled)
    monkeypatch.setattr(server, "_cached_profile", fake_profile)
    monkeypatch.setattr(server, "_cached_fable_enabled", fake_fable_enabled)

    # server.py:979-984 — the model factory (the LLM is the boundary we fake).
    monkeypatch.setattr(server, "make_model", lambda _model_id, **_kwargs: model)
    # server.py:925 — no fallback middleware in tests.
    monkeypatch.setattr(server, "fallback_model_id_for", lambda _model_id: None)
    monkeypatch.delenv("LLM_FALLBACK_MODEL_ID", raising=False)

    # server.py:756-798 — PrepareAgentRunMiddleware._prepare boundaries.
    async def fake_resolve_github_token(_config: Any, _thread_id: str) -> tuple[str, None]:
        return ("dummy-github-token", None)

    async def fake_ensure_sandbox(_thread_id: str, repo: Any = None) -> LocalShellBackend:  # noqa: ARG001
        return backend

    async def fake_work_dir(_backend: Any) -> str:
        return str(sandbox_root)

    async def fake_prompt_default_repo(_configurable: Any) -> None:
        return None

    async def fake_repo_custom_instructions(_repo: Any) -> None:
        return None

    async def fake_record_usage(**kwargs: Any) -> None:
        usage_records.append(kwargs)

    monkeypatch.setattr(server, "resolve_github_token", fake_resolve_github_token)
    monkeypatch.setattr(server, "ensure_sandbox_for_thread", fake_ensure_sandbox)
    monkeypatch.setattr(server, "aresolve_sandbox_work_dir", fake_work_dir)
    monkeypatch.setattr(server, "_resolve_prompt_default_repo", fake_prompt_default_repo)
    monkeypatch.setattr(server, "_resolve_repo_custom_instructions", fake_repo_custom_instructions)
    monkeypatch.setattr(server, "resolve_triggering_user_identity", lambda _cfg, _tok: None)
    monkeypatch.setattr(server, "record_agent_thread_usage", fake_record_usage)
    # server.py:157 — the module-level langgraph client used by _prepare.
    monkeypatch.setattr(server, "client", client)

    # server.py:956-960 — optional tool loaders stay offline.
    async def fake_observability_authorized(_config: Any, _login: str | None) -> bool:
        return False

    async def fake_observability_tools(_authorized: bool) -> list[Any]:
        return []

    async def fake_corridor_tools() -> list[Any]:
        return []

    monkeypatch.setattr(server, "_observability_authorized", fake_observability_authorized)
    monkeypatch.setattr(server, "_load_observability_tools", fake_observability_tools)
    monkeypatch.setattr(server, "_load_corridor_mcp_tools", fake_corridor_tools)
    monkeypatch.setattr(server, "load_browser_tools", lambda: [])

    # refresh_github_proxy_before_model would try a real proxy refresh.
    from agent.middleware import refresh_github_proxy

    async def fake_refresh(_thread_id: str) -> None:
        return None

    monkeypatch.setattr(refresh_github_proxy, "maybe_refresh_proxy_token", fake_refresh)

    # Per-test isolation for process-global caches (phase-0.md task 4a).
    ttl_cache._CACHE.clear()
    SANDBOX_BACKENDS.clear()
    set_sandbox_backend(THREAD_ID, backend)
    yield AgentGraphHarness(
        model=model,
        client=client,
        usage_records=usage_records,
        work_dir=str(sandbox_root),
    )
    SANDBOX_BACKENDS.clear()
    ttl_cache._CACHE.clear()
