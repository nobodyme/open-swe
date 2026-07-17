"""Opt-in: the compiled agent graph driven by a REAL model (local LiteLLM).

The scripted-model tests prove the loop mechanics; this proves the graph works
with a genuine chat model over the OpenAI wire protocol — tool binding, message
shapes, finish handling (docs/fast-api-migration/phase-0.md task 4c). Run with:

    uv run pytest -vvv -m litellm tests/agent_graph/test_litellm_smoke.py

Excluded from ``make test`` by ``addopts``; never calls paid cloud APIs.
"""

from __future__ import annotations

import pytest
from langchain_core.messages import AIMessage

from agent.server import get_agent
from tests.agent_graph.conftest import AgentGraphHarness, execution_config
from tests.support.litellm import litellm_chat_model

pytestmark = pytest.mark.litellm


async def test_agent_graph_completes_with_real_local_llm(
    agent_graph_harness: AgentGraphHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent import server

    model = litellm_chat_model()
    # Replace the conftest's scripted factory with the real local model —
    # everything else (middleware, tools, sandbox) stays as the harness wired it.
    monkeypatch.setattr(server, "make_model", lambda _model_id, **_kwargs: model)

    graph = await get_agent(execution_config())
    result = await graph.ainvoke(
        {
            "messages": [
                {
                    "role": "user",
                    "content": (
                        "Reply with a single short sentence confirming you are "
                        "operational. Do not call any tools."
                    ),
                }
            ]
        }
    )

    finals = [m for m in result["messages"] if isinstance(m, AIMessage)]
    assert finals, "real model produced no AI message"
    assert any(str(m.content).strip() for m in finals), "real model produced only empty content"
