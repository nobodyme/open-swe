"""First runtime-independent executions of the real compiled agent graph.

These invoke ``get_agent``'s full deepagents graph — real middleware stack,
real tool wiring, real sandbox backend (local shell in a tmp dir) — with no
server process. Phase 1 reran them unchanged against ``agent_runtime``
(docs/fast-api-migration/phase-0.md task 4).
"""

from __future__ import annotations

from langchain_core.messages import AIMessage, ToolMessage

from agent.server import get_agent
from tests.agent_graph.conftest import (
    TEAM_DEFAULT_MODEL,
    THREAD_ID,
    AgentGraphHarness,
    execution_config,
)
from tests.agent_graph.fake_model import tool_call_message


async def test_scripted_tool_loop_executes_and_completes(
    agent_graph_harness: AgentGraphHarness,
) -> None:
    """The compiled graph runs a model→tool→model loop to completion."""
    harness = agent_graph_harness
    harness.model.script = [
        tool_call_message(
            "Running a command in the sandbox.",
            "execute",
            {"command": "echo agent-graph-ok"},
            "call-exec",
        ),
        AIMessage(content="All done."),
    ]

    graph = await get_agent(execution_config())
    result = await graph.ainvoke({"messages": [{"role": "user", "content": "Run the check."}]})

    messages = result["messages"]
    tool_outputs = [
        m.content if isinstance(m.content, str) else str(m.content)
        for m in messages
        if isinstance(m, ToolMessage)
    ]
    assert any("agent-graph-ok" in out for out in tool_outputs), tool_outputs
    final = messages[-1]
    assert isinstance(final, AIMessage)
    assert final.content == "All done."


async def test_prepare_middleware_renders_prompt_and_records_thread(
    agent_graph_harness: AgentGraphHarness,
) -> None:
    """PrepareAgentRunMiddleware injects the rendered system prompt and
    persists thread metadata + usage before the first model call."""
    harness = agent_graph_harness
    harness.model.script = [AIMessage(content="Acknowledged.")]

    graph = await get_agent(execution_config())
    await graph.ainvoke({"messages": [{"role": "user", "content": "Hello"}]})

    # The model must have seen a system prompt rendered against the sandbox
    # work dir (server.py:800-815).
    assert harness.model.seen_prompts, "model was never called"
    first_prompt = harness.model.seen_prompts[0]
    system_text = str(first_prompt[0].content)
    assert harness.work_dir in system_text

    # Thread metadata written through the (faked) langgraph client
    # (server.py:777-786).
    assert harness.client.threads.updates
    metadata = harness.client.threads.updates[0]["metadata"]
    assert metadata["agent_kind"] == "agent"
    assert metadata["model"] == TEAM_DEFAULT_MODEL

    # Usage recorded (server.py:787-794).
    assert harness.usage_records
    assert harness.usage_records[0]["thread_id"] == THREAD_ID


async def test_factory_returns_inert_agent_without_thread_id() -> None:
    """Graph registration (no thread, not for execution) must not touch
    sandbox/settings machinery — get_agent returns a bare agent."""
    graph = await get_agent({"configurable": {}})
    assert graph is not None
