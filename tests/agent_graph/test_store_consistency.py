"""Pin MIGRATION §1's store-consistency constraint at the graph level.

``check_message_queue_before_model`` drains queued follow-ups via the
in-process ``get_store()`` — the store the graph was compiled/invoked with.
In Phase 1 the compiled-in store and the REST-served store must be the SAME
AsyncPostgresStore; this test pins the compile-time attachment contract that
makes that an invariant rather than a coincidence
(docs/fast-api-migration/phase-0.md task 4d).
"""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.store.memory import InMemoryStore

from agent.server import get_agent
from tests.agent_graph.conftest import THREAD_ID, AgentGraphHarness, execution_config

QUEUED_TEXT = "Queued follow-up: also update the docs."


async def test_queue_drain_reads_the_attached_store(
    agent_graph_harness: AgentGraphHarness,
) -> None:
    harness = agent_graph_harness
    harness.model.script = [AIMessage(content="Done.")]

    store = InMemoryStore()
    await store.aput(
        ("queue", THREAD_ID),
        "pending_messages",
        {"messages": [{"content": QUEUED_TEXT}]},
    )

    graph = await get_agent(execution_config())
    # get_agent().with_config() returns a Pregel copy, so the store attaches
    # exactly the way a serving runtime would attach it at compile time.
    graph.store = store

    result = await graph.ainvoke({"messages": [{"role": "user", "content": "Start."}]})

    human_texts = [str(m.content) for m in result["messages"] if isinstance(m, HumanMessage)]
    assert any(QUEUED_TEXT in text for text in human_texts), human_texts

    # Drained exactly once: the queue entry is deleted from the same store.
    assert await store.aget(("queue", THREAD_ID), "pending_messages") is None


async def test_no_store_attached_is_a_clean_no_op(
    agent_graph_harness: AgentGraphHarness,
) -> None:
    """Without a store the middleware must skip quietly, not fail the run."""
    harness = agent_graph_harness
    harness.model.script = [AIMessage(content="Done.")]

    graph = await get_agent(execution_config())
    result = await graph.ainvoke({"messages": [{"role": "user", "content": "Start."}]})

    final = result["messages"][-1]
    assert isinstance(final, AIMessage)
    assert final.content == "Done."
