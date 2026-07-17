"""Deterministic graph served by ``langgraph dev`` for the contract suite.

This module is loaded by the dev server via a config that ``conftest.py``
generates into the server's session tmpdir (absolute paths, because the inmem
runtime resolves graph paths and its ``.langgraph_api`` state dir against
cwd). Pattern copied from ``tests/e2e`` — no imports from it, per
docs/fast-api-migration/phase-0.md task 4a. It must stay
fully self-contained: no sandbox, no ``agent/`` imports, no network calls, and
a scripted fake model so every transcript the contract tests capture is
byte-stable after normalization.

Script (per human turn):

* turn input carries ``busy_seconds > 0`` → the model emits one ``busy_wait``
  tool call; the tool node ``asyncio.sleep``s that long (the deterministic
  "busy window" the double-dispatch/interrupt tests rely on), then the model
  emits the final message.
* otherwise → the model immediately emits ``echo: <last human text>``.
"""

from __future__ import annotations

import asyncio
from typing import Any

from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.graph import END, START, MessagesState, StateGraph

BUSY_TOOL_NAME = "busy_wait"


class ContractState(MessagesState):
    # Deterministic busy window, read from the run input:
    # {"messages": [...], "busy_seconds": 5}
    busy_seconds: float


def _text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            part.get("text", "") if isinstance(part, dict) else str(part) for part in content
        )
    return str(content)


class ScriptedContractModel(BaseChatModel):
    """Two-step scripted model: optional busy_wait tool call, then an echo."""

    busy_seconds: float = 0.0

    @property
    def _llm_type(self) -> str:
        return "contract-scripted"

    def bind_tools(self, tools: Any, **kwargs: Any) -> ScriptedContractModel:  # noqa: ARG002
        return self

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,  # noqa: ARG002
        run_manager: CallbackManagerForLLMRun | None = None,  # noqa: ARG002
        **kwargs: Any,
    ) -> ChatResult:
        last_human = max(
            (i for i, m in enumerate(messages) if isinstance(m, HumanMessage)), default=-1
        )
        prompt = _text(messages[last_human].content) if last_human >= 0 else ""
        steps_taken = sum(1 for m in messages[last_human + 1 :] if isinstance(m, AIMessage))

        if steps_taken == 0 and self.busy_seconds > 0:
            message = AIMessage(
                content="Holding the thread busy for a deterministic window.",
                tool_calls=[
                    {
                        "name": BUSY_TOOL_NAME,
                        "args": {"seconds": self.busy_seconds},
                        "id": "call-busy-1",
                    }
                ],
            )
        else:
            message = AIMessage(content=f"echo: {prompt}")
        return ChatResult(generations=[ChatGeneration(message=message)])


async def agent(state: ContractState) -> dict[str, Any]:
    model = ScriptedContractModel(busy_seconds=float(state.get("busy_seconds") or 0.0))
    response = await model.ainvoke(state["messages"])
    return {"messages": [response]}


async def tools(state: ContractState) -> dict[str, Any]:
    last = state["messages"][-1]
    results: list[ToolMessage] = []
    for call in getattr(last, "tool_calls", []) or []:
        seconds = float(call["args"].get("seconds", 0.0))
        await asyncio.sleep(seconds)
        results.append(
            ToolMessage(
                content=f"waited {seconds:g}s",
                name=BUSY_TOOL_NAME,
                tool_call_id=call["id"],
            )
        )
    return {"messages": results}


def _route(state: ContractState) -> str:
    last = state["messages"][-1]
    return "tools" if getattr(last, "tool_calls", None) else END


builder = StateGraph(ContractState)
builder.add_node("agent", agent)
builder.add_node("tools", tools)
builder.add_edge(START, "agent")
builder.add_conditional_edges("agent", _route, {"tools": "tools", END: END})
builder.add_edge("tools", "agent")

graph = builder.compile()
