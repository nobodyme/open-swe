"""Deterministic test graphs for the runtime suite (phase-1.md T2).

Registered via ``runtime.test.json``. Self-contained: no ``agent/`` imports,
no sandbox, no network — except ``model_call``, which builds its chat model
lazily from the LiteLLM env at node runtime (only the litellm smoke runs it).
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.types import interrupt


def _last_human(messages: list[Any]) -> str:
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            content = message.content
            return content if isinstance(content, str) else str(content)
    return ""


# -- echo: basic run lifecycle -------------------------------------------------


class EchoState(MessagesState):
    pass


async def _echo(state: EchoState) -> dict[str, Any]:
    return {"messages": [AIMessage(content=f"echo: {_last_human(state['messages'])}")]}


_echo_builder = StateGraph(EchoState)
_echo_builder.add_node("agent", _echo)
_echo_builder.add_edge(START, "agent")
_echo_builder.add_edge("agent", END)
echo = _echo_builder.compile()


# -- slow_busy: deterministic busy window over N checkpointed steps -------------


class SlowState(MessagesState):
    busy_seconds: float
    steps_done: int


async def _slow_step(state: SlowState) -> dict[str, Any]:
    await asyncio.sleep(float(state.get("busy_seconds") or 0.5))
    return {"steps_done": int(state.get("steps_done") or 0) + 1}


async def _slow_finish(state: SlowState) -> dict[str, Any]:
    return {"messages": [AIMessage(content=f"echo: {_last_human(state['messages'])}")]}


def _slow_route(state: SlowState) -> str:
    return "step" if int(state.get("steps_done") or 0) < 3 else "finish"


_slow_builder = StateGraph(SlowState)
_slow_builder.add_node("step", _slow_step)
_slow_builder.add_node("finish", _slow_finish)
_slow_builder.add_edge(START, "step")
_slow_builder.add_conditional_edges("step", _slow_route, {"step": "step", "finish": "finish"})
_slow_builder.add_edge("finish", END)
slow_busy = _slow_builder.compile()


# -- interrupting: interrupt() mid-graph, resumed via Command(resume=...) -------


class InterruptState(MessagesState):
    approved: bool


async def _ask(state: InterruptState) -> dict[str, Any]:
    decision = interrupt({"question": "approve?", "prompt": _last_human(state["messages"])})
    return {"approved": bool(decision)}


async def _conclude(state: InterruptState) -> dict[str, Any]:
    verdict = "approved" if state.get("approved") else "rejected"
    return {"messages": [AIMessage(content=f"resolution: {verdict}")]}


_int_builder = StateGraph(InterruptState)
_int_builder.add_node("ask", _ask)
_int_builder.add_node("conclude", _conclude)
_int_builder.add_edge(START, "ask")
_int_builder.add_edge("ask", "conclude")
_int_builder.add_edge("conclude", END)
interrupting = _int_builder.compile()


# -- store_probe: reads/writes the compile-time store from inside a node --------


class StoreProbeState(MessagesState):
    store_key: str
    store_value: dict[str, Any] | None


async def _store_probe(state: StoreProbeState) -> dict[str, Any]:
    from langgraph.config import get_store

    store = get_store()
    key = state.get("store_key") or "probe"
    existing = await store.aget(("runtime-probe",), key)
    await store.aput(("runtime-probe",), f"{key}-written", {"from": "graph-node"})
    return {
        "store_value": existing.value if existing is not None else None,
        "messages": [AIMessage(content="store probe done")],
    }


_store_builder = StateGraph(StoreProbeState)
_store_builder.add_node("probe", _store_probe)
_store_builder.add_edge(START, "probe")
_store_builder.add_edge("probe", END)
store_probe = _store_builder.compile()


# -- model_call: one real chat-model node (LiteLLM smoke only) ------------------


async def _model_node(state: MessagesState) -> dict[str, Any]:
    from langchain_openai import ChatOpenAI
    from pydantic import SecretStr

    base_url = os.environ["LITELLM_BASE_URL"].rstrip("/")
    if not base_url.endswith("/v1"):
        base_url = f"{base_url}/v1"
    model = ChatOpenAI(
        model=os.environ["LITELLM_MODEL"],
        base_url=base_url,
        api_key=SecretStr(os.environ["LITELLM_API_KEY"]),
        temperature=0,
        timeout=120,
        max_retries=1,
    )
    response = await model.ainvoke(state["messages"])
    return {"messages": [response]}


_model_builder = StateGraph(MessagesState)
_model_builder.add_node("model", _model_node)
_model_builder.add_edge(START, "model")
_model_builder.add_edge("model", END)
model_call = _model_builder.compile()
