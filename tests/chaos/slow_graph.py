"""Slow, checkpoint-dense graph for the SIGKILL chaos suite (phase-2.md T3).

~30 sequential steps, each ~0.5s, each appending its index to state — so a
kill at any moment leaves a measurable, strictly consistent PREFIX of steps
in the checkpoint. Self-contained: no agent/ or tests/e2e imports (D5).
"""

from __future__ import annotations

import asyncio
from typing import Any

from langgraph.graph import END, START, MessagesState, StateGraph

TOTAL_STEPS = 30
STEP_SECONDS = 0.5


class SlowState(MessagesState):
    steps: list[int]


async def _step(state: SlowState) -> dict[str, Any]:
    done = list(state.get("steps") or [])
    await asyncio.sleep(STEP_SECONDS)
    return {"steps": [*done, len(done)]}


def _route(state: SlowState) -> str:
    return "step" if len(state.get("steps") or []) < TOTAL_STEPS else END


_builder = StateGraph(SlowState)
_builder.add_node("step", _step)
_builder.add_edge(START, "step")
_builder.add_conditional_edges("step", _route, {"step": "step", END: END})

graph = _builder.compile()
