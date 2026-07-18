"""Scripted chat model for compiled-graph tests.

Pattern copied from tests/e2e/fake_llm.py — deliberately NOT imported from
there (docs/fast-api-migration/phase-0.md task 4a: importing tests/e2e modules
mutates os.environ and rebinds agent internals process-wide).

The model replays a fixed list of AIMessages. The step index is the number of
AIMessages already in the conversation, so re-invocations from middleware stay
deterministic. Every call's message list is recorded for assertions on what
the model actually saw (e.g. the rendered system prompt).
"""

from __future__ import annotations

from typing import Any

from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult


def tool_call_message(content: str, name: str, args: dict[str, Any], call_id: str) -> AIMessage:
    return AIMessage(content=content, tool_calls=[{"name": name, "args": args, "id": call_id}])


class ScriptedChatModel(BaseChatModel):
    """Returns the next scripted AIMessage; records every prompt it receives."""

    script: list[AIMessage] = []
    seen_prompts: list[list[BaseMessage]] = []

    @property
    def _llm_type(self) -> str:
        return "scripted"

    def bind_tools(self, tools: Any, **kwargs: Any) -> ScriptedChatModel:  # noqa: ARG002
        return self

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,  # noqa: ARG002
        run_manager: CallbackManagerForLLMRun | None = None,  # noqa: ARG002
        **kwargs: Any,
    ) -> ChatResult:
        self.seen_prompts.append(list(messages))
        step_index = sum(1 for m in messages if isinstance(m, AIMessage))
        if step_index < len(self.script):
            message = self.script[step_index]
        else:
            message = AIMessage(content="(script exhausted)")
        return ChatResult(generations=[ChatGeneration(message=message)])
