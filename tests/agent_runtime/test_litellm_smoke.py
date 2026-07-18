"""Opt-in: a REAL model streaming through the runtime (phase-1.md T12).

Targets the dedicated one-node ``model_call`` graph — no sandbox, no auth, no
agent-factory stubs — so the only thing under test is real-model streaming
through the executor and the T8 normalizers. Run with:

    uv run pytest -vvv -m litellm tests/agent_runtime/test_litellm_smoke.py

Never calls a paid cloud API (local LiteLLM proxy from .env).
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from tests.support.litellm import litellm_settings

pytestmark = pytest.mark.litellm


async def test_model_call_graph_streams_through_runtime(sdk_client: Any, monkeypatch: Any) -> None:
    settings = litellm_settings()  # skips loudly when the proxy isn't configured
    for key, value in settings.items():
        monkeypatch.setenv(key, value)

    thread_id = str(uuid.uuid4())
    await sdk_client.threads.create(thread_id=thread_id)

    parts = []
    async for part in sdk_client.runs.stream(
        thread_id,
        "model_call",
        input={
            "messages": [
                {
                    "role": "user",
                    "content": "Reply with one short sentence confirming you are operational.",
                }
            ]
        },
        stream_mode=["values", "messages-tuple"],
    ):
        parts.append(part)

    assert parts[0].event == "metadata"
    values = [p for p in parts if p.event == "values"]
    assert values, [p.event for p in parts]
    final_messages = values[-1].data["messages"]
    assert final_messages[-1]["type"] == "ai"
    assert str(final_messages[-1]["content"]).strip(), "empty model reply"

    message_events = [p for p in parts if p.event == "messages"]
    assert message_events, "no messages-tuple events from the real model"

    ids = [p.id for p in parts if p.id]
    assert ids == sorted(ids, key=lambda x: tuple(int(n) for n in x.split("-")))

    run = (await sdk_client.runs.list(thread_id, limit=1))[0]
    assert run["status"] == "success"
