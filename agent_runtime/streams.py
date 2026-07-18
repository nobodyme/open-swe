"""Pure stream normalizers: executor items → wire events.

Two consumers, one canonical event stream (rows in ``rt_thread_event``):

* SDK run/thread streams (``runs.stream``, ``threads.join_stream``): events
  named after the run's requested stream modes (``metadata``, ``values``,
  ``updates``, ``messages``, ``checkpoints``, …), SSE ``id:`` = the durable
  redis-style ``<ms>-<n>`` event id (what ``Last-Event-ID`` resumes against).
* The v2 dashboard wire (``POST /threads/{id}/stream/events``): the channel
  envelope ``{type, event_id, seq, method, params:{namespace, timestamp,
  data}}``, SSE ``id:`` = session seq, resumed via body ``since``.

The Phase 0 golden transcripts (tests/contract/golden/) are the spec for both
shapes. ``messages-tuple``/``tools``/``events`` are not MIT StreamModes
(MIGRATION §1): ``messages-tuple`` is MIT ``messages``; ``tools`` is
synthesized from updates; ``events`` is accepted-and-ignored (no v2 channel
and no app consumer — divergence ledger).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from langchain_core.messages import BaseMessage

# Dashboard modes (thread_api.py:56-64) → what we ask MIT astream for.
MIT_STREAM_MODES = {"values", "updates", "messages", "checkpoints", "custom", "tasks", "debug"}
MODE_ALIASES = {"messages-tuple": "messages"}
IGNORED_MODES = {"events"}  # no MIT equivalent, no v2 channel, no app consumer

# v2 channel per wire event name. Pinned by stream_events_transcript.json:
# dev's v2 stream for a non-streaming model carries ONLY lifecycle + values —
# run-mode events (updates, messages-tuple, whole-message completes,
# checkpoints) never surface on the v2 channels; token chunks do.
V2_CHANNEL_FOR_EVENT = {
    "values": "values",
    "lifecycle": "lifecycle",
    "messages/partial": "messages",
    "tools": "tools",
    "custom": "custom",
    "tasks": "tasks",
    "updates": None,
    "messages": None,
    "messages/metadata": None,
    "messages/complete": None,
    "checkpoints": None,
    "metadata": None,
    "events": None,
}


def v2_eligible(event_name: str) -> bool:
    return V2_CHANNEL_FOR_EVENT.get(event_name) is not None


def mit_modes_for(requested: list[str]) -> list[str]:
    """The stream_mode list to pass to MIT astream (always a list — the
    3-tuple (ns, mode, payload) shape depends on it; phase-1.md T6)."""
    modes: set[str] = {"values"}  # terminal values are always needed
    for mode in requested:
        mode = MODE_ALIASES.get(mode, mode)
        if mode in MIT_STREAM_MODES:
            modes.add(mode)
    return sorted(modes)


def _dump(obj: Any) -> Any:
    """Best-effort JSON-able rendering of langchain objects."""
    if isinstance(obj, BaseMessage):
        data = obj.model_dump(mode="json")
        # Wire shape carries `type`, not the serialized class envelope.
        data.pop("example", None)
        return data
    # Interrupt (and friends) are plain dataclasses — without this branch they
    # fall through to `default=str` and the wire carries a repr STRING where
    # the dashboard expects {"value": ..., "id": ...}.
    import dataclasses

    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return _dump(dataclasses.asdict(obj))
    if isinstance(obj, dict):
        return {k: _dump(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_dump(v) for v in obj]
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    remainder: Any = obj
    if hasattr(remainder, "model_dump"):
        return remainder.model_dump(mode="json")
    return json.loads(json.dumps(remainder, default=str))


@dataclass
class WireEvent:
    """One canonical event: stored in rt_thread_event and served both ways."""

    event: str  # SSE event name on the SDK stream / v2 method source
    data: Any
    event_id: str = ""
    seq: int = 0  # v2 protocol sequence; 0 = not a v2-channel event
    run_id: str = ""
    ord: int = 0  # global replay order (rt_thread_event.id)


@dataclass
class EventIdAllocator:
    """Redis-style ``<ms>-<n>`` ids, monotonic within the allocator."""

    _last_ms: int = 0
    _counter: int = 0

    def next_id(self) -> str:
        ms = int(time.time() * 1000)
        if ms <= self._last_ms:
            ms = self._last_ms
            self._counter += 1
        else:
            self._last_ms = ms
            self._counter = 0
        return f"{ms}-{self._counter}"


@dataclass
class StreamNormalizer:
    """Maps MIT astream items to wire events for one run."""

    requested_modes: list[str]
    _message_meta_sent: set[str] = field(default_factory=set)

    def wants(self, mode: str) -> bool:
        return mode in self.requested_modes

    def normalize(self, ns: tuple[str, ...], mode: str, payload: Any) -> list[WireEvent]:
        events: list[WireEvent] = []
        if mode == "values":
            if self.wants("values"):
                events.append(WireEvent("values", _dump(payload)))
        elif mode == "updates":
            if self.wants("updates"):
                events.append(WireEvent("updates", _dump(payload)))
            if self.wants("tools"):
                events.extend(self._tool_events(payload))
        elif mode == "messages":
            events.extend(self._message_events(payload))
        elif mode == "checkpoints":
            if self.wants("checkpoints"):
                events.append(WireEvent("checkpoints", _dump(payload)))
        elif mode == "custom":
            if self.wants("custom"):
                events.append(WireEvent("custom", _dump(payload)))
        elif mode == "tasks":
            if self.wants("tasks"):
                events.append(WireEvent("tasks", _dump(payload)))
        return events

    def _tool_events(self, payload: Any) -> list[WireEvent]:
        """Synthesize `tools` events from tool messages inside node updates."""
        events: list[WireEvent] = []
        if not isinstance(payload, dict):
            return events
        for node, update in payload.items():
            messages = (update or {}).get("messages") if isinstance(update, dict) else None
            if not isinstance(messages, list):
                continue
            tool_msgs = [
                _dump(m)
                for m in messages
                if getattr(m, "type", None) == "tool"
                or (isinstance(m, dict) and m.get("type") == "tool")
            ]
            if tool_msgs:
                events.append(WireEvent("tools", {"node": node, "messages": tool_msgs}))
        return events

    def _message_events(self, payload: Any) -> list[WireEvent]:
        """MIT `messages` yields (chunk, metadata); serve both platform modes.

        Platform `messages` family: metadata once per message id, `partial`
        for streamed chunks, `complete` for a whole message (non-chunk) —
        exactly what run_stream_mode_messages.json pins for a one-shot model.
        """
        from langchain_core.messages import BaseMessageChunk

        events: list[WireEvent] = []
        if not (isinstance(payload, (list, tuple)) and len(payload) == 2):
            return events
        message, metadata = payload
        if self.wants("messages-tuple"):
            events.append(WireEvent("messages", [_dump(message), _dump(metadata)]))
        if self.wants("messages"):
            message_id = getattr(message, "id", None) or ""
            if message_id and message_id not in self._message_meta_sent:
                self._message_meta_sent.add(message_id)
                events.append(
                    WireEvent("messages/metadata", {message_id: {"metadata": _dump(metadata)}})
                )
            kind = (
                "messages/partial" if isinstance(message, BaseMessageChunk) else "messages/complete"
            )
            events.append(WireEvent(kind, [_dump(message)]))
        return events


def lifecycle_event(run_id: str, name: str, graph_name: str) -> WireEvent:
    """v2 lifecycle synthetic; event_id format pinned by the golden
    (`synth:<run_id>:lc||<name>`)."""
    return WireEvent(
        "lifecycle",
        {"event": name, "graph_name": graph_name},
        event_id=f"synth:{run_id}:lc||{name}",
    )


def compact_messages(obj: Any) -> Any:
    """Compact message rendering for v2 payloads: dev's v2 stream drops
    null/empty message fields ({content, id, type} for a plain human message
    — stream_events_transcript.json)."""
    if isinstance(obj, dict):
        return {
            k: compact_messages(v) for k, v in obj.items() if not (v is None or v == {} or v == [])
        }
    if isinstance(obj, list):
        return [compact_messages(v) for v in obj]
    return obj


def to_v2_envelope(
    event: WireEvent, *, namespace: list[str] | None = None
) -> dict[str, Any] | None:
    """The /stream/events body shape (golden: stream_events_transcript.json)."""
    channel = V2_CHANNEL_FOR_EVENT.get(event.event)
    if channel is None or event.seq <= 0:
        return None
    return {
        "type": "event",
        "event_id": event.event_id,
        "seq": event.seq,
        "method": channel,
        "params": {
            "namespace": namespace or [],
            "timestamp": int(time.time() * 1000),
            "data": compact_messages(event.data),
        },
    }
