"""Normalization + golden-comparison helpers for the contract suite.

``Normalizer`` replaces every unstable identifier in a payload with a stable
placeholder so a ``langgraph dev`` transcript can be committed as a golden and
diffed on replay (docs/fast-api-migration/phase-0.md §5 risk 1):

* UUIDs (thread/run/checkpoint/assistant/message ids) → ``<uuid-N>`` in order
  of first appearance — identity relationships survive normalization (the same
  UUID always maps to the same placeholder within one payload).
* Stream event ids (inmem runtime's Redis-style ``<ms>-<seq>``) → ``<event-N>``.
* ISO-8601 timestamps → ``<ts>``.
* Loopback host:port (ephemeral test ports) → ``127.0.0.1:<port>``.

Event-id *monotonicity* is deliberately not part of the golden diff — assert it
separately with :func:`assert_event_ids_monotonic`.

Goldens live in ``tests/contract/golden/``. The first run records
(:func:`assert_matches_golden` writes the file); subsequent runs diff and fail
on any change.
"""

from __future__ import annotations

import difflib
import json
import os
import re
from pathlib import Path
from typing import Any

GOLDEN_DIR = Path(__file__).resolve().parent / "golden"

_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_EVENT_ID_RE = re.compile(r"\b\d{13}-\d+\b")
_ISO_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")
_LOOPBACK_RE = re.compile(r"(127\.0\.0\.1|localhost):\d{4,5}")

# Millisecond-epoch JSON numbers (e.g. the v2 protocol's ``timestamp`` field).
# Any 13-digit int in a plausible epoch window is a wall-clock reading.
_MS_EPOCH_MIN = 1_500_000_000_000  # 2017
_MS_EPOCH_MAX = 3_000_000_000_000  # 2065

# Environment/version metadata keys the server stamps into configs — they
# track dependency versions and license tier, not contract semantics, and
# they won't exist on the Phase 1 server at all.
_VERSIONED_ENV_KEYS = frozenset(
    {
        "langgraph_version",
        "langgraph_api_version",
        "langgraph_plan",
        "langgraph_host",
        "langgraph_api_url",
    }
)


class Normalizer:
    """Stateful placeholder mapper — one instance per golden payload."""

    def __init__(self) -> None:
        self._uuids: dict[str, str] = {}
        self._event_ids: dict[str, str] = {}

    def _uuid(self, match: re.Match[str]) -> str:
        raw = match.group(0).lower()
        if raw not in self._uuids:
            self._uuids[raw] = f"<uuid-{len(self._uuids) + 1}>"
        return self._uuids[raw]

    def _event_id(self, match: re.Match[str]) -> str:
        raw = match.group(0)
        if raw not in self._event_ids:
            self._event_ids[raw] = f"<event-{len(self._event_ids) + 1}>"
        return self._event_ids[raw]

    def normalize_str(self, value: str) -> str:
        value = _UUID_RE.sub(self._uuid, value)
        value = _EVENT_ID_RE.sub(self._event_id, value)
        value = _ISO_TS_RE.sub("<ts>", value)
        return _LOOPBACK_RE.sub(r"\1:<port>", value)

    def normalize(self, obj: Any) -> Any:
        if isinstance(obj, str):
            return self.normalize_str(obj)
        if isinstance(obj, dict):
            # Canonical key order: the server's JSON key order is not part of
            # the contract, and first-appearance UUID numbering must not
            # depend on it.
            return {
                self.normalize(k): ("<env>" if k in _VERSIONED_ENV_KEYS else self.normalize(v))
                for k, v in sorted(obj.items(), key=lambda kv: str(kv[0]))
            }
        if isinstance(obj, (list, tuple)):
            return [self.normalize(item) for item in obj]
        if (
            isinstance(obj, int)
            and not isinstance(obj, bool)
            and _MS_EPOCH_MIN <= obj <= _MS_EPOCH_MAX
        ):
            return "<ts-ms>"
        return obj


def normalize(obj: Any) -> Any:
    """One-shot normalization with a fresh placeholder map."""
    return Normalizer().normalize(obj)


def parse_sse(raw: bytes) -> list[dict[str, Any]]:
    """Parse an SSE byte stream into ``{"event", "id", "data"}`` dicts.

    Comment lines (``: heartbeat``) are dropped. ``data`` is JSON-decoded when
    possible, otherwise kept as text. Only ``\\n\\n``-terminated blocks are
    parsed — a trailing partial event (mid-delivery) is ignored so callers
    polling this during a live stream never act on a half-received event.
    """
    events: list[dict[str, Any]] = []
    text = raw.decode("utf-8", errors="replace").replace("\r\n", "\n")
    complete, separator, _partial = text.rpartition("\n\n")
    if not separator:
        return events
    for block in complete.split("\n\n"):
        event: str | None = None
        event_id: str | None = None
        data_lines: list[str] = []
        for line in block.split("\n"):
            if not line or line.startswith(":"):
                continue
            field, _, value = line.partition(":")
            value = value.removeprefix(" ")
            if field == "event":
                event = value
            elif field == "id":
                event_id = value
            elif field == "data":
                data_lines.append(value)
        if event is None and event_id is None and not data_lines:
            continue
        data: Any = "\n".join(data_lines)
        if data_lines:
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                pass
        events.append({"event": event, "id": event_id, "data": data})
    return events


def _event_id_key(event_id: str) -> tuple[int, ...]:
    if _EVENT_ID_RE.fullmatch(event_id):
        ms, _, seq = event_id.partition("-")
        return (int(ms), int(seq))
    if event_id.isdigit():
        return (int(event_id),)
    raise AssertionError(f"unrecognized event id format: {event_id!r}")


def assert_event_ids_monotonic(event_ids: list[str]) -> None:
    """Event ids must be non-decreasing in delivery order (asserted outside the golden diff)."""
    keys = [_event_id_key(eid) for eid in event_ids]
    for prev, cur in zip(keys, keys[1:], strict=False):
        assert cur >= prev, f"event ids regressed: {keys}"


def assert_matches_golden(name: str, payload: Any) -> None:
    """Record-then-diff golden comparison.

    Recording only happens with ``CONTRACT_RECORD=1`` in the environment
    (``CONTRACT_RECORD=1 make contract-test``); otherwise a missing golden is
    a hard failure — a lost/renamed golden must never silently convert the
    parity check into self-certification of the current server's behavior.
    """
    GOLDEN_DIR.mkdir(exist_ok=True)
    path = GOLDEN_DIR / name
    rendered = json.dumps(normalize(payload), indent=2, ensure_ascii=False) + "\n"
    if not path.exists():
        if os.environ.get("CONTRACT_RECORD") == "1":
            path.write_text(rendered, encoding="utf-8")
            return
        raise AssertionError(
            f"golden transcript {name} is missing. If this is intentional "
            "(new test or deliberate re-record), run: CONTRACT_RECORD=1 make contract-test"
        )
    existing = path.read_text(encoding="utf-8")
    if existing == rendered:
        return
    diff = "\n".join(
        difflib.unified_diff(
            existing.splitlines(),
            rendered.splitlines(),
            fromfile=f"golden/{name} (recorded)",
            tofile=f"golden/{name} (this run)",
            lineterm="",
        )
    )
    raise AssertionError(f"golden transcript {name} diverged:\n{diff}")
