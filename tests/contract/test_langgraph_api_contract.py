"""Contract tests: pin ``langgraph dev``'s observable behavior as the golden
baseline for the Phase 1 ``agent_runtime`` server.

Each test exercises an operation the app calls today (MIGRATION.md §1
inventory; docs/fast-api-migration/phase-0.md task 7) through the same
transport the app uses — the ``langgraph_sdk`` client or raw ``httpx`` for the
wire endpoints the dashboard proxies verbatim. Divergences dev exhibits are
recorded as skips-with-reason (the divergence ledger in golden/README.md), not
papered over.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import httpx
import pytest

from tests.contract.conftest import ContractServer
from tests.contract.normalize import (
    assert_event_ids_monotonic,
    assert_matches_golden,
    parse_sse,
)

# The dashboard's run-level stream modes (agent/dashboard/thread_api.py:56-64).
DASHBOARD_STREAM_MODES = [
    "values",
    "updates",
    "messages",
    "messages-tuple",
    "tools",
    "checkpoints",
    "events",
]
# The v2 event-stream channels @langchain/react subscribes to
# (langgraph_api/api/event_streaming.py accepts: values, updates, messages,
# tools, custom, lifecycle, input, tasks).
V2_STREAM_CHANNELS = ["values", "updates", "messages", "tools", "lifecycle"]


def _tid() -> str:
    return str(uuid.uuid4())


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


async def _run_to_completion(
    client: Any, thread_id: str, text: str, **create_kwargs: Any
) -> dict[str, Any]:
    run = await client.runs.create(
        thread_id,
        "agent",
        input={"messages": [{"role": "user", "content": text}]},
        if_not_exists="create",
        **create_kwargs,
    )
    return await client.runs.join(thread_id, run["run_id"])


# ---------------------------------------------------------------------------
# Threads
# ---------------------------------------------------------------------------


async def test_thread_create_get_update_delete(contract_client: Any) -> None:
    thread_id = _tid()
    created = await contract_client.threads.create(
        thread_id=thread_id, metadata={"source": "contract", "owner": "tester"}
    )
    assert created["thread_id"] == thread_id
    assert_matches_golden("thread_create.json", created)

    await contract_client.threads.update(thread_id=thread_id, metadata={"stage": "updated"})
    fetched = await contract_client.threads.get(thread_id)
    # Metadata updates MERGE (webhook retrigger paths depend on this).
    assert fetched["metadata"]["source"] == "contract"
    assert fetched["metadata"]["stage"] == "updated"
    assert_matches_golden("thread_get_after_update.json", fetched)

    await contract_client.threads.delete(thread_id)
    with pytest.raises(Exception, match="not found|404"):
        await contract_client.threads.get(thread_id)


async def test_thread_create_if_exists_do_nothing(contract_client: Any) -> None:
    """Idempotent create is load-bearing at 9 call sites (phase-0.md task 7.1):
    every webhook retrigger breaks if the second create 409s."""
    thread_id = _tid()
    first = await contract_client.threads.create(
        thread_id=thread_id, metadata={"origin": "first-create"}
    )
    second = await contract_client.threads.create(
        thread_id=thread_id,
        metadata={"origin": "second-create"},
        if_exists="do_nothing",
    )
    assert second["thread_id"] == thread_id
    # Pin what dev does to metadata on the no-op create (Phase 1 must match).
    fetched = await contract_client.threads.get(thread_id)
    assert fetched["metadata"]["origin"] == first["metadata"]["origin"]
    assert_matches_golden("thread_create_if_exists_do_nothing.json", fetched)


async def test_thread_search_metadata_filters_and_pagination(contract_client: Any) -> None:
    batch = f"batch-{_tid()[:8]}"
    ids = []
    for index in range(3):
        thread_id = _tid()
        ids.append(thread_id)
        await contract_client.threads.create(
            thread_id=thread_id,
            metadata={
                "contract_batch": batch,
                "index": index,
                "source_context": {"kind": "slack", "channel": f"C{index}"},
            },
        )

    by_top_level = await contract_client.threads.search(
        metadata={"contract_batch": batch}, limit=10
    )
    assert {t["thread_id"] for t in by_top_level} == set(ids)

    # Nested-dict metadata filter (schedules.py/profiles.py rely on the
    # analogous store behavior; pin what threads.search does with it).
    by_nested = await contract_client.threads.search(
        metadata={"contract_batch": batch, "source_context": {"kind": "slack", "channel": "C1"}},
        limit=10,
    )
    assert [t["metadata"]["index"] for t in by_nested] == [1]

    page_one = await contract_client.threads.search(
        metadata={"contract_batch": batch}, limit=2, offset=0
    )
    page_two = await contract_client.threads.search(
        metadata={"contract_batch": batch}, limit=2, offset=2
    )
    assert len(page_one) == 2
    assert len(page_two) == 1
    assert {t["thread_id"] for t in page_one} | {t["thread_id"] for t in page_two} == set(ids)


async def test_thread_search_sort_by_updated_at_desc(contract_client: Any) -> None:
    """thread_api.py:545-551 lists dashboard threads sort_by=updated_at desc."""
    batch = f"sort-{_tid()[:8]}"
    ids = [_tid() for _ in range(3)]
    for thread_id in ids:
        await contract_client.threads.create(
            thread_id=thread_id, metadata={"contract_batch": batch}
        )
    # Touch the FIRST-created thread last: updated_at order must now differ
    # from created_at order.
    await contract_client.threads.update(thread_id=ids[0], metadata={"touched": True})

    results = await contract_client.threads.search(
        metadata={"contract_batch": batch},
        sort_by="updated_at",
        sort_order="desc",
        limit=10,
    )
    assert results[0]["thread_id"] == ids[0]
    # Chronological, not lexicographic: the Phase 1 serializer may format
    # timestamps differently and string order would silently diverge.
    updated = [_parse_ts(t["updated_at"]) for t in results]
    assert updated == sorted(updated, reverse=True)


async def test_thread_search_sort_by_created_at(contract_client: Any) -> None:
    """agent_usage.py:494-500 pages threads sort_by=created_at."""
    batch = f"created-{_tid()[:8]}"
    ids = [_tid() for _ in range(3)]
    for thread_id in ids:
        await contract_client.threads.create(
            thread_id=thread_id, metadata={"contract_batch": batch}
        )
        # Distinct created_at values so exact-order assertions can't tie.
        await asyncio.sleep(0.02)

    ascending = await contract_client.threads.search(
        metadata={"contract_batch": batch},
        sort_by="created_at",
        sort_order="asc",
        limit=10,
    )
    assert [t["thread_id"] for t in ascending] == ids

    descending = await contract_client.threads.search(
        metadata={"contract_batch": batch},
        sort_by="created_at",
        sort_order="desc",
        limit=10,
    )
    assert [t["thread_id"] for t in descending] == list(reversed(ids))


async def test_thread_search_select_subset(contract_client: Any) -> None:
    """thread_api.py:506 passes select=_THREAD_LIST_SELECT to skip values."""
    # Fixed token: this result is golden-recorded, and the server is fresh
    # per session, so the value can't collide.
    batch = "select-subset-batch"
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id, metadata={"contract_batch": batch})

    results = await contract_client.threads.search(
        metadata={"contract_batch": batch},
        select=["thread_id", "status", "metadata"],
        limit=10,
    )
    assert len(results) == 1
    assert_matches_golden("thread_search_select_subset.json", results[0])


async def test_thread_search_status_busy(contract_client: Any) -> None:
    """agent/reconcile.py sweeps threads.search(status='busy')."""
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id, metadata={"busy_probe": thread_id})
    run = await contract_client.runs.create(
        thread_id,
        "agent",
        input={"messages": [{"role": "user", "content": "hold"}], "busy_seconds": 3},
    )
    try:
        busy: list[dict[str, Any]] = []
        for _ in range(20):
            busy = await contract_client.threads.search(
                metadata={"busy_probe": thread_id}, status="busy", limit=5
            )
            if busy:
                break
            await asyncio.sleep(0.25)
        assert [t["thread_id"] for t in busy] == [thread_id]
        assert busy[0]["status"] == "busy"
    finally:
        await contract_client.runs.join(thread_id, run["run_id"])

    idle_again = await contract_client.threads.search(
        metadata={"busy_probe": thread_id}, status="busy", limit=5
    )
    assert idle_again == []


# ---------------------------------------------------------------------------
# State + history (SDK get_state and the raw wire the dashboard proxies)
# ---------------------------------------------------------------------------


async def test_thread_state_and_history(
    contract_client: Any, contract_server: ContractServer
) -> None:
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id)
    await _run_to_completion(contract_client, thread_id, "state probe")

    state = await contract_client.threads.get_state(thread_id)
    assert_matches_golden("thread_get_state.json", state)

    async with httpx.AsyncClient(base_url=contract_server.base_url, timeout=30.0) as http:
        # thread_api.py:1961 forwards POST /threads/{id}/history verbatim.
        history = await http.post(f"/threads/{thread_id}/history", json={"limit": 10})
        assert history.status_code == 200
        assert_matches_golden("thread_history_wire.json", history.json())

        # review_chat_api.py:510 proxies GET /threads/{id}/state verbatim.
        raw_state = await http.get(f"/threads/{thread_id}/state")
        assert raw_state.status_code == 200
        assert_matches_golden("thread_state_wire.json", raw_state.json())


# ---------------------------------------------------------------------------
# Runs — durable-dispatch semantics (agent/dispatch.py:create_durable_run)
# ---------------------------------------------------------------------------


class _WebhookReceiver(BaseHTTPRequestHandler):
    received: list[dict[str, Any]] = []

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"raw": body.decode(errors="replace")}
        type(self).received.append({"path": self.path, "payload": payload})
        self.send_response(200)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass


@pytest.fixture()
def webhook_receiver() -> Any:
    _WebhookReceiver.received = []
    server = HTTPServer(("127.0.0.1", 0), _WebhookReceiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)


async def test_run_create_durable_semantics_and_completion_webhook(
    contract_client: Any, webhook_receiver: Any
) -> None:
    """The exact kwargs create_durable_run sends (dispatch.py): multitask
    interrupt + if_not_exists create + durability sync + stream_resumable +
    completion webhook (?token= appended by open-swe, not platform signing)."""
    thread_id = _tid()
    port = webhook_receiver.server_address[1]
    webhook_url = f"http://127.0.0.1:{port}/webhooks/run-complete?token=contract-secret"

    run = await contract_client.runs.create(
        thread_id,
        "agent",
        input={"messages": [{"role": "user", "content": "webhook probe"}]},
        if_not_exists="create",
        multitask_strategy="interrupt",
        stream_resumable=True,
        durability="sync",
        webhook=webhook_url,
    )
    assert_matches_golden("run_create.json", run)

    joined = await contract_client.runs.join(thread_id, run["run_id"])
    assert joined["messages"][-1]["content"] == "echo: webhook probe"

    final = await contract_client.runs.get(thread_id, run["run_id"])
    assert final["status"] == "success"

    for _ in range(40):
        if _WebhookReceiver.received:
            break
        await asyncio.sleep(0.25)
    if not _WebhookReceiver.received:
        pytest.skip(
            "divergence ledger: langgraph dev did not deliver the completion "
            "webhook to a loopback URL (platform docs say loopback is rejected; "
            "record and match actual Phase 1 behavior instead)"
        )
    delivery = _WebhookReceiver.received[0]
    assert delivery["path"].endswith("?token=contract-secret")
    assert_matches_golden("run_complete_webhook_payload.json", delivery["payload"])


async def test_double_dispatch_interrupt_on_busy_thread(contract_client: Any) -> None:
    """The slack-debounce crown jewel: a second run on a busy thread with
    multitask_strategy=interrupt supersedes the first (MIGRATION §1)."""
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id)

    busy_seconds = 8
    first = await contract_client.runs.create(
        thread_id,
        "agent",
        input={
            "messages": [{"role": "user", "content": "long task"}],
            "busy_seconds": busy_seconds,
        },
        multitask_strategy="interrupt",
    )
    observed_running = False
    for _ in range(40):
        current = await contract_client.runs.get(thread_id, first["run_id"])
        if current["status"] == "running":
            observed_running = True
            break
        await asyncio.sleep(0.25)
    assert observed_running, "first run never reached 'running'; test would not exercise preemption"

    second = await contract_client.runs.create(
        thread_id,
        "agent",
        input={"messages": [{"role": "user", "content": "supersede"}]},
        multitask_strategy="interrupt",
    )
    join_started = time.monotonic()
    result = await contract_client.runs.join(thread_id, second["run_id"])
    join_elapsed = time.monotonic() - join_started
    assert result["messages"][-1]["content"] == "echo: supersede"
    # Measured dev semantics (stable across runs): "interrupt" cancels at the
    # next STEP BOUNDARY — the in-flight busy tool finishes its ~8s step, THEN
    # the first run halts, so the second join waits ≈ the remaining window.
    # It is NOT a preemptive mid-tool kill, and not full serialization either:
    # full serialization would let the first run emit its final echo and end
    # "success" (caught by the status golden below). Bound only the
    # pathological case of both runs executing end-to-end sequentially.
    assert join_elapsed < busy_seconds + 5, (
        f"second run joined in {join_elapsed:.1f}s — even step-boundary "
        "cancellation should not take longer than one busy window"
    )

    first_final = await contract_client.runs.get(thread_id, first["run_id"])
    second_final = await contract_client.runs.get(thread_id, second["run_id"])
    assert second_final["status"] == "success"
    # The superseded run must never have produced ITS final answer: the
    # thread's message history ends with the second run's echo and contains
    # no "echo: long task" from the first.
    state = await contract_client.threads.get_state(thread_id)
    contents = [m.get("content") for m in state["values"]["messages"] if isinstance(m, dict)]
    assert "echo: supersede" in contents
    assert "echo: long task" not in contents, (
        "first run completed despite interrupt — serialization, not cancellation"
    )
    # Pin the exact terminal status dev gives the superseded run —
    # completion.py treats "interrupted" as healthy, so Phase 1 must match it.
    assert_matches_golden(
        "run_double_dispatch_statuses.json",
        {"first": first_final["status"], "second": second_final["status"]},
    )


async def test_runs_list_cancel_and_cancel_many(contract_client: Any) -> None:
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id)

    run = await contract_client.runs.create(
        thread_id,
        "agent",
        input={"messages": [{"role": "user", "content": "cancel me"}], "busy_seconds": 20},
    )
    listed = await contract_client.runs.list(thread_id)
    assert [r["run_id"] for r in listed] == [run["run_id"]]
    # reconcile.py's sweep depends on status="pending" actually filtering:
    # the queued/just-started run must appear under pending|running and must
    # NOT appear under a terminal-status filter.
    listed_active = [
        r["run_id"]
        for status in ("pending", "running")
        for r in await contract_client.runs.list(thread_id, status=status)
    ]
    assert run["run_id"] in listed_active
    listed_error = await contract_client.runs.list(thread_id, status="error")
    assert run["run_id"] not in [r["run_id"] for r in listed_error]

    # reconcile.py cancels via cancel_many(action="interrupt").
    cancelled_ids = await contract_client.runs.cancel_many(
        thread_id=thread_id, run_ids=[run["run_id"]], action="interrupt"
    )
    assert_matches_golden("runs_cancel_many_response.json", cancelled_ids)

    final = await contract_client.runs.get(thread_id, run["run_id"])
    for _ in range(40):
        if final["status"] not in {"pending", "running"}:
            break
        await asyncio.sleep(0.25)
        final = await contract_client.runs.get(thread_id, run["run_id"])
    assert_matches_golden("run_status_after_cancel.json", {"status": final["status"]})


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


async def test_store_put_get_search_delete(contract_client: Any) -> None:
    # Fixed namespace: get_item's result is golden-recorded (fresh server per
    # session, no collision risk).
    namespace = ("contract", "store-crud")
    await contract_client.store.put_item(
        namespace,
        "profile-1",
        {"login": "alice", "prefs": {"model": "sonnet", "notify": True}},
    )
    await contract_client.store.put_item(
        namespace,
        "profile-2",
        {"login": "bob", "prefs": {"model": "opus", "notify": False}},
    )

    item = await contract_client.store.get_item(namespace, "profile-1")
    assert item["value"]["login"] == "alice"
    assert_matches_golden("store_get_item.json", item)

    everything = await contract_client.store.search_items(namespace, limit=10)
    assert len(everything["items"]) == 2

    # The filter form the app actually uses is flat top-level keys
    # (e.g. {"created_by": login}, schedules.py) — asserted unconditionally.
    top_level = await contract_client.store.search_items(
        namespace, filter={"login": "bob"}, limit=10
    )
    assert [i["key"] for i in top_level["items"]] == ["profile-2"]

    # Dotted nested filters are a recorded divergence probe only.
    filtered = await contract_client.store.search_items(
        namespace, filter={"prefs.model": "opus"}, limit=10
    )
    dotted_supported = [i["key"] for i in filtered["items"]] == ["profile-2"]

    await contract_client.store.delete_item(namespace, "profile-1")
    # Unlike threads.get (404 after delete), store.get_item on a missing
    # item returns None — Phase 1 must preserve this asymmetry.
    assert await contract_client.store.get_item(namespace, "profile-1") is None

    if not dotted_supported:
        pytest.skip(
            "divergence ledger: dev's inmem store does not match dotted nested "
            "filters (top-level filters — the form the app uses — are asserted "
            "above and pass)."
        )


# ---------------------------------------------------------------------------
# Crons
# ---------------------------------------------------------------------------


async def test_crons_surface(contract_client: Any) -> None:
    """analyzer_cron.py / schedules.py create crons; record dev's support."""
    try:
        cron = await contract_client.crons.create(
            "agent",
            schedule="0 4 * * *",
            input={"messages": [{"role": "user", "content": "nightly"}]},
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"divergence ledger: langgraph dev (inmem) does not implement crons "
            f"({type(exc).__name__}: {exc}); Phase 1 implements them from the "
            "SDK surface the app uses (crons.create/create_for_thread/search/delete)"
        )
    searched = await contract_client.crons.search()
    assert any(c["cron_id"] == cron["cron_id"] for c in searched)
    await contract_client.crons.delete(cron["cron_id"])
    searched_after = await contract_client.crons.search()
    assert all(c["cron_id"] != cron["cron_id"] for c in searched_after)


async def test_crons_create_for_thread(contract_client: Any) -> None:
    """schedule_thread_wakeup.py:129 uses crons.create_for_thread with
    end_time + timezone — the exact call Phase 1 must serve."""
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id)
    try:
        cron = await contract_client.crons.create_for_thread(
            thread_id,
            "agent",
            schedule="30 7 * * *",
            input={"messages": [{"role": "user", "content": "wakeup"}]},
            end_time=datetime.now(UTC) + timedelta(days=1),
            timezone="America/Los_Angeles",
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"divergence ledger: langgraph dev (inmem) rejected "
            f"crons.create_for_thread(end_time=…, timezone=…) "
            f"({type(exc).__name__}: {exc}); Phase 1 implements it regardless "
            "(agent/tools/schedule_thread_wakeup.py depends on it)"
        )
    assert cron["thread_id"] == thread_id
    searched = await contract_client.crons.search(thread_id=thread_id)
    assert [c["cron_id"] for c in searched] == [cron["cron_id"]]
    await contract_client.crons.delete(cron["cron_id"])


# ---------------------------------------------------------------------------
# The v2 wire: /commands + /stream/events (dashboard proxies these verbatim)
# ---------------------------------------------------------------------------


async def test_commands_run_start_wire(
    contract_client: Any, contract_server: ContractServer
) -> None:
    """POST /threads/{id}/commands with the enriched run.start body the
    dashboard sends (thread_api.py:1284-1289)."""
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id)

    command = {
        "id": 1,
        "method": "run.start",
        "params": {
            "assistant_id": "agent",
            "input": {"messages": [{"role": "user", "content": "command probe"}]},
            "stream_mode": DASHBOARD_STREAM_MODES,
            "stream_resumable": True,
            "config": {"configurable": {}},
            "metadata": {"source": "contract"},
        },
    }
    async with httpx.AsyncClient(base_url=contract_server.base_url, timeout=30.0) as http:
        response = await http.post(f"/threads/{thread_id}/commands", json=command)
    assert response.status_code == 200
    payload = response.json()
    assert_matches_golden("commands_run_start_response.json", payload)

    run_id = (payload.get("result") or {}).get("run_id") or payload.get("run_id")
    assert run_id, f"no run_id in command response: {payload}"
    # Divergence datum: for a /commands-started run, runs.join returns a v2
    # protocol event envelope (type/method/params/seq), NOT the final values
    # dict it returns for SDK-created runs. Pin it — the dashboard never joins
    # these runs, but Phase 1 should not accidentally "fix" the shape.
    joined = await contract_client.runs.join(thread_id, run_id)
    assert set(joined) >= {"type", "method", "params"}, joined

    state = await contract_client.threads.get_state(thread_id)
    assert state["values"]["messages"][-1]["content"] == "echo: command probe"


async def _collect_stream_events(
    http: httpx.AsyncClient,
    thread_id: str,
    body: dict[str, Any],
    *,
    stop_when: Any,
    timeout_s: float = 30.0,
    connected: asyncio.Event | None = None,
) -> list[dict[str, Any]]:
    """Consume /stream/events until ``stop_when(events)`` is true (live tail:
    the connection never closes on its own, so the caller decides when done).

    ``connected`` is set once the response headers arrive — by then the
    server has installed the subscription, so a run started after this event
    cannot race past the tail (adversarial-review finding 9).
    """
    collected = bytearray()

    async def consume() -> None:
        async with http.stream(
            "POST",
            f"/threads/{thread_id}/stream/events",
            json=body,
            headers={"Accept": "text/event-stream"},
        ) as response:
            assert response.status_code == 200
            if connected is not None:
                connected.set()
            async for chunk in response.aiter_bytes():
                collected.extend(chunk)
                if stop_when(parse_sse(bytes(collected))):
                    return

    await asyncio.wait_for(consume(), timeout=timeout_s)
    return parse_sse(bytes(collected))


def _is_lifecycle(event: dict[str, Any], name: str) -> bool:
    data = event.get("data")
    if not isinstance(data, dict):
        return False
    params = data.get("params") or {}
    inner = params.get("data") or {}
    return data.get("method") == "lifecycle" and inner.get("event") == name


async def test_stream_events_wire_transcript(
    contract_client: Any, contract_server: ContractServer
) -> None:
    """Golden SSE transcript of POST /threads/{id}/stream/events while a run
    started via /commands executes — the exact wire surface the dashboard
    proxies to @langchain/react (thread_api.py:1835). This endpoint is a LIVE
    tail: connect first, then start the run, stop at lifecycle=completed."""
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id)

    async with httpx.AsyncClient(base_url=contract_server.base_url, timeout=60.0) as http:
        connected = asyncio.Event()

        async def start_run() -> None:
            await connected.wait()
            command = {
                "id": 1,
                "method": "run.start",
                "params": {
                    "assistant_id": "agent",
                    "input": {"messages": [{"role": "user", "content": "stream probe"}]},
                    "stream_mode": DASHBOARD_STREAM_MODES,
                    "stream_resumable": True,
                },
            }
            start = await http.post(f"/threads/{thread_id}/commands", json=command)
            assert start.status_code == 200

        starter = asyncio.create_task(start_run())
        events = await _collect_stream_events(
            http,
            thread_id,
            {"channels": V2_STREAM_CHANNELS},
            stop_when=lambda evs: any(_is_lifecycle(e, "completed") for e in evs),
            connected=connected,
        )
        await starter

    seqs = [e["id"] for e in events if e["id"] is not None]
    assert_event_ids_monotonic(seqs)
    assert_matches_golden("stream_events_transcript.json", events)


async def test_stream_events_resume_with_since(
    contract_client: Any, contract_server: ContractServer
) -> None:
    """Reconnect semantics: a client that reconnects mid-run passing the last
    seq it saw as ``since`` gets exactly the events after it — no loss, no
    duplication (the resumable-SSE contract Phase 1's run-event log must
    honor). Uses the contract graph's deterministic busy window so the
    disconnect happens while the run is still producing events."""
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id)

    async with httpx.AsyncClient(base_url=contract_server.base_url, timeout=60.0) as http:
        connected = asyncio.Event()

        async def start_busy_run() -> None:
            await connected.wait()
            run = await contract_client.runs.create(
                thread_id,
                "agent",
                input={
                    "messages": [{"role": "user", "content": "resume probe"}],
                    "busy_seconds": 4,
                },
                stream_resumable=True,
            )
            start_busy_run.run_id = run["run_id"]  # type: ignore[attr-defined]

        starter = asyncio.create_task(start_busy_run())
        # Connection 1: read the first two events, then drop the connection
        # mid-run (the busy window guarantees the run is still going).
        head = await _collect_stream_events(
            http,
            thread_id,
            {"channels": V2_STREAM_CHANNELS},
            stop_when=lambda evs: len(evs) >= 2,
            connected=connected,
        )
        await starter
        cut = head[-1]["id"]
        assert cut is not None

        # Connection 2: resume with since=<last seq seen>.
        tail = await _collect_stream_events(
            http,
            thread_id,
            {"channels": V2_STREAM_CHANNELS, "since": int(cut)},
            stop_when=lambda evs: any(_is_lifecycle(e, "completed") for e in evs),
        )
        await contract_client.runs.join(thread_id, start_busy_run.run_id)  # type: ignore[attr-defined]

    head_seqs = [int(e["id"]) for e in head if e["id"] is not None]
    tail_seqs = [int(e["id"]) for e in tail if e["id"] is not None]
    assert all(seq > int(cut) for seq in tail_seqs), (head_seqs, tail_seqs)
    # Contiguity is the loss check: seqs are dense per thread, so a gap in
    # head+tail means an event was dropped across the reconnect, and a repeat
    # means duplication. (head starts at seq 1 on a fresh thread.)
    combined = head_seqs + tail_seqs
    assert combined == list(range(combined[0], combined[0] + len(combined))), (
        f"loss or duplication across resume: {combined}"
    )


async def test_threads_join_stream_live_tail(contract_client: Any) -> None:
    """threads.join_stream is the dashboard's live tail (thread_api.py:2016).
    Tail a busy run to completion and pin the final values event."""
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id)
    run = await contract_client.runs.create(
        thread_id,
        "agent",
        input={"messages": [{"role": "user", "content": "join probe"}], "busy_seconds": 2},
        stream_resumable=True,
    )

    events: list[dict[str, Any]] = []

    async def tail() -> None:
        stream = await contract_client.threads.join_stream(thread_id)
        async for part in stream:
            events.append({"event": part.event, "data": part.data})
            if part.event == "values" and isinstance(part.data, dict):
                messages = part.data.get("messages") or []
                if messages and messages[-1].get("content") == "echo: join probe":
                    return

    await asyncio.wait_for(tail(), timeout=30)
    await contract_client.runs.join(thread_id, run["run_id"])
    values_events = [e for e in events if e["event"] == "values"]
    assert values_events, f"no values events on the live tail: {[e['event'] for e in events]}"


async def test_join_stream_after_completion_replays_nothing(contract_client: Any) -> None:
    """Divergence datum: dev's inmem runtime does NOT replay a finished run's
    events on runs.join_stream — the stream ends immediately. The dashboard
    only live-tails, so Phase 1 must match the live path; record this so the
    non-replay is a known baseline, not a surprise."""
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id)
    run = await contract_client.runs.create(
        thread_id,
        "agent",
        input={"messages": [{"role": "user", "content": "replay probe"}]},
        stream_resumable=True,
    )
    await contract_client.runs.join(thread_id, run["run_id"])

    events = []
    async with asyncio.timeout(15):
        async for part in contract_client.runs.join_stream(
            thread_id, run["run_id"], stream_mode="values"
        ):
            events.append(part.event)
    assert_matches_golden("join_stream_after_completion.json", {"events": events})


async def test_threads_join_stream_reconnect_with_last_event_id(contract_client: Any) -> None:
    """The dashboard's actual resume path: threads.join_stream(thread_id,
    last_event_id=<id>) after a mid-run disconnect (thread_api.py:2016-2018).
    Nothing may be lost or duplicated across the reconnect."""
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id)
    run = await contract_client.runs.create(
        thread_id,
        "agent",
        input={"messages": [{"role": "user", "content": "le probe"}], "busy_seconds": 4},
        stream_resumable=True,
    )

    head: list[Any] = []

    async def read_head() -> None:
        stream = await contract_client.threads.join_stream(thread_id)
        async for part in stream:
            head.append(part)
            if len(head) >= 2 and head[-1].id is not None:
                return  # drop the connection mid-run

    await asyncio.wait_for(read_head(), timeout=30)
    last_seen = next(p.id for p in reversed(head) if p.id is not None)

    tail: list[Any] = []

    async def read_tail() -> None:
        stream = await contract_client.threads.join_stream(thread_id, last_event_id=last_seen)
        async for part in stream:
            tail.append(part)
            if part.event == "values" and isinstance(part.data, dict):
                messages = part.data.get("messages") or []
                if messages and messages[-1].get("content") == "echo: le probe":
                    return

    await asyncio.wait_for(read_tail(), timeout=30)
    await contract_client.runs.join(thread_id, run["run_id"])

    head_ids = [p.id for p in head if p.id is not None]
    tail_ids = [p.id for p in tail if p.id is not None]
    assert last_seen not in tail_ids, f"duplicated event across reconnect: {last_seen}"
    assert not set(head_ids) & set(tail_ids), (head_ids, tail_ids)
    assert any(p.event == "values" for p in tail), "no values events after reconnect"


# ---------------------------------------------------------------------------
# Per-mode run streams — the StreamModes Phase 1 must map itself
# (messages-tuple / tools / checkpoints / events are NOT MIT StreamModes;
# MIGRATION §1). One golden per dashboard mode.
# ---------------------------------------------------------------------------


def _shape_for_mode(mode: str, part: Any) -> dict[str, Any]:
    if mode == "events":
        # astream_events firehose: pin the protocol envelope (event name +
        # inner event type), not the full payload — it embeds run internals
        # whose exact contents are langchain-version-coupled, not wire contract.
        inner = part.data.get("event") if isinstance(part.data, dict) else None
        return {"event": part.event, "inner_event": inner}
    return {"event": part.event, "data": part.data}


@pytest.mark.parametrize("mode", DASHBOARD_STREAM_MODES)
async def test_run_stream_mode_golden(contract_client: Any, mode: str) -> None:
    import os

    if mode == "events" and os.environ.get("CONTRACT_RUNTIME") == "embedded":
        pytest.skip(
            "divergence ledger: run-level stream_mode='events' (astream_events "
            "firehose) has no app caller (D6) and is not wired in agent_runtime; "
            "the mode is accepted-and-ignored there."
        )
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id)
    parts = []
    try:
        async for part in contract_client.runs.stream(
            thread_id,
            "agent",
            input={"messages": [{"role": "user", "content": f"mode {mode}"}]},
            stream_mode=mode,
        ):
            parts.append(_shape_for_mode(mode, part))
    except Exception as exc:
        # "tools" is a v2 event-stream channel, NOT a run-level StreamMode —
        # dev's schema validation 422s it. Pin the rejection: Phase 1's run
        # endpoints must reject it identically (it only works inside run.start
        # stream_mode lists on the /commands path, where it is ignored).
        assert_matches_golden(
            f"run_stream_mode_{mode}.json",
            {"rejected": type(exc).__name__},
        )
        return
    assert parts, f"stream_mode={mode} produced no events"
    assert_matches_golden(f"run_stream_mode_{mode}.json", parts)


async def test_run_cancel_single_and_raw_route(
    contract_client: Any, contract_server: ContractServer
) -> None:
    """Single-run cancel via the raw wire the dashboard proxies
    (thread_api.py:1979: POST /threads/{id}/runs/{run_id}/cancel?wait=0&action=interrupt)."""
    thread_id = _tid()
    await contract_client.threads.create(thread_id=thread_id)
    run = await contract_client.runs.create(
        thread_id,
        "agent",
        input={"messages": [{"role": "user", "content": "raw cancel"}], "busy_seconds": 20},
    )
    for _ in range(40):
        current = await contract_client.runs.get(thread_id, run["run_id"])
        if current["status"] == "running":
            break
        await asyncio.sleep(0.25)

    async with httpx.AsyncClient(base_url=contract_server.base_url, timeout=30.0) as http:
        response = await http.post(
            f"/threads/{thread_id}/runs/{run['run_id']}/cancel",
            params={"wait": "0", "action": "interrupt"},
        )
    assert_matches_golden(
        "run_cancel_raw_route.json",
        {"status_code": response.status_code},
    )

    final = await contract_client.runs.get(thread_id, run["run_id"])
    for _ in range(40):
        if final["status"] not in {"pending", "running"}:
            break
        await asyncio.sleep(0.25)
        final = await contract_client.runs.get(thread_id, run["run_id"])
    assert final["status"] == "interrupted"
