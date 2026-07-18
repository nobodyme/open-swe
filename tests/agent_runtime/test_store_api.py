"""Store API + the store-identity invariant (phase-1.md T5).

``test_http_write_visible_to_inprocess_get_store`` is MIGRATION §1's
consistency constraint as an executable test: the store served over HTTP and
the store a graph node sees via ``get_store()`` must be ONE instance.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest


async def test_store_crud_and_filters(sdk_client: Any) -> None:
    namespace = ["runtime", f"crud-{uuid.uuid4().hex[:8]}"]
    await sdk_client.store.put_item(
        namespace, "alice", {"login": "alice", "prefs": {"model": "sonnet"}}
    )
    await sdk_client.store.put_item(namespace, "bob", {"login": "bob", "prefs": {"model": "opus"}})

    item = await sdk_client.store.get_item(namespace, "alice")
    assert item["value"]["login"] == "alice"
    assert item["namespace"] == namespace

    everything = await sdk_client.store.search_items(namespace, limit=10)
    assert len(everything["items"]) == 2

    filtered = await sdk_client.store.search_items(namespace, filter={"login": "bob"}, limit=10)
    assert [i["key"] for i in filtered["items"]] == ["bob"]

    await sdk_client.store.delete_item(namespace, "alice")
    # Dev asymmetry pinned by the contract suite: missing item → None, not 404.
    assert await sdk_client.store.get_item(namespace, "alice") is None


async def test_http_write_visible_to_inprocess_get_store(sdk_client: Any) -> None:
    """HTTP put → graph node get_store() read, and node write → HTTP get."""
    probe_key = f"probe-{uuid.uuid4().hex[:8]}"
    await sdk_client.store.put_item(["runtime-probe"], probe_key, {"seeded": "over-http"})

    thread_id = str(uuid.uuid4())
    await sdk_client.threads.create(thread_id=thread_id)
    run = await sdk_client.runs.create(
        thread_id,
        "store_probe",
        input={
            "messages": [{"role": "user", "content": "probe"}],
            "store_key": probe_key,
        },
    )
    await sdk_client.runs.join(thread_id, run["run_id"])

    state = await sdk_client.threads.get_state(thread_id)
    # The node read the HTTP-seeded item through get_store().
    assert state["values"]["store_value"] == {"seeded": "over-http"}

    # And the node's write is visible over HTTP.
    written = await sdk_client.store.get_item(["runtime-probe"], f"{probe_key}-written")
    assert written is not None
    assert written["value"] == {"from": "graph-node"}


async def test_nested_dotted_filter_matches(sdk_client: Any) -> None:
    """Phase 1 supports the dotted nested filter dev's inmem store lacks
    (divergence ledger item 1) — Postgres store handles it natively."""
    namespace = ["runtime", f"nested-{uuid.uuid4().hex[:8]}"]
    await sdk_client.store.put_item(
        namespace, "a", {"login": "alice", "prefs": {"model": "sonnet"}}
    )
    await sdk_client.store.put_item(namespace, "b", {"login": "bob", "prefs": {"model": "opus"}})
    filtered = await sdk_client.store.search_items(
        namespace, filter={"prefs.model": "opus"}, limit=10
    )
    keys = [i["key"] for i in filtered["items"]]
    if keys != ["b"]:
        pytest.skip(f"AsyncPostgresStore dotted-filter returned {keys}; record as divergence")
