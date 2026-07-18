"""Store router (phase-1.md T5) — thin wire over the ONE AsyncPostgresStore.

The same instance is attached to every graph the registry resolves, so
in-process ``get_store()`` and this HTTP surface are one store (MIGRATION §1).
Wire shapes follow langgraph_sdk/_async/store.py; the get-missing-item →
``null`` (not 404) asymmetry is pinned by the Phase 0 contract suite.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from agent_runtime.models import StoreDeleteBody, StorePutBody, StoreSearchBody

router = APIRouter(tags=["store"])


def _item_dict(item: Any) -> dict[str, Any]:
    return {
        "namespace": list(item.namespace),
        "key": item.key,
        "value": item.value,
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "updated_at": item.updated_at.isoformat() if item.updated_at else None,
    }


@router.put("/store/items")
async def put_item(body: StorePutBody, request: Request) -> JSONResponse:
    await request.app.state.store.aput(tuple(body.namespace), body.key, body.value)
    return JSONResponse(None)


@router.get("/store/items")
async def get_item(namespace: str, key: str, request: Request) -> JSONResponse:
    parts = tuple(namespace.split(".")) if namespace else ()
    item = await request.app.state.store.aget(parts, key)
    return JSONResponse(_item_dict(item) if item is not None else None)


@router.delete("/store/items")
async def delete_item(body: StoreDeleteBody, request: Request) -> JSONResponse:
    await request.app.state.store.adelete(tuple(body.namespace), body.key)
    return JSONResponse(None)


@router.post("/store/items/search")
async def search_items(body: StoreSearchBody, request: Request) -> dict[str, Any]:
    items = await request.app.state.store.asearch(
        tuple(body.namespace_prefix),
        filter=body.filter,
        limit=body.limit,
        offset=body.offset,
        query=body.query,
    )
    return {"items": [_item_dict(item) for item in items]}
