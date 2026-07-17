"""StateSnapshot → wire dict (shape pinned by thread_get_state.json golden)."""

from __future__ import annotations

from typing import Any

from agent_runtime.streams import _dump


def _checkpoint_ref(config: Any) -> dict[str, Any] | None:
    if not isinstance(config, dict):
        return None
    configurable = config.get("configurable") or {}
    if not configurable.get("checkpoint_id"):
        return None
    return {
        "checkpoint_id": configurable.get("checkpoint_id"),
        "checkpoint_ns": configurable.get("checkpoint_ns", ""),
        "thread_id": configurable.get("thread_id"),
    }


def serialize_state_snapshot(snapshot: Any, *, thread_id: str | None = None) -> dict[str, Any]:
    checkpoint = _checkpoint_ref(getattr(snapshot, "config", None))
    parent = _checkpoint_ref(getattr(snapshot, "parent_config", None))
    tasks = []
    interrupts: list[Any] = []
    for task in getattr(snapshot, "tasks", ()) or ():
        task_interrupts = [_dump(i) for i in getattr(task, "interrupts", ()) or ()]
        interrupts.extend(task_interrupts)
        tasks.append(
            {
                "id": getattr(task, "id", None),
                "name": getattr(task, "name", None),
                "path": list(getattr(task, "path", ()) or ()),
                "error": _dump(getattr(task, "error", None)),
                "interrupts": task_interrupts,
                "checkpoint": _dump(getattr(task, "state", None))
                if isinstance(getattr(task, "state", None), dict)
                else None,
                "state": None,
                "result": _dump(getattr(task, "result", None)),
            }
        )
    created_at = getattr(snapshot, "created_at", None)
    metadata = _dump(getattr(snapshot, "metadata", None) or {})
    # Dev's serializer stamps thread_id into the wire metadata (the
    # checkpointer itself strips runtime keys from checkpoint metadata).
    if thread_id is not None and isinstance(metadata, dict):
        metadata.setdefault("thread_id", thread_id)
    return {
        "values": _dump(getattr(snapshot, "values", None)),
        "next": list(getattr(snapshot, "next", ()) or ()),
        "tasks": tasks,
        "metadata": metadata,
        "created_at": created_at,
        "checkpoint": checkpoint,
        "checkpoint_id": (checkpoint or {}).get("checkpoint_id"),
        "parent_checkpoint": parent,
        "parent_checkpoint_id": (parent or {}).get("checkpoint_id"),
        "interrupts": interrupts,
    }
