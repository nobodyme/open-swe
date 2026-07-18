"""Wire models. Literals match langgraph_sdk/schema.py; response field sets
match the Phase 0 golden transcripts (tests/contract/golden/)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

RunStatus = Literal["pending", "running", "error", "success", "timeout", "interrupted"]
ThreadStatus = Literal["idle", "busy", "interrupted", "error"]
MultitaskStrategy = Literal["reject", "interrupt", "rollback", "enqueue"]
OnConflictBehavior = Literal["raise", "do_nothing"]
IfNotExists = Literal["create", "reject"]
CancelAction = Literal["interrupt", "rollback"]
SortOrder = Literal["asc", "desc"]
ThreadSortBy = Literal["thread_id", "status", "created_at", "updated_at"]


def iso(dt: datetime) -> str:
    return dt.isoformat()


class ThreadCreateBody(BaseModel):
    thread_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    if_exists: OnConflictBehavior = "raise"


class ThreadUpdateBody(BaseModel):
    metadata: dict[str, Any] = Field(default_factory=dict)


class ThreadSearchBody(BaseModel):
    metadata: dict[str, Any] | None = None
    values: dict[str, Any] | None = None
    status: ThreadStatus | None = None
    limit: int = 10
    offset: int = 0
    sort_by: ThreadSortBy | None = None
    sort_order: SortOrder | None = None
    select: list[str] | None = None


class RunCreateBody(BaseModel):
    assistant_id: str
    input: dict[str, Any] | None = None
    command: dict[str, Any] | None = None
    config: dict[str, Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    multitask_strategy: MultitaskStrategy = "reject"
    if_not_exists: IfNotExists = "reject"
    durability: str | None = None
    webhook: str | None = None
    stream_mode: list[str] | str | None = None
    stream_resumable: bool = False
    # Accepted-and-ignored knobs the SDK may send with defaults:
    stream_subgraphs: bool | None = None
    on_disconnect: str | None = None
    checkpoint_during: bool | None = None
    on_completion: str | None = None
    after_seconds: int | None = None


class CancelManyBody(BaseModel):
    thread_id: str | None = None
    run_ids: list[str] | None = None
    status: str | None = None


class StorePutBody(BaseModel):
    namespace: list[str]
    key: str
    value: dict[str, Any]
    index: Any | None = None
    ttl: float | None = None


class StoreDeleteBody(BaseModel):
    namespace: list[str]
    key: str


class StoreSearchBody(BaseModel):
    namespace_prefix: list[str]
    filter: dict[str, Any] | None = None
    limit: int = 10
    offset: int = 0
    query: str | None = None


class CronCreateBody(BaseModel):
    schedule: str
    assistant_id: str
    input: dict[str, Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] | None = None
    end_time: datetime | None = None
    timezone: str | None = None
    # SDK forwards run-shaped extras on cron bodies; accept and store them.
    multitask_strategy: MultitaskStrategy | None = None
    interrupt_before: Any | None = None
    interrupt_after: Any | None = None
    webhook: str | None = None


class CronSearchBody(BaseModel):
    assistant_id: str | None = None
    thread_id: str | None = None
    limit: int = 10
    offset: int = 0
