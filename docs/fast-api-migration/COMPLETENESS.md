# FastAPI-Runtime Migration — Completeness Report

**Date:** 2026-07-17 · **Branch:** `feat/fastapi-runtime` · **Status:** complete (phases 0–3)

## What the migration removed, and what replaced it

Open SWE previously ran on `langgraph dev` / `langgraph up` — the
**Elastic-2.0-licensed** `langgraph-api` server (pulled in via
`langgraph-cli[inmem]`), which was the app's de facto database, task queue,
streaming layer, and cron scheduler (~180 SDK call sites across 42 files,
plus the browser's `@langchain/react` wire protocol).

It now runs on **`agent_runtime/`** — a FastAPI app served by plain uvicorn,
built ONLY from MIT-licensed packages (`langgraph`, `langgraph-sdk`,
`langgraph-checkpoint-postgres`, FastAPI, psycopg, APScheduler) over
Postgres. Nothing in `agent/` changed behavior: the app still talks to
`LANGGRAPH_URL` through the same SDK; the runtime behind that URL changed.

| Surface the app consumes | Status |
|---|---|
| Threads: create (`if_exists="do_nothing"`), get, update (merge), delete, search (metadata incl. nested, `status`, `limit/offset`, `sort_by/sort_order`, `select`) | ✅ contract-pinned |
| Thread state/history: SDK `get_state`, raw `GET /state`, `POST /history` | ✅ contract-pinned |
| Runs: create (`multitask_strategy="interrupt"` arbitration, `if_not_exists`, `durability="sync"`, completion webhook on every terminal state), get, list (status filter), cancel, `cancel_many(action=…)` as query param | ✅ contract-pinned |
| Store: put/get/delete/search over ONE `AsyncPostgresStore` shared between HTTP and in-graph `get_store()` | ✅ identity-tested |
| Crons: `create`, `create_for_thread(end_time, timezone)` one-shot, `search`, `delete`, APScheduler firing | ✅ live-fire tested |
| v2 dashboard wire: `POST /threads/{id}/stream/events` (channels, `since` resume), `POST /commands` (`run.start`), SDK `join_stream` with `Last-Event-ID` | ✅ golden-transcript parity |
| Reconcile sweep, completion webhooks (`?token=` auth), queue-drain middleware, thread-wakeup crons | ✅ exercised end-to-end |

## Evidence (all gates green at the phase-boundary commits)

- **Contract parity:** 28-test suite + 25 golden transcripts captured from
  `langgraph dev` (Phase 0); `CONTRACT_RUNTIME=embedded` matches the same
  goldens (27 passed + 2 ledgered skips) and `platform` still passes —
  the suite was never bent to fit the new runtime.
- **e2e (the hard gate):** all six Playwright specs — 14 tests including the
  multitask-interrupt "slack_debounce" crown jewel and the real-browser
  `@langchain/react` dashboard — green on `RUNTIME=embedded`, twice
  back-to-back (hermetic), and still green on `RUNTIME=platform`. CI runs
  both legs as a matrix.
- **Chaos floor:** SIGKILL mid-run → consistent checkpoint prefix, orphans
  swept to `error` with exactly one token-valid failure webhook,
  `agent.reconcile` frees stuck-busy threads, randomized kill offsets hold —
  three consecutive green runs (`RUN_CHAOS=1`).
- **Runtime suite:** 35 Postgres-backed tests (arbitration, checkpoint
  survival, exactly-once webhooks, seq persistence across restarts,
  streaming resume, crons, TTL sweep, store identity, the REAL deep-agent
  factory in a patched subprocess, opt-in LiteLLM real-model smoke).
- **Hermetic suite:** 1590+ tests pass with Docker stopped (everything
  Postgres-dependent collects-and-skips; exit 0).
- **Real-model smoke:** the full stack (webhook → run → real minimax-m3 via
  the local LiteLLM proxy → Slack reply) exercised live on the embedded
  runtime; streaming reconnect, cancel, cron wakeup, thread-list ordering
  verified; zero stuck-busy threads. No paid cloud API involved (the
  `litellm:` provider is fail-closed on `LLM_PROVIDER` + `LITELLM_BASE_URL`).
- **Adversarial reviews:** every phase was reviewed by an adversarial
  subagent; all blockers/majors fixed with regression tests (notable: cancel
  double-finalize, post-restart seq collision, Interrupt wire corruption,
  LiteLLM cloud-fallback hole). Dispositions are in each phase doc's
  completion record.

## License posture

- Runtime dependencies of the serving path: MIT only. The import-hygiene
  test pins that `agent_runtime.app`'s transitive closure contains neither
  `langgraph_api` nor `langgraph_runtime_inmem`.
- `langgraph-cli[inmem]` remains a NORMAL dependency by decision — ELv2
  permits development use; `make dev-platform` and the contract baseline
  need it. Production never imports it.
- Guards: ruff TID251 bans `langgraph_sdk.get_client`/`get_sync_client`
  outside the two URL-resolving helpers (fires in `make lint` and editors);
  the import-hygiene subprocess probes are the dynamic pin.

## Recorded divergences from `langgraph dev` (full ledger: tests/contract/golden/README.md)

1. Postgres store SUPPORTS dotted nested filters (superset of dev's inmem).
2. Run-level `stream_mode="events"` accepted-and-ignored (no app caller).
3. TTL sweep keeps `rt_thread`/`rt_run` (metadata is app state); platform
   `strategy="delete"` would drop the thread. New enforcement either way —
   dev's TTL sweep is a no-op.
4. `.env` handling: `make dev` lets the shell win; `langgraph dev` lets
   `.env` override the shell (documented in CLAUDE.md).
5. Preserved dev quirks (deliberately): `runs.join` on `/commands`-started
   runs returns a v2 envelope; no post-completion stream replay; store
   `get_item` returns `null` (not 404) for missing items; `stream_mode="tools"`
   422s on SDK endpoints while `/commands` accepts it.

## Architectural properties worth knowing

- **Single process by design** (D1): per-thread arbitration locks, seq
  counters, and SSE fanout are in-process; a Postgres advisory lock fails a
  second worker's boot. Scaling beyond one process is future work.
- **No durable run queue / no auto-resume** (D3): `durability="sync"`
  checkpoints before each step; a crash marks orphans `error` on the next
  boot and the failure webhook tells the user; a re-trigger resumes from the
  checkpoint. The chaos suite pins exactly this floor.
- **Worker-pickup delay** (`AGENT_RUNTIME_PICKUP_DELAY_MS`, default 500ms)
  models the platform queue's pickup latency — load-bearing for the Slack
  untagged-follow-up coalescing window.
- Two MIT-langgraph integration facts encoded in the runtime (see
  phase-1.md §8): config-key checkpointer injection breaks DeltaChannel
  replay in `aget_state` (attach the saver as the compiled checkpointer
  instead), and gated factories need `__is_for_execution__` in the FACTORY
  config only (never the run config, or it leaks into checkpoints).

## Out of scope (unchanged, each with its own follow-up)

- **LangSmith sandboxes** (`SANDBOX_TYPE=langsmith`) and **LangSmith
  tracing** — paid SaaS dependencies, untouched; `SANDBOX_TYPE` is already
  pluggable (docs/CUSTOMIZATION.md).
- Deployment/infra (compose files for prod, monitoring, backups) — deferred
  until there's something to deploy (pre-production posture).
- Multi-worker scaling of `agent_runtime`.

## How to keep it honest

- `make contract-test` (+ `CONTRACT_RUNTIME=embedded`) is the parity oracle;
  goldens re-record only via `CONTRACT_RECORD=1`.
- `RUNTIME=platform|embedded npx playwright test` is the acceptance
  instrument; CI runs both.
- `RUN_CHAOS=1 uv run pytest tests/chaos/` is the durability floor.
- Every deliberate behavior difference goes in the divergence ledger, in the
  same commit as the change.
