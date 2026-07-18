# Migration: Self-Hosted FastAPI + `langgraph` Runtime (Removing `langgraph-api`)

This document plans the removal of Open SWE's runtime dependency on `langgraph-api`
(the `langgraph dev` / `langgraph up` server), which is Elastic License 2.0 and
requires an enterprise license key to self-host in production. The target end
state is a self-hosted runtime built entirely from MIT-licensed packages
(`langgraph`, `langgraph-sdk`, `langgraph-checkpoint-postgres`) that can run on
plain infrastructure (e.g. a single EC2 instance + RDS Postgres) with no
LangGraph Cloud/Platform license.

> **Status (2026-07-18):** the migration is complete, and `langgraph-cli[inmem]`
> has since been removed entirely — `make dev-platform` is gone, the contract
> suite boots `agent_runtime` only, and the golden transcripts in
> `tests/contract/golden/` remain as frozen Phase-0 recordings from
> `langgraph dev`. The rest of this document is the historical design record.

See the licensing discussion that motivated this doc for background on the
Elastic License 2.0 boundary — it applies to `langgraph-api` specifically, not
to `langgraph`, `langgraph-sdk`, `langchain-core`, `deepagents`, or the
Postgres-backed checkpoint/store packages used below.

**Context: we are pre-production.** This is a build-phase evaluation of a
viable agentic-platform stack — there is no production deployment, no users,
and **no data**. That simplifies everything downstream: no data migration, no
staged cutover, no rollback window, no soak periods. The decision being made
is *which runtime to build on*, and the work is: prove the app runs on an
MIT-only runtime, make that the default, and defer all deployment/infra
questions until there is something to deploy.

## 1. What `langgraph-api` actually provides to this app

The obvious assumption is that `langgraph-api` is "just how the agent graph
runs," and that migrating means swapping out `langgraph dev` for a plain
`uvicorn` process. Reading the codebase shows this is **not** the real
dependency surface. `langgraph_sdk.get_client()` — pointed at the `LANGGRAPH_URL`
/ `LANGGRAPH_URL_PROD` env var (defaulting to `http://localhost:2024`, i.e. a
locally running `langgraph dev`) — is used as this app's **primary persistence
and job-orchestration client**, across **42 files** (36 import `langgraph_sdk`
directly; the rest reach it via helpers like `dispatch_client()` and
`thread_ops.langgraph_client()`). Note that **28 call sites construct
`get_client()` with no URL at all** — and this is worse than a config gap:
`langgraph_sdk` 0.4.x's `get_client(url=None)` does
`from langgraph_api.server import app` and mounts an in-process ASGI
transport, i.e. **bare call sites literally import the Elastic-licensed
package at runtime**. Routing them through the URL-resolving helpers is
therefore mandatory Phase 0 work, not cleanup:

| Surface | Used by (examples) | Purpose |
|---|---|---|
| `client.store.{put,get,delete,search}_item(s)` (34/25/12/8 call sites respectively) | `agent/dashboard/plan_store.py`, `agent_usage.py`, `repo_snapshots.py`, `agent_overrides.py`, `agent/utils/{github,slack}_feedback.py`, `agent/utils/thread_ops.py` | Plans, usage tracking, repo snapshots, profile overrides, feedback/reaction dedup state — i.e. general app KV storage |
| `client.threads.{create,get,update,delete,search}` | `agent/dispatch.py`, `agent/server.py`, `agent/tools/slack_start_new_thread.py`, `agent/utils/sandbox_state.py`, `agent/dashboard/thread_api.py`, `schedules.py`, `workflow_approval.py` | Thread lifecycle + metadata (sandbox id, GitHub PR link, Slack channel, etc.) |
| `client.threads.get_state` | `agent/dashboard/thread_api.py:1524` | Reading checkpointed graph state for the dashboard |
| `client.threads.join_stream(thread_id, last_event_id=...)` | `agent/dashboard/thread_api.py:2016` | **Resumable** SSE attach to an in-flight run (reconnect picks up from `last_event_id`) |
| `client.runs.{create,get,list,cancel,cancel_many}` | `agent/dispatch.py`, `agent/dashboard/thread_api.py`, `review_style_jobs.py` | Kicking off and querying agent runs |
| `client.crons.create` (schedule-based) | `agent/dashboard/analyzer_cron.py:42`, `agent/dashboard/schedules.py:236` | Nightly per-repo analyzer crons; user-defined scheduled agents |
| `client.crons.{create_for_thread,search,delete}` | `agent/tools/schedule_thread_wakeup.py` (`create_for_thread` with `end_time` + `timezone`) | User-facing "remind me later" scheduling |
| `threads.search(status="busy")` + `runs.list(status="pending")` + `runs.cancel_many(action="interrupt")` | `agent/reconcile.py` (stale-run sweep, fired by a cron on the `scheduler` graph) | Frees threads stuck `busy` — depends on the server's thread *status* semantics, not just metadata |
| Raw wire proxies (`httpx` straight to the LangGraph server, `X-API-Key` added server-side) | `agent/dashboard/thread_api.py:1835` (`POST /threads/{id}/stream/events`), `:1901` (`POST /threads/{id}/commands`), `:1961` (`POST /threads/{id}/history`), `:1979` (`POST /threads/{id}/runs/{run_id}/cancel`); `review_chat_api.py:471` (commands), `:496` (state/history) | The dashboard browser app (`ui/`) speaks the LangGraph **v2 event-stream / commands protocol** through these proxies |

The raw-proxy rows are a hard compatibility contract with the **frontend**:
`ui/package.json` depends on `@langchain/langgraph-sdk` and `@langchain/react`
(`useStream`), which consume the v2 wire protocol byte-for-byte. The
replacement runtime must reproduce `/stream/events`, `/commands`, `/history`,
and `/state` exactly as `langgraph-api` serves them (or the UI must be
changed — much bigger scope). Pin the `@langchain/*` package versions for the
duration of the migration.

Beyond the endpoint list, `agent/dispatch.py:create_durable_run` passes
run-create options whose *mechanisms* mostly already exist in MIT `langgraph`
— the new server's job is the thin orchestration layer around them, not
reimplementation of the machinery:

- `multitask_strategy="interrupt"` — a new run on a busy thread interrupts
  the in-flight one rather than erroring or double-running. The interrupt/
  checkpoint-resume machinery is MIT `langgraph` (`Command(resume=...)`,
  checkpoint survives asyncio cancellation); the genuinely new code is only
  **per-thread run arbitration** (cancel the in-flight task, then start).
- `durability="sync"` and `if_not_exists="create"`. Note `durability` is a
  plain kwarg on MIT `langgraph`'s `astream`/`ainvoke` (checkpoint is
  written before the next step begins) — the server just forwards it. What
  it does **not** promise is auto-resume after process death; that is a
  server/queue design decision (see §7).
- `webhook=COMPLETION_WEBHOOK_URL` — the **server calls back**
  `/webhooks/run-complete` (HMAC-signed via `RUN_COMPLETE_WEBHOOK_SECRET`)
  when a run finishes; Slack/Linear completion replies depend on this
  (`tests/webhooks/test_completion_webhook.py` covers the receiving side).
  This is genuinely server-side work.
- `after_seconds` — plumbed through `create_durable_run`'s signature but
  **passed by no caller** (verified: all three call sites omit it; thread
  wakeups use `crons.create_for_thread` instead). **Drop it**: delete the
  parameter in Phase 0 rather than carrying an unused platform feature into
  the new server's API.
- `stream_resumable=True` + the dashboard's **seven** stream modes —
  `values, updates, messages, messages-tuple, tools, checkpoints, events`
  (`_DASHBOARD_STREAM_MODES`, `thread_api.py:56-64`). Genuine server-side
  work on two counts: (1) `messages-tuple`, `tools`, and `events` are **not**
  MIT `StreamMode`s (`langgraph/types.py` defines
  values/updates/checkpoints/tasks/debug/messages/custom), so the server owns
  the mode mapping and SSE envelope; (2) resumability requires **buffering
  run events with stable IDs** so a dropped dashboard connection can replay
  from `last_event_id`.

In short: **`langgraph-api` is this app's de facto database and task queue**,
not just "the thing that runs the graph." Any migration plan that only
addresses graph execution and ignores this will miss most of the actual
dependency surface.

The one piece of state that is *not* SDK-mediated is the in-process `Store`
accessor (`langgraph.config.get_store()` / `BaseStore`, used in
`agent/middleware/check_message_queue.py`) — that works against whatever store
the graph was compiled with, regardless of `langgraph-api`, and needs no
protocol-level replacement, only a store instance to be attached at compile
time. Note `check_message_queue.py` uses both access paths for different
things: `get_client().threads.get(...)` (line 40) fetches **thread metadata**
over HTTP, while `get_store()` (line 158) reads/deletes the actual
`pending_messages` / `pending_event` queue items in-process. Both must work
against the new runtime; neither is redundant. **Important consistency
constraint**: the store the graph is compiled with and the store served by the
Store REST endpoints must be the *same* Postgres-backed store, or in-process
reads won't see items written via the HTTP API (and vice versa).

## 2. Two possible strategies

**Strategy A — API-compatible self-hosted server.** Build a small FastAPI
service that implements exactly the REST operations enumerated in the table
above (nothing more — not the full LangGraph Platform API), backed by
`langgraph-checkpoint-postgres`'s `AsyncPostgresSaver` / `AsyncPostgresStore`
(both MIT). Because every call site already resolves its target from one env
var (`LANGGRAPH_URL` / `LANGGRAPH_URL_PROD`), pointing that at the new service
requires **zero changes** to the 36 files above.

**Strategy B — Rip out the SDK.** Replace every `get_client()` call site with
direct in-process store/graph calls. Removes an HTTP hop, but touches
dashboard config storage, feedback tracking, plan storage, thread lifecycle,
webhook dispatch, and cron across 36 files, with no natural way to roll back
incrementally and much higher regression risk for behavior that today has
thin test coverage (see §5).

Strategy B is also worse than it first appears: the browser UI speaks the
LangGraph wire protocol through the raw proxies (§1), so even a full call-site
rewrite still needs a server implementing that protocol — B doesn't remove the
compatibility problem, it just adds a 42-file rewrite on top of it.

**Recommendation: Strategy A.** It concentrates all migration risk into one
new, independently testable service instead of scattering it across the
application, and `LANGGRAPH_URL` stays a clean per-environment switch — during
the build phase that means flipping any dev environment between `langgraph
dev` and the replacement in seconds while comparing behavior.

**Prerequisite cleanup for A:** unify client construction. The 28 bare
`get_client()` (no-URL) sites must resolve through the same env-driven helper
(`thread_ops.langgraph_client()` / `dispatch.dispatch_client()`) so that
`LANGGRAPH_URL` is authoritative everywhere before any cutover.

### 2.1 Prior art: adopt instead of build?

Checked (July 2026): **no fork of `langchain-ai/open-swe` has done this
migration** — none of the ~1,170 forks show related work, and the upstream
repo has no issues/PRs about `langgraph-api` licensing or self-hosted
runtimes. But the *generic* problem is solved in the ecosystem:

**[Aegra](https://github.com/aegra/aegra)** (Apache-2.0, ~1.1k stars,
v0.9.x, actively developed) is an open-source, self-hosted reimplementation
of the LangGraph Platform API — FastAPI + Postgres + Redis, explicitly
compatible with the existing `langgraph_sdk` client. It already provides:
threads CRUD/search, runs create/cancel (cancel propagates cross-instance
via Redis pub-sub), state + full checkpoint history endpoints, the seven
SSE stream modes, **resumable streams**, store endpoints (KV + semantic
search), and crons with timezone support. Verified gaps against *our*
required surface (§1):

- **Run-completion webhooks: explicitly "not yet planned"** in Aegra's
  feature matrix — and Open SWE's Slack/Linear completion replies depend on
  them (`dispatch.py`, `completion.py`). This is the hard blocker.
- **`multitask_strategy`**: present in Aegra's source (`models/runs.py`,
  `run_preparation.py`) but absent from its feature matrix — actual
  interrupt semantics unverified.
- `runs.cancel_many`: no hits in Aegra's source (used twice here, incl.
  `reconcile.py`).
- Aegra **requires Redis** (job queue + pub-sub), conflicting with this
  plan's Postgres-only stance — adopting it means accepting its stack.

**Recommendation**: keep Strategy A's Phase 0 exactly as written — the
contract suite is runtime-agnostic, so run it against Aegra *before* writing
any `agent-runtime` code. Being pre-production strengthens the
adopt-first case: there is no legacy stack the Redis dependency conflicts
with, no migration burden, and evaluating Aegra now costs days against the
weeks of building. If Aegra passes or is close (e.g. only the completion
webhook is missing — a feature upstream may accept as a contribution, or
which can be replaced app-side by polling `runs.get`), adopting it converts
Phase 1 from "build a server" into "close specific gaps." If it diverges
badly on the wire protocol or interrupt semantics, fall back to building
`agent-runtime` as planned — still with full freedom to make
runtime-friendly app changes, since nothing is deployed. Either way Phase 0
is unchanged and nothing is wasted.

(Also checked: `marlenezw/open-swe-python` is an unrelated small port, not
a runtime migration.)

**Dependency facts** (from `uv.lock` / `pyproject.toml`): the Elastic-licensed
code enters only via `langgraph-cli[inmem]`, which pulls `langgraph-api` and
`langgraph-runtime-inmem`. Already-present MIT deps: `langgraph`,
`langgraph-sdk`, `langgraph-checkpoint` (core), `deepagents`, `fastapi`,
`uvicorn`. **`langgraph-checkpoint-postgres` is *not* currently a dependency
and must be added** in Phase 1.

## 3. Target architecture

**Where FastAPI fits — the point of this migration.** Open SWE already *is*
a FastAPI app (`agent/webapp.py` → `agent/api/app.py`), but today it doesn't
serve itself: `langgraph.json`'s `"http": {"app": "agent.webapp:app"}` mounts
it **inside** the Elastic-licensed `langgraph-api` server, which owns the
process, the event loop, and every runtime service around it. The end state
inverts that: **FastAPI (run by plain `uvicorn`) becomes the top-level
server**, twice over —

1. the existing `webapp` FastAPI app is served directly by `uvicorn`
   (`make run` already does exactly this; it just isn't sufficient today
   because the LangGraph runtime is missing), and
2. the new `agent-runtime` is a second FastAPI app that provides the runtime
   services `langgraph-api` used to: it wraps the MIT `langgraph` library
   (compiled graphs, `AsyncPostgresSaver`/`AsyncPostgresStore`) behind the
   REST/SSE endpoints the app already speaks.

`langgraph dev` disappears from every environment except local development.
Whether the two apps run as two `uvicorn` processes or `webapp` is mounted as
a sub-app of `agent-runtime` (single process, preserving today's topology) is
a Phase 1 decision — but note `dispatch.py` refuses loopback completion-webhook
URLs, so the two-process split must keep `COMPLETION_WEBHOOK_URL` pointing at
a real reachable address either way.

```
                     ┌─────────────────────────────┐
  Webhooks/Dashboard │   webapp (existing FastAPI)  │
  (GitHub/Slack/      │   agent/api/app.py            │
   Linear/UI)  ─────► │   unchanged application code  │
                     └───────────────┬───────────────┘
                                     │ langgraph_sdk (LANGGRAPH_URL)
                                     ▼
                     ┌─────────────────────────────┐
                     │   agent-runtime (new)         │
                     │   FastAPI: Threads/Runs/       │
                     │   Store/Crons + SSE run-stream │
                     │   Run executor → compiled       │
                     │   graphs (agent/reviewer/        │
                     │   analyzer/chat/scheduler)        │
                     └───────────────┬───────────────┘
                                     │ AsyncPostgresSaver / AsyncPostgresStore
                                     ▼
                     ┌─────────────────────────────┐
                     │   Postgres (RDS or self-hosted)│
                     │   checkpoints, store, threads,  │
                     │   runs, crons                    │
                     └─────────────────────────────┘
```

- **`agent-runtime`** (new package, e.g. `agent_runtime/`): implements only
  the endpoints in the table in §1 — Threads CRUD+search+`get_state`
  (search must support metadata filters **and** `status="busy"` semantics for
  the reconcile sweep), Runs create/get/list/cancel/cancel_many, Store
  put/get/delete/search (including `filter=` metadata queries + pagination),
  Crons create/create_for_thread (`end_time`, `timezone`)/search/delete, the
  wire endpoints the UI proxies require (`/threads/{id}/stream/events`,
  `/commands`, `/history`, `/state`, `/runs/{run_id}/cancel`, plus SDK
  `join_stream`), and the run-completion webhook callback. Backed by
  `AsyncPostgresSaver` (checkpointer) and `AsyncPostgresStore` (store) from
  `langgraph-checkpoint-postgres`, plus a small owned Postgres schema for
  `threads` (with status), `runs`, `crons`, and a **run-event log** (the
  checkpoint/store tables are created by that package's own `.setup()`).
- **Run execution**: `runs.create` starts the target compiled graph
  in-process, honoring `multitask_strategy="interrupt"` (cancel/interrupt the
  in-flight run on the same thread first — asyncio cancellation propagates
  and the checkpoint survives, which is what makes resume-with-history work)
  and firing the HMAC-signed completion `webhook` on every terminal state.
  Completion handling deliberately treats `interrupted` as healthy
  (`agent/completion.py`). Start with `asyncio.create_task(...)`; if a
  durable queue proves necessary (see §7), implement it as **Postgres
  claimed-rows** — no new infrastructure. Redis is explicitly out of scope
  for this migration: nothing in the licensed surface requires it, current
  production runs none, and adding it would be infra creep unrelated to the
  license removal.
- **Resumable streaming**: because the dashboard sets `stream_resumable=True`
  and reconnects with `last_event_id`, run events must be persisted (the
  run-event log above), not just fanned out live. Simplest viable design:
  append each stream event to Postgres with a monotonic per-run sequence id
  and serve SSE by replay-then-tail. Postgres-only is the design, not one
  option among several — revisit only if a *measured* latency problem
  appears after cutover.
- **Cron firing**: `crons.create_for_thread` persists a row; an APScheduler
  (BSD-licensed) loop or a plain periodic task polls due crons and creates
  the corresponding run — replacing LangGraph Platform's managed cron.
- **Checkpoint TTL**: `langgraph.json`'s `checkpointer.ttl` (delete strategy,
  60 min sweep, 43200 min = 30 day TTL — note the unit is minutes, and the
  inmem dev runtime's TTL sweep is a no-op, so today's config is effectively
  dead) is a Platform feature; reimplement as a periodic
  sweep job in `agent-runtime` deleting expired rows via `AsyncPostgresSaver`.
- **`webapp`** (existing dashboard + webhooks app): unchanged code, but a
  changed *host*: instead of being mounted inside `langgraph-api` via
  `langgraph.json`'s `http.app`, it is served by plain `uvicorn` (standalone,
  or mounted as a sub-app of `agent-runtime` — Phase 1 decision). Its
  `LANGGRAPH_URL` config changes from `http://localhost:2024` (`langgraph
  dev`) to the new `agent-runtime` service's URL.
- **`langgraph.json` / `langgraph dev`**: kept as a **local-dev-only** path
  (already covered by the Elastic license's non-production terms), clearly
  documented as such. Once Phase 2 lands, it is a comparison/debugging tool,
  not the default runtime. [Since removed — 2026-07-18: `langgraph dev` is
  gone entirely; `langgraph.json` lives on as `agent_runtime`'s own config
  file.]
- **Infra (build phase)**: a local/CI `docker-compose` Postgres is all that's
  needed now. The eventual deployment footprint this design implies — one box
  running `uvicorn` + one Postgres instance (e.g. EC2 + RDS), no Redis, no
  queue service — is a consequence to note, not work to do today.

## 4. Testing strategy

### 4.1 Current state (baseline, read before planning further work)

The suite has 136 test files / ~1,285 test functions under `tests/`, plus a
Playwright `tests/e2e/` suite. `pyproject.toml` confirms `asyncio_mode =
"auto"`; there is exactly one `conftest.py` with a single autouse fixture
(stubs `is_review_repo_enabled`) — most mocking is bespoke per test file, not
shared fixtures. No tests exercise a real Postgres/Redis instance today.

**Strong coverage**: GitHub webhooks (`tests/github/test_github_issue_webhook.py`,
30 tests, real HMAC signatures through a `TestClient` against the actual
FastAPI app), reviewer tools (`add_finding`/`update_finding`/`list_findings`,
28 tests), dashboard routes (21 files — OAuth, team settings, thread API),
sandbox lifecycle (11 files), and all five named middlewares.

**Coverage gaps on the migrated surface** — only gaps that touch the
`langgraph-api` boundary block this migration:

- **No unit-level test invokes the compiled `agent` graph at all.** The only
  place the real graph runs today is the Playwright e2e suite via `langgraph
  dev` + a fake LLM — i.e. the one test that actually exercises graph
  execution is itself hard-wired to the runtime being replaced.
- **No test for the compiled `scheduler` graph** (only the dashboard API that
  *registers* scheduler cron jobs is tested).

Two further gaps exist — Linear signature verification is only tested with
`verify_linear_signature` patched to `True`, and `get_thread_id_from_branch`
has no direct unit test — but both live in webapp code this plan leaves
**unchanged** (§3), so they are general hygiene backlog, **not** migration
prerequisites. They're noted here so nobody re-adds them to the critical path.

### 4.2 Phase 0 — close the gaps (before touching the runtime)

1. Add a unit-level test that compiles and invokes the `agent` graph
   in-process (fake LLM, mocked sandbox/store/checkpointer, no `langgraph
   dev`). This is the most important addition: it's the first test that can
   run identically against either runtime, and becomes the seed for the
   contract suite in §4.3.
2. Add a compiled-graph invocation test for `scheduler`.

### 4.3 Phase 0 — build a runtime-swappable contract-test harness

This is the "clean, reproducible way to test" the migration itself, and the
mechanism for knowing when it's actually done:

1. **Contract suite.** Turn the operation inventory in §1's table into a
   fixed pytest module (e.g. `tests/contract/test_langgraph_api_contract.py`)
   that, given a `LANGGRAPH_URL`, runs a scripted sequence: create thread →
   update metadata → search → `get_state` → create run (with
   `multitask_strategy="interrupt"`, `if_not_exists="create"`, a completion
   `webhook` pointed at a local receiver) → poll run to
   completion → assert the completion webhook fired with a valid HMAC →
   put/get/delete/search store items → create/create_for_thread/search/delete
   crons → `join_stream` a run, kill the connection mid-stream, reconnect
   with `last_event_id`, and assert no events were lost or duplicated. Run it
   against **today's** `langgraph dev` first to capture a golden baseline,
   then re-run against `agent-runtime` once built. Any diff is a correctness
   bug in the new service, not a matter of opinion. (One pre-production
   liberty: where matching `langgraph-api` exactly is expensive and the
   behavior is only consumed by our own code, it's legitimate to change the
   *app* instead and update the contract test — the baseline is a tool for
   catching accidental divergence, not a compatibility oath to a server
   we're removing.)
2. **Parameterize the e2e suite.** The e2e boot path is
   `tests/e2e/playwright.config.ts`, which launches `uv run langgraph dev
   --config tests/e2e/langgraph.e2e.json` as Playwright's `webServer`
   (`tests/e2e/harness.py` is the http.app *served by* it, and
   `agent_entrypoint.py` wires the real agent graph). Add a
   `RUNTIME=platform|embedded` switch to the Playwright config so the same
   six specs (`full_flow.spec.ts`, `slack_untagged_reply.spec.ts`,
   `slack_debounce.spec.ts`, `plan_review.spec.ts`, `dashboard.spec.ts`,
   `sandbox_id.spec.ts`) can launch either `langgraph dev` or `uvicorn
   agent_runtime.app:app` on `E2E_PORT`. `slack_debounce.spec.ts` is
   especially valuable: it exercises **multitask-interrupt mid-run** against
   a deterministic busy window (`E2E_BUSY_HOLD_SECONDS`), i.e. the exact
   platform semantic that is hardest to reimplement. The dashboard spec runs
   the real built `ui/` app, so it also validates the v2 wire protocol
   end-to-end. **Passing the full e2e suite unmodified with
   `RUNTIME=embedded` is the acceptance bar for cutover** — not "it looks
   like it works."
3. **Reproducible local Postgres.** Add a `docker-compose.test.yml` (or a
   `testcontainers-python` fixture, MIT-licensed) providing an ephemeral
   Postgres for `agent-runtime`'s own tests — today's suite uses no real
   database at all, so this is new infrastructure, not a tweak.

### 4.4 Post-cutover

- Keep the contract suite running in CI on every PR touching `agent-runtime`.
- Run the full e2e suite in CI (nightly or per-PR) with `RUNTIME=embedded`,
  so the replacement runtime is continuously validated as the app evolves.

## 5. Phased task breakdown

**Phase 0 — Test hardening + boundary cleanup (no behavior changes)**
1. Compiled-`agent`-graph invocation unit test
2. Compiled-`scheduler`-graph test
3. Contract-test module, run against current `langgraph dev` as golden baseline
4. Parameterize the e2e boot (`playwright.config.ts`) for a pluggable runtime backend
5. `docker-compose.test.yml` / testcontainers Postgres fixture
6. Route the 28 bare `get_client()` sites through the env-driven helpers
   (`thread_ops.langgraph_client()` / `dispatch.dispatch_client()`) — these
   sites import `langgraph_api` at runtime via the SDK's in-process
   transport, so this is mandatory, not cleanup
7. Delete the unused `after_seconds` parameter from `create_durable_run`

(Deliberately excluded: Linear-signature and `get_thread_id_from_branch`
tests — webapp-side hygiene unrelated to the replaced surface; see §4.1.)

**Phase 1 — Build `agent-runtime` (additive; not wired to prod yet)**
1. New `agent_runtime/` package + Postgres schema for `threads` (with
   status)/`runs`/`crons` and the run-event log (checkpoint/store tables come
   from `langgraph-checkpoint-postgres`'s own `.setup()`); **add
   `langgraph-checkpoint-postgres` to dependencies** (it is not one today)
2. Threads endpoints (create/get/update/delete/search); `get_state`,
   `/state`, and `/history` are **thin HTTP wrappers over MIT `langgraph`'s
   `graph.aget_state` / `aget_state_history` / `aupdate_state`** — size the
   work as serialization shims, not checkpoint-traversal logic
3. Store endpoints wrapping `AsyncPostgresStore` — the **same instance** the
   graphs are compiled with (see §1's consistency constraint)
4. Runs create + async executor invoking the target compiled graph
   (`agent`, `reviewer`, `analyzer`, `chat`, `scheduler`) with
   `AsyncPostgresSaver` as checkpointer — honoring
   `multitask_strategy="interrupt"`, `if_not_exists`, optionally
   `after_seconds` (see §1 — currently uncalled; implement or drop), and
   firing the HMAC-signed completion webhook (`RUN_COMPLETE_WEBHOOK_SECRET`)
5. Runs get/list/cancel/cancel_many
6. Run-event log + resumable SSE: persist `astream_events` output with
   per-run sequence ids; implement `threads.join_stream` (with
   `last_event_id` replay) and the UI wire endpoints —
   `POST /threads/{id}/stream/events`, `/commands`, `/history`, `/state`,
   `/runs/{run_id}/cancel` — matched against `langgraph-api`'s actual wire
   format via the Phase 0 golden transcripts (see §7)
7. Crons — both `crons.create` (schedule-based: analyzer nightly, scheduled
   agents, the reconcile sweep on the `scheduler` graph) and
   `crons.create_for_thread` with `end_time`/`timezone` (thread wakeups) —
   plus an APScheduler (BSD) firing loop
8. Decide the serving topology: two `uvicorn` processes (`webapp` +
   `agent-runtime`) vs `webapp` mounted as a sub-app of `agent-runtime`
   (single process, closest to today's layout); either way `uvicorn`+FastAPI
   replaces `langgraph dev` as the process entrypoint
9. Run the Phase 0 contract suite against `agent-runtime`; iterate to parity

**Phase 2 — Validate and adopt as the default dev runtime**

There is no production to cut over and no data to migrate — validation is
just: the app works on the new runtime, provably.

1. Run the parameterized e2e suite with `RUNTIME=embedded`; all six specs
   green is the acceptance bar
2. Chaos test: SIGKILL the runtime mid-run. What `durability="sync"`
   (an MIT `langgraph` kwarg) actually guarantees is that the checkpoint is
   written before the next step — so the assertable floor is: no state
   corruption, and the reconcile sweep frees the thread left stuck `busy`.
   Whether the run *auto-resumes* is a property of the queue design chosen
   in Phase 1 (see §7) — pin the test's acceptance criterion to whichever
   behavior was decided, don't assume resume
3. Manual smoke pass of the real flows: dashboard chat streaming (real
   `ui/` build), Slack/Linear/GitHub-triggered runs, a scheduled analyzer
   cron firing, plan review flow — checking for stuck-busy threads and a
   completion webhook on every terminal run
4. Flip the default: make `agent-runtime` (+ docker-compose Postgres) the
   documented way to run the stack; `langgraph dev` remains available but
   is no longer the default path [Since removed — 2026-07-18]

**Phase 3 — Hygiene**
1. Add a lint guard banning bare `get_client()` calls (no `url=`) — e.g. a
   ruff `banned-api` rule or a small CI grep. Rationale: the SDK's
   `get_client(url=None)` silently imports `langgraph_api`'s in-process
   transport when that package is importable, so a future bare call would
   silently bind to the Elastic runtime in dev; the lint makes the
   regression loud at review time instead. (`langgraph-cli[inmem]` itself
   stays a normal dependency — ELv2 permits dev use, and having the package
   installed violates nothing. [Since removed — 2026-07-18: it is no longer
   a dependency at all.] Licensing facts for the record:
   `langgraph-cli` is MIT; its `[inmem]` extra pulls `langgraph-api` and
   `langgraph-runtime-inmem`, the only Elastic-2.0 packages in the
   dependency tree. If a deployment artifact is ever built, excluding the
   extra there is a size/auditability nicety, decided then.)
2. Reimplement the checkpoint TTL sweep as a periodic `agent-runtime` job
   (replaces `langgraph.json`'s `checkpointer.ttl`)
3. Update `docs/INSTALLATION.md`, which currently documents only "push to
   GitHub → connect to LangGraph Cloud" as the production path
4. Note the LangSmith-managed sandbox default and LangSmith tracing as a
   **separate**, not-yet-addressed SaaS dependency (out of scope here)

**Deferred until there's something to deploy** (explicitly *not* part of
this migration): provisioning EC2/RDS or any cloud infra, building an
app-server Docker image (the current root `Dockerfile` is
sandbox-execution-only — no `CMD`, no app code), production hardening
(reverse proxy, rate limiting, TLS), and any data-migration story — there
is no data. When deployment happens, the runtime choice made here means it
needs only: a box running `uvicorn` and a Postgres instance.

## 6. Effort shape (rough, not a commitment)

- Phase 0: small — mostly additive tests, days not weeks
- Aegra evaluation (§2.1): days — run the Phase 0 contract suite against it
  before committing to build
- Phase 1: the bulk of the work *if building* — a small but correct stateful
  HTTP API, Postgres schema, SSE streaming, and cron, all with real test
  coverage; shrinks to gap-closing if Aegra pans out
- Phases 2–3: small — validation against the Phase 0 harness and dependency
  hygiene, not new application logic

## 7. Open questions / risks

- **Exact wire-protocol shape.** The dashboard proxies four raw endpoints
  (`/stream/events`, `/commands`, `/history`, `/state` — §1) consumed by
  `@langchain/react`'s `useStream` in the browser, plus `join_stream`
  (event/data/id triples) re-emitted by `thread_api.py:2016`.
  `agent-runtime` must match all of these precisely — capture
  `langgraph-api`'s actual wire output as golden transcripts in the Phase 0
  contract baseline before implementing Phase 1 item 6, don't assume. Pin
  `@langchain/langgraph-sdk` / `@langchain/react` versions in `ui/` for the
  duration of the migration.
- **Search/filter operator semantics.** `threads.search` is used with
  metadata filters, `status="busy"`, and pagination (`reconcile.py`,
  `agent_usage.py:494`), and `store.search_items` with `filter=` metadata
  queries (`schedules.py:185`, `profiles.py:363`) — including behavior on
  nested keys. These operator semantics must be pinned by the contract suite,
  not guessed at.
- **Run durability.** Whether `asyncio.create_task`-based execution is
  durable enough (survives an `agent-runtime` restart mid-run) or a
  Postgres claimed-rows queue is required is a real design decision, not a
  default — resolve it based on acceptable run-loss risk once Phase 1
  starts, not here. Be precise about what exists today: `durability="sync"`
  is an MIT `langgraph` execution kwarg (checkpoint-before-next-step);
  auto-restart of dead runs is a **server** behavior (`langgraph-api`'s run
  sweeper) that the new runtime either replicates via the queue or
  explicitly declines, relying on the reconcile sweep + user re-trigger.
- **`multitask_strategy="interrupt"` semantics.** Interrupting an in-flight
  deep-agent run mid-tool-call (sandbox command running, Slack status
  mid-update) has edge cases the platform currently owns. The contract suite
  should include a double-dispatch test on one thread to pin the expected
  observable behavior before reimplementing it.
- **`langgraph-checkpoint-sqlite` license-file gap.** Unrelated to this plan
  (we're targeting Postgres, not SQLite), but noted because a GitHub
  discussion flags that package's PyPI distribution as missing its LICENSE
  file in some releases — avoid it if this ever becomes relevant.
- **Not in scope.** This migration addresses the `langgraph-api` licensing
  hurdle only. It does not address the default `SANDBOX_TYPE=langsmith`
  (paid, usage-billed) sandbox provider or LangSmith tracing — both are
  separate SaaS dependencies with their own follow-up.
