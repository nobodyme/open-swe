# Phase 1 — Final Implementation Plan: Build `agent_runtime`

**Branch:** `feat/fastapi-runtime`. **Source of truth:** `docs/MIGRATION.md` §1 (operation inventory), §3 (target architecture), §5 Phase 1. **Inputs:** Phase 0's contract suite + golden transcripts (`tests/contract/`), `tests/support/postgres.py`, `docker-compose.test.yml`, the `RUNTIME=platform|embedded` Playwright switch, and the §1 name ledger in `docs/fast-api-migration/phase-0.md` (binding here). **Additive phase:** nothing in `agent/` changes behavior; `langgraph dev` remains the default runtime until Phase 2 flips it.

---

## 0. Objective and non-goals

**Objective.** Build the `agent_runtime/` package: a FastAPI app served by plain `uvicorn` that provides exactly the runtime surface the app consumes from `langgraph-api` today — Threads (CRUD + search with `sort_by`/`sort_order`/`select` + `if_exists="do_nothing"` + `get_state`/`/state`/`/history`), Runs (create/get/list/cancel/cancel_many with `multitask_strategy="interrupt"` arbitration and HMAC-tokened completion webhooks on **all** terminal states), Store (put/get/delete/search over the *same* `AsyncPostgresStore` the graphs run with), Crons (`create` + `create_for_thread(end_time, timezone)` fired by APScheduler), and the dashboard wire endpoints (`/threads/{id}/stream/events`, `/commands`, `/history`, `/state`, `/runs/{run_id}/cancel`, SDK `join_stream` with `last_event_id` replay) — backed by MIT `langgraph`, `langgraph-checkpoint-postgres` (`AsyncPostgresSaver`/`AsyncPostgresStore`), and a small owned Postgres schema. Parity is judged by the Phase 0 contract suite and golden transcripts, not by opinion.

**Non-goals.** No production wiring (Phase 2 flips the default; the e2e suite under `RUNTIME=embedded` is explicitly **not** a Phase 1 gate). No Redis, ever (MIGRATION §3). No durable run queue / auto-resume (D3 declines it; see below). No checkpoint-TTL sweep or lint guards (Phase 3). No endpoints without an app caller — the full Platform API is out of scope (MIGRATION §3: "implements only the endpoints in the table in §1"). No changes to `ui/` or to `agent/` application code beyond test fixtures. No data migration machinery (pre-production).

---

## 1. Decisions (D1–D6) and the cross-phase name ledger

- **D1 — Serving topology: single process, webapp mounted as sub-app.** `agent_runtime/app.py` builds the runtime FastAPI app, registers all runtime routers first, then mounts the existing webapp (`agent.webapp:app`, a re-export of `agent/api/app.py` — `agent/webapp.py:3`) as a root catch-all (`app.mount("/", webapp)` after all runtime routes; Starlette matches routes in order, so runtime paths win and everything else falls through to the webapp). Justification: (a) MIGRATION §1's store-consistency constraint (in-process `get_store()` in `agent/middleware/check_message_queue.py:158` must see HTTP-store writes) is trivially satisfied by one process holding one `AsyncPostgresStore`; (b) Phase 2's e2e suite hard-requires one origin (`slack_debounce.spec.ts` does same-origin `GET /threads/{id}` against the harness base URL; `tests/e2e/e2e_env.py` points `LANGGRAPH_URL` at the same origin); (c) it preserves today's topology (webapp mounted inside `langgraph-api` via `langgraph.json`'s `http.app`), so `LANGGRAPH_URL=http://localhost:2024` keeps working unchanged. The webapp's SDK calls go over HTTP to the same socket — same as under `langgraph dev` today. `dispatch.py`'s loopback-webhook degradation (`agent/dispatch.py:64-72` warns and attaches no webhook for loopback URLs) behaves identically to today; the e2e harness/Phase 2 own that env.
- **D2 — Crash-swept orphans are marked `error`, not `interrupted`.** The startup sweep (T6) marks runs left `pending`/`running` by a dead process as `error` and fires their completion webhooks. Rationale (folds the major finding): the completion receiver acts only on `_TERMINAL_FAILURE_STATUSES = frozenset({"error", "timeout"})` and *intentionally excludes* `interrupted` because interrupt is the healthy multitask-follow-up path (`agent/completion.py:35`, non-failure early-return at `:155-156`); sweeping orphans to `interrupted` would silence exactly the "run died on a server recycle" case completion replies exist for (`agent/completion.py:5-7`, `agent/dispatch.py:12-13`). Cancel-driven interrupts (user follow-up, reconcile's `cancel_many(action="interrupt")`) stay `interrupted`. **This supersedes the `UPDATE rt_run SET status='interrupted'` wording in phase-2.md's header** — Phase 2 T3 already defers to "what Phase 1 actually shipped": its chaos test should assert exactly one HMAC-valid **failure** delivery per orphan.
- **D3 — Run execution: `asyncio.create_task`, no durable queue, no auto-resume.** MIGRATION §3/§7 offer this explicitly. What's guaranteed: `durability="sync"` (forwarded to `astream`) checkpoints before each step; on process death the checkpoint survives, the startup sweep (D2) marks the orphan `error`, the failure reply tells the user, and a re-trigger resumes from the checkpoint (that is what `multitask_strategy="interrupt"` + checkpoint history already give the app). What's declined: silent re-execution of dead runs. Phase 2's chaos suite pins this floor.
- **D4 — Naming authority (binding on Phases 2–3; extends Phase 0's §1 ledger).** Owned Postgres tables: **`rt_thread`**, **`rt_run`**, **`rt_cron`**, **`rt_thread_event`** (run-event log). Cron loop module: **`agent_runtime/cron_scheduler.py`**. Runtime app: **`agent_runtime/app.py`** exposing `app` (matches Phase 0's `RUNTIME=embedded` = `uvicorn agent_runtime.app:app`). Config env var: **`AGENT_RUNTIME_CONFIG`** (path to a `langgraph.json`-shaped file; default `langgraph.json`). Phase 3's sweep SQL and §0 verify commands must target these names — the stale `runs`/`run_events`/`agent_runtime.scheduler` names in earlier Phase 3 drafts are wrong and must be rewritten (cross-phase naming finding, folded). The checkpoint/store tables are created by `langgraph-checkpoint-postgres`'s own `.setup()` and are not renamed.
- **D5 — No-Docker behavior of `tests/agent_runtime/` (load-bearing for Phase 2/3 acceptance).** Per Phase 0's §1 ledger: `tests/agent_runtime/conftest.py` reuses `tests/support/postgres.py` and a session-scoped autouse fixture that `pytest.skip`s the entire package when `TEST_POSTGRES_DSN` is unset **and** Docker is absent. The modules ride the plain suite (no marker needed — the skip fixture is the gate), so `make test` with Docker stopped exits 0 with `tests/agent_runtime/` collected-and-skipped, never erroring. The one exception: `test_litellm_smoke` additionally carries `@pytest.mark.litellm` (Phase 0 marker conventions; excluded from default runs by its own env gate + marker).
- **D6 — Scope rule: every route must cite an app caller.** The verified SDK-method inventory over `agent/` is exactly: `threads.{create,get,update,delete,search,get_state,join_stream}`, `runs.{create,get,list,cancel,cancel_many}`, `store.{put_item,get_item,delete_item,search_items}`, `crons.{create,create_for_thread,search,delete}`, plus the four raw proxies (`thread_api.py:1835,1901,1961,1979`) and `review_chat_api.py`'s commands/state/history proxies. Consequences (folds two findings): **no `DELETE /threads/{id}/runs/{run_id}` route** (SDK has it at `langgraph_sdk/_async/runs.py:1187`; zero app callers); **`action="rollback"` collapses to a 400** with a one-line comment (app sends only `interrupt` — `agent/reconcile.py:96-100`, `agent/dashboard/thread_api.py:1423` — or the default on `runs.cancel` at `:1403`); no `/runs/batch`, `/threads/copy|count|prune`, `/store/namespaces`, no run-level `join`. Anything outside the T4–T10 route tables 404s and needs a contract-suite justification citing a call site to be added.

---

## 2. Ordered tasks

### T1 — Dependencies, package skeleton, schema, lifespan, typecheck widening, D5 convention

**Files:** `pyproject.toml`, `Makefile`, `agent_runtime/{__init__.py,app.py,config.py,db.py,schema.sql}`, `tests/agent_runtime/conftest.py`.

- **Deps:** add `langgraph-checkpoint-postgres` (pin the exact current release after reading its changelog — it is *not* installed today; verified zero hits in `uv.lock`) and `apscheduler>=3.10,<4` (**the pin is load-bearing**: 4.x removes `AsyncIOScheduler` and `CronTrigger.from_crontab`; APScheduler is not installed today, so Phase 1 chooses the major version — folded finding). `psycopg[binary,pool]` arrives transitively with the checkpoint package; make it explicit anyway.
- **Skeleton:** `agent_runtime/app.py` — FastAPI app factory + module-level `app`; lifespan opens one `AsyncConnectionPool` (`db.py`), runs `AsyncPostgresSaver.setup()` / `AsyncPostgresStore.setup()`, executes `schema.sql` (idempotent `CREATE TABLE IF NOT EXISTS`), runs the startup sweep (T6), starts the cron scheduler (T10), and stores `saver`/`store`/`registry`/`executor` on `app.state`. `config.py` reads `DATABASE_URL` and `AGENT_RUNTIME_CONFIG`.
- **Schema (`schema.sql`, D4 names — DDL is the contract for Phase 3's sweep SQL):**

```sql
CREATE TABLE IF NOT EXISTS rt_thread (
  thread_id  UUID PRIMARY KEY,
  status     TEXT NOT NULL DEFAULT 'idle'
             CHECK (status IN ('idle','busy','interrupted','error')),
  metadata   JSONB NOT NULL DEFAULT '{}'::jsonb,
  "values"   JSONB,                                   -- latest checkpoint values, updated at run-terminal
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rt_thread_status_idx   ON rt_thread (status);
CREATE INDEX IF NOT EXISTS rt_thread_metadata_idx ON rt_thread USING gin (metadata jsonb_path_ops);

CREATE TABLE IF NOT EXISTS rt_run (
  run_id             UUID PRIMARY KEY,
  thread_id          UUID NOT NULL REFERENCES rt_thread(thread_id) ON DELETE CASCADE,
  assistant_id       TEXT NOT NULL,
  status             TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','running','error','success','timeout','interrupted')),
  multitask_strategy TEXT NOT NULL DEFAULT 'interrupt',
  kwargs             JSONB NOT NULL DEFAULT '{}'::jsonb,  -- input, config, webhook, durability, stream_mode, stream_resumable
  metadata           JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS rt_run_thread_status_idx ON rt_run (thread_id, status);

CREATE TABLE IF NOT EXISTS rt_cron (
  cron_id      UUID PRIMARY KEY,
  assistant_id TEXT NOT NULL,
  thread_id    UUID REFERENCES rt_thread(thread_id) ON DELETE CASCADE,  -- NULL = schedule cron (fresh thread per fire)
  schedule     TEXT NOT NULL,                                           -- 5-field crontab
  timezone     TEXT NOT NULL DEFAULT 'UTC',
  end_time     TIMESTAMPTZ,
  payload      JSONB NOT NULL DEFAULT '{}'::jsonb,                      -- input, config
  metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,
  next_run_date TIMESTAMPTZ,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rt_thread_event (
  run_id     UUID NOT NULL REFERENCES rt_run(run_id) ON DELETE CASCADE,
  thread_id  UUID NOT NULL,
  seq        BIGINT NOT NULL,           -- per-run monotonic, assigned by the executor
  event_id   TEXT NOT NULL,             -- the SSE `id:` value; format pinned by the Phase 0 goldens
  event      TEXT NOT NULL,             -- SSE event name
  data       TEXT NOT NULL,             -- SSE data payload, verbatim
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, seq)
);
CREATE INDEX IF NOT EXISTS rt_thread_event_thread_idx ON rt_thread_event (thread_id, run_id, seq);
```

- **Typecheck widening (folds the basedpyright finding):** `pyproject.toml:88` `include = ["agent", "tests"]` → `["agent", "agent_runtime", "tests"]`; `Makefile:57` `npx --yes basedpyright agent tests` → `... agent agent_runtime tests`. Without both edits, T13's "typecheck covers agent_runtime" would be vacuous. Phase 3 T8 shrinks to any `scripts/` remainder.
- **D5 convention:** `tests/agent_runtime/conftest.py` — session autouse fixture: resolve DSN via `tests/support/postgres.py` (Phase 0); on failure (`TEST_POSTGRES_DSN` unset and Docker absent) `pytest.skip("agent_runtime tests require Postgres", allow_module_level=True)`-equivalent at session scope; otherwise session-scoped schema creation + per-test `TRUNCATE rt_thread, rt_run, rt_cron, rt_thread_event CASCADE` and store/checkpoint table truncation.

**Verify:** `uv sync --extra dev` clean; `docker compose -f docker-compose.test.yml up -d && uv run pytest tests/agent_runtime/` (empty pass); `docker compose ... down && uv run pytest tests/agent_runtime/` → all SKIPPED, exit 0; `make typecheck` runs over `agent_runtime/`.

### T2 — Graph registry + test graphs

**Files:** `agent_runtime/registry.py`, `tests/agent_runtime/testgraphs.py`, `tests/agent_runtime/runtime.test.json`.

- `registry.py`: parse the `AGENT_RUNTIME_CONFIG` file's `graphs` mapping (`langgraph.json` shape — today: `agent`, `reviewer`, `analyzer`, `chat`, `scheduler` under `agent.graphs.*`) and `http.app` (D1 mount target; default `agent.webapp:app`). `resolve(assistant_id, config) -> Pregel`: import `module:attr` lazily; if the attr is callable and not a Pregel, treat it as a factory and call it with the run's `RunnableConfig` (awaiting if coroutine) — this is exactly the real app's shape (`agent/server.py:817` `async def get_agent(config) -> Pregel`, wrapped at `:1065`). Checkpointer injection happens in the executor via `config["configurable"][CONFIG_KEY_CHECKPOINTER]`, mirroring `langgraph-api` itself (`langgraph_api/graph.py:381-384`); MIT `langgraph` honors that key in `Pregel._defaults` (`langgraph/pregel/main.py:2581-2582`). Store: the registry sets `compiled.store = app.state.store` on the resolved Pregel when the factory didn't attach one, so `langgraph.config.get_store()` (used by `check_message_queue.py:158`) resolves to the same instance the Store router serves — the T5 identity test pins this; if attribute assignment proves insufficient for factory-compiled deep agents, fall back to `CONFIG_KEY_STORE` injection (verified during T6, not assumed).
- `testgraphs.py` (registered via `runtime.test.json`, which the tests point `AGENT_RUNTIME_CONFIG` at): **`echo`** — two nodes, no model, appends to a `steps` list (basic run lifecycle); **`slow_busy`** — N sequential steps, each `await asyncio.sleep(input-controlled delay)` then checkpoint (deterministic busy window for interrupt-arbitration and cancel tests); **`interrupting`** — calls `interrupt()` mid-graph and resumes via `Command(resume=...)` (drives `/commands` and state/history); **`model_call`** — one node that invokes a chat model bound at graph-build time (only the LiteLLM smoke uses it; folds the "smoke test must not need the real agent factory" finding).

**Verify:** `uv run pytest tests/agent_runtime/test_registry.py` — resolves compiled graphs and factories from both `runtime.test.json` and the real `langgraph.json` (import-only for the real one: asserts the five entrypoints import and are callable/Pregel, without invoking `get_agent`).

### T3 — Wire models

**Files:** `agent_runtime/models.py`.

Pydantic models for Thread, Run, Cron, StoreItem, and every request body, with literals matching `langgraph_sdk/schema.py`: `RunStatus` (`:23`), `ThreadStatus` (`:34`), `MultitaskStrategy` (`:81`), `OnConflictBehavior` (`:90`), `IfNotExists` (`:123`), `CancelAction` (`:137`). Response field names/shapes come from the Phase 0 golden transcripts (e.g. threads carry `thread_id, created_at, updated_at, metadata, status, values`; runs carry `run_id, thread_id, assistant_id, created_at, updated_at, metadata, status, kwargs, multitask_strategy`).

**Verify:** `test_wire_models.py` — round-trip the golden JSON fixtures through the models without field loss (catches snake_case/shape drift before any router exists).

### T4 — Threads router: CRUD, search (sort/select/`if_exists`), state, history

**Files:** `agent_runtime/routers/threads.py`, `agent_runtime/threads_repo.py`, tests `tests/agent_runtime/test_threads_api.py`.

Route table (every row has a cited caller; SDK wire paths verified in `langgraph_sdk/_async/threads.py`):

| Route | Behavior | Caller |
|---|---|---|
| `POST /threads` (`threads.py:175`) | create; body `{thread_id?, metadata?, if_exists?}` (`threads.py:152-153`). **`if_exists="do_nothing"`: second create of the same `thread_id` returns the existing row 2xx, metadata untouched** — semantics pinned by the Phase 0 golden (`test_thread_create_if_exists_do_nothing`). Default `raise` → 409. | 9 verified sites: `agent/webhooks/common.py:649,717,832,892`, `agent/webhooks/github.py:676`, `agent/dashboard/thread_api.py:1011`, `schedules.py:442`, `review_chat_api.py:302`, `agent/tools/slack_start_new_thread.py:239` (folds the `if_exists` finding) |
| `GET /threads/{id}` (`threads.py:96`) | fetch | 30 `threads.get` sites incl. `check_message_queue.py:40` |
| `PATCH /threads/{id}` (`threads.py:260`) | merge-update `metadata` (JSONB `||` merge; merge depth pinned by golden) | 30 `threads.update` sites (sandbox ids, PR links, failure flags) |
| `DELETE /threads/{id}` (`threads.py:294`) | delete row (cascades `rt_run`/`rt_thread_event`) + `await saver.adelete_thread(thread_id)` + store cleanup is **not** implied (store is namespace-keyed app data) | `thread_ops` delete paths |
| `POST /threads/search` (`threads.py:372`) | `metadata` filter → JSONB containment `@>` incl. nested keys; `status`; `limit`/`offset`; **`sort_by` ∈ {`thread_id`,`status`,`created_at`,`updated_at`} + `sort_order` ∈ {`asc`,`desc`} → `ORDER BY`; `select=[...]` → column projection** | metadata/status/pagination: `agent/reconcile.py:58-63`; sort/select (folds the major finding): `thread_api.py:549-551` with `select=_THREAD_LIST_SELECT` (`:506`), `agent_usage.py:498-499` (`sort_by="created_at"`), `review_api.py:296-297`, `review_chat_api.py:91-92` |
| `GET /threads/{id}/state` (`threads.py:616`) + `POST /threads/{id}/history` (`threads.py:733`) | thin wrappers over MIT `graph.aget_state` / `aget_state_history` (MIGRATION §5.1 item 2): resolve the graph via the registry from the latest `rt_run.assistant_id` (default `agent`), inject the checkpointer, serialize the `StateSnapshot` per golden | `threads.get_state`: `thread_api.py:1524`; raw `/history` proxy: `thread_api.py:1961`; `review_chat_api.py` state/history proxies |

`threads_repo.py` owns all SQL and the single **`recompute_status(thread_id)`** function: `busy` if any `rt_run` in (`pending`,`running`); else `interrupted` if the latest run is `interrupted` or the latest checkpoint holds a pending interrupt; else `error` if the latest run errored; else `idle`. Status transitions happen nowhere else — this is what `reconcile.py`'s `status="busy"` search and the dashboard busy pill depend on, and the contract goldens pin it.

**Verify:** `uv run pytest -vvv tests/agent_runtime/test_threads_api.py` — includes `test_create_if_exists_do_nothing_idempotent`, `test_search_sort_by_updated_at_desc`, `test_search_sort_by_created_at`, `test_search_select_subset`, `test_search_nested_metadata_filter`, `test_search_status_busy`, `test_get_state_thin_wrapper_roundtrip` (run the `echo` graph, then `GET /state` returns the final values with checkpoint ids).

### T5 — Store router + store-identity tests

**Files:** `agent_runtime/routers/store.py`, tests `tests/agent_runtime/test_store_api.py`.

Wire paths from `langgraph_sdk/_async/store.py`: `PUT /store/items` (`:84`), `GET /store/items` (`:142`), `DELETE /store/items` (`:174`), `POST /store/items/search` (`:250`, with `filter=` metadata queries incl. nested keys — `schedules.py:186`, `profiles.py:363` — plus `namespace_prefix`/`limit`/`offset`). All four delegate to the **one** `app.state.store` (`AsyncPostgresStore`) — the same instance the registry attaches to graphs (D1/T2). `/store/namespaces` is omitted (no app caller, D6).

**Verify:** `test_store_api.py` — CRUD + nested-filter search vs golden; **`test_http_write_visible_to_inprocess_get_store`**: PUT an item over HTTP, run a graph whose node reads it via `langgraph.config.get_store()`, and the reverse (node writes → HTTP GET sees it). This is MIGRATION §1's consistency constraint as an executable invariant.

### T6 — Run executor: arbitration, event log, startup sweep

**Files:** `agent_runtime/executor.py`, `agent_runtime/runs_repo.py`, tests `tests/agent_runtime/test_executor.py`.

The heart of the runtime. `RunExecutor` (held on `app.state`) owns `_tasks: dict[run_id, asyncio.Task]` and per-thread `asyncio.Lock`s (single process per D1, so per-thread arbitration is serialized by construction — no cross-process races).

1. **Create** (called from T7's router): insert `rt_run` (`pending`), auto-creating the thread when `if_not_exists="create"` (reject → 404). Under the thread lock, arbitrate: if another run on the thread is `pending`/`running` — `multitask_strategy="interrupt"` cancels the in-flight `asyncio.Task`, awaits its teardown (asyncio cancellation propagates; the `durability="sync"` checkpoint survives — MIGRATION §1), marks it `interrupted`, fires its webhook, then starts the new run; `reject` → 409; `enqueue`/`rollback` → 400 with a comment (no app caller sends them — D6). Then `asyncio.create_task(self._run(run))` and status → `running`.
2. **Invoke**: `graph = await registry.resolve(run.assistant_id, config)`; inject `config["configurable"][CONFIG_KEY_CHECKPOINTER] = saver` (the `langgraph_api/graph.py:381-384` mechanism, honored by `pregel/main.py:2581-2582`); set `configurable.thread_id`; forward `durability` from `rt_run.kwargs` as the plain `astream` kwarg.
3. **Stream loop** (folds the tuple-order finding): **always pass `stream_mode` as a `list`** and iterate `async for ns, mode, payload in graph.astream(input, config, stream_mode=[...], subgraphs=True, durability=...)`. MIT `langgraph` 1.x yields `(ns, mode, payload)` for `subgraphs=True` + list mode (`langgraph/pregel/main.py:4237`) but collapses to `(ns, payload)` for a bare-string mode (`:4241`) and `(mode, payload)` without subgraphs (`:4239`) — the list-always rule forces the 3-tuple shape so namespace and mode can never be silently swapped.
4. **Event log**: each yielded item passes through T8's normalizers into zero-or-more SSE events; assign the per-run `seq`, format `event_id` per the golden transcripts, `INSERT INTO rt_thread_event`, and fan out to live subscriber queues (`asyncio.Queue` per attached connection). Replay-then-tail (T8) reads the table first, then the queue — MIGRATION §3's resumable-streaming design.
5. **Terminal**: update `rt_run.status` (`success` / `error` on exception / `interrupted` on cancellation / `timeout` if a configured ceiling fires), update `rt_thread."values"` from the final checkpoint, `recompute_status`, and **fire the completion webhook for every terminal state** (locked decision) via T7's sender.
6. **Startup sweep** (in `app.py` lifespan, before serving): `UPDATE rt_run SET status='error', updated_at=now() WHERE status IN ('pending','running') RETURNING run_id, thread_id, kwargs, ...`; for each row `recompute_status` and fire the completion webhook — per **D2** the `error` status makes `agent/completion.py` post the failure reply (`_TERMINAL_FAILURE_STATUSES`, `completion.py:35`), so a crash never leaves the user in silence.

**Verify:** `test_executor.py` — `test_run_success_terminal_and_values`, `test_if_not_exists_create_and_404`, `test_interrupt_arbitration_cancels_inflight` (double-dispatch on `slow_busy`; first run ends `interrupted`, second completes; both webhooks fire), `test_interrupt_preserves_checkpoint` (resume sees the pre-interrupt steps), `test_startup_sweep_marks_orphans_error` (seed a `running` row, boot the app, assert `error` + webhook captured), `test_cancel_interrupt_stays_interrupted_and_receiver_ignores_it` (cancel path stays `interrupted`; asserts `agent/completion.py`'s receiver returns `ignored` for it — pins both sides of the D2 split).

### T7 — Runs router + completion-webhook sender

**Files:** `agent_runtime/routers/runs.py`, `agent_runtime/webhooks.py`, tests `tests/agent_runtime/test_runs_api.py` + a local capture receiver fixture.

| Route | Behavior | Caller |
|---|---|---|
| `POST /threads/{id}/runs` | create background run; body: `input`, `config`, `metadata?`, `multitask_strategy`, `durability`, `if_not_exists`, `webhook`, `stream_mode?`, `stream_resumable?` — exactly `create_durable_run`'s kwargs (`agent/dispatch.py:124-136`); returns the run dict | `dispatch.py:138` (sole `runs.create` site) |
| `GET /threads/{id}/runs?status=` (`runs.py:901`) | list | `reconcile.py:76`, `thread_api.py`, `review_style_jobs.py` |
| `GET /threads/{id}/runs/{run_id}` (`runs.py:938`) | get | `review_style_jobs.py` |
| `GET`+`POST /threads/{id}/runs/{run_id}/cancel` (`runs.py:988,995`) | query params `wait`, `action` (default `interrupt`); cancels via the executor | `thread_api.py:1403,1449` (`wait=False`), raw proxy `thread_api.py:1979` |
| `POST /runs/cancel` | cancel_many. **`action` is a query parameter, not a body field** — the SDK puts only `thread_id`/`run_ids`/`status` in the JSON body and sends `params={"action": action}` (`langgraph_sdk/_async/runs.py:1043-1058`); a body-read implementation would silently always use the default (folds the finding). `status="all"` supported (`thread_api.py:1423`). | `reconcile.py:96-100`, `thread_api.py:1423` |

No `DELETE` run route and no `rollback` semantics (D6). `webhooks.py`: `async def send_completion_webhook(run_row, status, values, exception)` — POST with payload matching `langgraph-api`'s shape: `{**run, "status": status, "run_started_at": ..., "run_ended_at": ..., "webhook_sent_at": now, "values": values}` plus `"error"` on failure (`langgraph_api/webhook.py:180-189`); the URL is taken verbatim from `rt_run.kwargs["webhook"]`, which already carries the `?token=<RUN_COMPLETE_WEBHOOK_SECRET>` auth appended by `dispatch.py:73-75` and verified by `completion.py:53-62` (`hmac.compare_digest`) — the runtime adds no signing of its own. 3 attempts with backoff; failures logged, never fatal to the run.

**Verify:** `test_runs_api.py` drives everything **through the real `langgraph_sdk` client** pointed at the app (so the query-param/body split can't be faked), including `test_cancel_many_action_query_param` and `test_webhook_fires_on_all_terminal_states` (success, error, interrupt, sweep) asserted at the capture receiver; the existing `tests/webhooks/test_completion_webhook.py` remains the receiver-side complement.

### T8 — Stream normalizers + SSE endpoints (resumable)

**Files:** `agent_runtime/streams.py`, `agent_runtime/routers/streaming.py`, tests `tests/agent_runtime/test_streaming.py`.

- `streams.py` — pure functions mapping the executor's `(ns, mode, payload)` items into wire SSE events for the seven dashboard modes (`_DASHBOARD_STREAM_MODES`, `thread_api.py:56-64`: `values, updates, messages, messages-tuple, tools, checkpoints, events`). `messages-tuple`, `tools`, and `events` are **not** MIT `StreamMode`s (MIGRATION §1) — they are synthesized here from `messages`/`updates`/`custom` payloads. **The Phase 0 golden transcripts are the spec**: event names, `id:` format, data envelopes, heartbeats are written to match `tests/contract/golden/`, not guessed. Pure functions → goldens replay as unit tests without a server.
- `routers/streaming.py`:
  - `GET /threads/{id}/stream` — SDK `threads.join_stream`; honors the **`Last-Event-ID` header** (`langgraph_sdk/_async/threads.py:823-826`): replay `rt_thread_event` rows after that id, then tail the live queue; no gap, no duplicate. Caller: `thread_api.py:2016`.
  - `POST /threads/{id}/stream/events` — the v2 dashboard endpoint (create-run-and-stream): accepts the dashboard payload (`thread_api.py:1285` sets `stream_mode` to all seven modes), creates the run via the executor, streams normalized events. Caller: raw proxy `thread_api.py:1835`, consumed by `@langchain/react` `useStream` in `ui/`.

**Verify:** `test_streaming.py` — golden-replay unit tests for the normalizers; `test_join_stream_resume_no_loss_no_dup` (attach, kill the connection mid-run on `slow_busy`, reconnect with `last_event_id`, diff the concatenation against an uninterrupted capture); `test_stream_events_wire_shape` (byte-diff against the Phase 0 golden after `normalize.py` placeholder substitution).

### T9 — Commands endpoint

**Files:** `agent_runtime/routers/commands.py`, tests in `test_streaming.py`.

`POST /threads/{id}/commands` — the v2 command protocol (resume-from-interrupt and friends). Callers: raw proxies `thread_api.py:1901` and `review_chat_api.py:471` (plan-approval / review-chat resume). Implementation: parse the command body per the Phase 0 golden, translate to `Command(resume=...)` (or state-update + continue) on the thread's graph, execute through the executor as a run (so arbitration, event log, and webhooks apply), stream the response in the same v2 envelope as T8. The request/response shape is pinned by the golden transcript captured from `langgraph dev` in Phase 0 task 7 — no shape is invented here.

**Verify:** `test_commands_resume_interrupting_graph` — run `interrupting`, hit the interrupt, POST the resume command, assert the graph completes and state/history show the resume; wire shape byte-diffed against the golden.

### T10 — Crons + APScheduler firing loop

**Files:** `agent_runtime/routers/crons.py`, `agent_runtime/cron_scheduler.py` (D4 name), tests `tests/agent_runtime/test_crons.py`.

Routes (wire paths from `langgraph_sdk/_async/cron.py`): `POST /runs/crons` (`:288` — schedule cron; callers `analyzer_cron.py:42`, `schedules.py:236`), `POST /threads/{id}/runs/crons` (`:169` — `create_for_thread` with `end_time` + `timezone`; caller `schedule_thread_wakeup.py:129-141`), `POST /runs/crons/search` (`:500`; caller `schedule_thread_wakeup.py:72`), `DELETE /runs/crons/{id}` (`:407`; callers `schedule_thread_wakeup.py:102`, `analyzer_cron.py:67`, `schedules.py:258`). `rt_cron` is the source of truth; the scheduler is a projection of it.

`cron_scheduler.py`: `AsyncIOScheduler` started in the lifespan; on boot, load all `rt_cron` rows and `add_job` each; on create/delete, insert/remove both row and job. **Trigger construction (folds the `from_crontab` finding):** APScheduler 3.x's `CronTrigger.from_crontab(expr, timezone)` accepts no `end_date` — `end_date` is a `CronTrigger` *constructor* parameter. So either parse the 5-field expression and build `CronTrigger(minute=..., hour=..., day=..., month=..., day_of_week=..., timezone=ZoneInfo(tz), end_date=end_time)` field-wise, or build via `from_crontab` and set `trigger.end_date` before `add_job` — either way `end_time` must actually stop re-fires, because the wakeup tool pads `end_time` ~90s past the fire and relies on it for one-shot semantics (`schedule_thread_wakeup.py:94-96,128-141`). `misfire_grace_time=60`: a just-missed tick fires on boot; older ones are skipped (matching the app's "cron is best-effort, purge cleans up" posture — `purge_expired_wakeup_crons`, `schedule_thread_wakeup.py:92-105`). Job body: thread-bound crons create a run on their thread via the executor (interrupt arbitration applies); schedule crons create a **fresh thread per fire** then run (platform semantics for `crons.create`, exercised by the `scheduler`/`analyzer` graphs).

**Verify:** `test_crons.py` — `test_create_for_thread_end_time_one_shot` (near-term schedule + `end_time` = fire once, never again; assert exactly one run), `test_schedule_cron_fresh_thread_per_fire`, `test_timezone_respected` (non-UTC tz maps to the right UTC instant), `test_search_and_delete_wire_shapes` vs golden, `test_boot_reloads_rt_cron_rows`.

### T11 — Contract parity iteration

**Files:** `tests/contract/conftest.py` (extend, Phase 0 artifact), divergence ledger in `tests/contract/golden/README.md`.

Add a `CONTRACT_RUNTIME=platform|embedded` switch to the Phase 0 contract harness: `platform` (default) boots `langgraph dev` exactly as Phase 0 shipped it; `embedded` boots `uvicorn agent_runtime.app:app` with `AGENT_RUNTIME_CONFIG=tests/contract/langgraph.contract.json` (the Phase 0 contract graph, now also registrable here) against the compose Postgres. Then iterate `agent_runtime` until `CONTRACT_RUNTIME=embedded uv run pytest -vvv tests/contract/` matches the goldens. Divergences are handled per MIGRATION §4.3's pre-production liberty: accidental → fix the runtime; deliberate (e.g. dev's inmem cron gaps recorded as skips in Phase 0) → entry in the ledger with rationale. **Baseline addendum:** if the Phase 0 goldens are missing any of the `sort_by`/`sort_order`/`select`/`if_exists="do_nothing"` pins (they are in Phase 0's acceptance, but verify), capture them against `langgraph dev` *first*, commit the golden, then match it — never author a golden from the new runtime's own output.

**Verify:** `CONTRACT_RUNTIME=embedded uv run pytest -vvv tests/contract/` green; `CONTRACT_RUNTIME=platform uv run pytest -vvv tests/contract/` still green (proves the suite wasn't bent to fit the new runtime).

### T12 — Real-agent-factory test (subprocess) + executable LiteLLM smoke

**Files:** `tests/agent_runtime/fake_boundary_app.py`, `tests/agent_runtime/test_real_agent_graph.py`, `tests/agent_runtime/test_litellm_smoke.py`, `tests/support/litellm.py` (extend, Phase 0 artifact).

- **`test_real_agent_factory_via_registry`** — proves the runtime can execute the *real* `agent` deep-agent factory (`agent/server.py:817`), not just test graphs. **Binding constraint (folds both major e2e-import findings):** Phase 0's ledger bans importing `tests/e2e/` modules into the pytest process (`tests/e2e/patches.py:18` does a module-level `import e2e_env` that mutates `os.environ` process-wide, and `patches.apply()` irreversibly rebinds `agent.server.make_model` and the OAuth token accessors with a permanent `_applied` guard). Sanctioned alternative (1) — **subprocess** — is used: `fake_boundary_app.py` is a small boot module that, *in its own interpreter*, inserts `tests/e2e` on `sys.path`, calls `patches.apply()` (fake model, fake tokens, `SANDBOX_TYPE=local`), and serves `agent_runtime.app:app` with the real `langgraph.json` registry. The pytest test launches it with `subprocess.Popen([sys.executable, "-m", "uvicorn", ...])` — the same isolation idea as `tests/agent/test_import_hygiene.py:13-21`'s `_closure_check` — waits for readiness, drives one scripted run over HTTP via `langgraph_sdk`, asserts terminal `success` + streamed `messages` events + `get_state` shows the scripted tool call, and tears the process down. The pytest process itself imports nothing from `tests/e2e/`.
- **`test_litellm_smoke`** — the one real-model test, made actually executable (folds the env-gating finding): `tests/support/litellm.py` is extended to load the repo-root `.env` via `python-dotenv` (already installed transitively) **before** the skip gate evaluates, because nothing else loads `.env` into pytest (pyproject's `[tool.pytest.ini_options]` has only `asyncio_mode`/`testpaths`; `langgraph.json`'s `"env": ".env"` is dev-server-only) — and `.env:1-4` does set `LITELLM_BASE_URL`/`LITELLM_API_KEY`/`LITELLM_MODEL=minimax-m3`, so on this machine the gate fires and the test **runs**. Marked `@pytest.mark.litellm`. It targets the dedicated one-node `model_call` test graph (T2) bound to `ChatOpenAI(base_url=LITELLM_BASE_URL, api_key=LITELLM_API_KEY, model=LITELLM_MODEL)` — **not** the real agent factory (folds the underspecification finding: no sandbox/auth/settings stubs needed, and flaky model output can't fail it). Asserts: terminal `success`, ≥1 `messages` event with multi-chunk streaming through the T8 normalizers, event ids monotonic. Never calls a paid cloud API.

**Verify:** `uv run pytest -vvv tests/agent_runtime/test_real_agent_graph.py` (subprocess boots, run completes); `uv run pytest -vvv -m litellm tests/agent_runtime/test_litellm_smoke.py` — **runs** on this machine (env in `.env`); on machines without the proxy it reports a visible SKIPPED line, never silence.

### T13 — Phase gate + commit

Run the full §5 acceptance block. The typecheck claim is real this time because T1 widened both `pyproject.toml:88`'s `include` and `Makefile:57`'s argument list (folded finding — Phase 3 T8 keeps only the `scripts/` remainder). Existing 136 test files are untouched — Phase 1 is additive; behavior parity is proven by the contract suite (T11), not by assertion. Commit on `feat/fastapi-runtime`: `feat: agent_runtime — self-hosted MIT runtime (phase 1)`.

---

## 3. Test rationale — what each pins and why it's load-bearing

- **`test_threads_api.py` sort/select/`if_exists` cases (T4):** these are the operations that fail *silently* — wrong dashboard ordering, missing fields, 409s on every webhook retrigger — nothing 4xxes without them (the two cross-phase major findings). The 9 `if_exists` call sites and 4 sort/select call sites are cited in the route table.
- **`test_http_write_visible_to_inprocess_get_store` (T5):** MIGRATION §1's consistency constraint as an invariant; the single highest-risk architectural assumption (one store instance) gets one unambiguous test.
- **Executor tests (T6):** interrupt arbitration is the hardest platform semantic being reimplemented (MIGRATION §7); the checkpoint-survives-cancel test is the floor Phase 2's chaos suite builds on; the paired sweep tests (`orphans → error`, `cancel → interrupted` ignored by the receiver) pin both sides of D2 so neither regression direction is silent.
- **SDK-client-driven runs tests (T7):** driving through `langgraph_sdk` (not raw httpx) makes wire-shape mistakes — like reading `cancel_many`'s `action` from the body — fail loudly.
- **Golden-replay normalizer tests (T8/T9):** the v2 wire protocol is consumed byte-for-byte by `@langchain/react` in the browser; goldens are the only spec that exists. Pure-function normalizers make the replay a unit test.
- **`test_create_for_thread_end_time_one_shot` (T10):** the wakeup tool's one-shot semantics depend entirely on `end_time` stopping re-fires (`schedule_thread_wakeup.py:94-96`) — the exact thing the `from_crontab` API misuse would have broken.
- **T12's two tests:** the real-factory test is the only pre-Phase-2 proof the runtime runs the actual deep agent; the LiteLLM smoke is the only real-model streaming test — both isolated so they can't contaminate or flake the deterministic suite.
- **Deliberately not added:** duplicate reconcile-internals tests (pinned in `tests/reviewer/test_reconcile_sweep.py`), duplicate queue-drain tests (`tests/middleware/test_check_message_queue.py`), receiver-side webhook tests (`tests/webhooks/test_completion_webhook.py` already covers them), and any new bare-`get_client` guard mechanisms (Phase 0 shipped the test guard; Phase 3 owns the durable lint — the consolidation finding's split, respected here by adding nothing).

## 4. Fixtures and fakes inventory (reuse, don't reinvent — but never in-process from `tests/e2e/`)

| Existing artifact | Location | Reused by |
|---|---|---|
| `FakeScriptedChatModel` + boundary patches | `tests/e2e/fake_llm.py`, `tests/e2e/patches.py` | T12 — **subprocess only**, via `fake_boundary_app.py`; never imported into the pytest process |
| Postgres compose + DSN fixture | `docker-compose.test.yml`, `tests/support/postgres.py` (Phase 0) | every `tests/agent_runtime/` module via its conftest (D5) |
| Golden SSE transcripts + contract module + `normalize.py` | `tests/contract/` (Phase 0) | T8/T9 normalizer replay tests; T11 parity gate |
| `.env` loader for LiteLLM env | `tests/support/litellm.py` (Phase 0) | T12 smoke — extended to load repo-root `.env` via python-dotenv before the skip gate |
| Completion-webhook receiver tests | `tests/webhooks/test_completion_webhook.py` | T7 (producer-side complement; untouched) |
| Autouse `is_review_repo_enabled` stub | `tests/conftest.py` | inherited, harmless |

New fakes are limited to: the four test graphs (`tests/agent_runtime/testgraphs.py` + `runtime.test.json`, T2), the local webhook-capture receiver fixture (T7), and the subprocess boot module `tests/agent_runtime/fake_boundary_app.py` (T12). Nothing else.

## 5. Acceptance criteria (all must pass)

```bash
uv sync --extra dev
docker compose -f docker-compose.test.yml up -d
make lint && make typecheck                                  # typecheck now includes agent_runtime/ (T1)
uv run pytest -vvv tests/                                    # existing suite untouched
uv run pytest -vvv tests/agent_runtime/                      # new runtime suite green
uv run pytest -vvv -m litellm tests/agent_runtime/test_litellm_smoke.py  # RUNS (env in .env), or visible SKIPPED
CONTRACT_RUNTIME=embedded uv run pytest -vvv tests/contract/ # parity vs Phase 0 golden baseline
CONTRACT_RUNTIME=platform uv run pytest -vvv tests/contract/ # baseline still green (suite not bent to fit)
DATABASE_URL=... uv run uvicorn agent_runtime.app:app --port 2024   # boots; /docs serves; a webapp
                                                             # route (e.g. /me) reachable via the D1 mount
docker compose -f docker-compose.test.yml down
uv run pytest -vvv tests/                                    # Docker stopped: tests/agent_runtime/ SKIPs cleanly (D5), exit 0
```

Plus: the D4 names exist verbatim (`psql`: `\dt rt_*` shows the four tables; `python -c "import agent_runtime.app, agent_runtime.cron_scheduler"` succeeds — the import Phase 3's §0 pre-flight will run); no route outside the T4–T10 tables answers anything but 404; the e2e suite under `RUNTIME=embedded` is explicitly **not** a Phase 1 gate (Phase 2's hard gate).

## 6. Risks and mitigations

- **Wire-shape drift on the v2 protocol** (`/stream/events`, `/commands`) breaking `@langchain/react` in ways transcript diffs tolerate. Mitigation: golden transcripts are the spec (T8/T9/T11); pin `@langchain/langgraph-sdk`/`@langchain/react` versions in `ui/package.json` for the migration (MIGRATION §7); Phase 2's dashboard spec is the backstop.
- **Interrupt-arbitration edge cases** (cancel landing mid-sandbox-command or mid-Slack-status). Mitigation: `durability="sync"` checkpoint floor pinned by `test_interrupt_preserves_checkpoint`; the double-dispatch contract test and Phase 2's `slack_debounce.spec.ts` exercise the real path; arbitration is serialized per thread in one process (D1), eliminating cross-process races by construction.
- **Silent user-facing regression on crash-swept runs.** Mitigated by design (D2): orphans land in `error` so `handle_run_completion` posts the failure reply; the paired T6 tests pin both sides of the `interrupted`-vs-`error` split; Phase 2's chaos test asserts the reply actually fires.
- **Thread-status semantics divergence** (what counts as `busy`, when `interrupted` shows) silently breaking `reconcile.py` and the dashboard busy pill. Mitigation: `recompute_status` is a single function in `threads_repo`; contract goldens pin the transitions.
- **`langgraph-checkpoint-postgres` API mismatch at the untested seam** (never run in this repo; `.setup()`, pool sharing, `adelete_thread` may differ by version). Mitigation: pin the exact version at T1 after reading its changelog; `test_get_state_thin_wrapper_roundtrip` and the T5 identity test fail immediately on any wiring error.
- **APScheduler API/version hazards.** Mitigated at T1 (pin `>=3.10,<4` — 4.x removes `AsyncIOScheduler`/`from_crontab`) and T10 (`end_date` attached by field-wise construction or trigger mutation, since `from_crontab` can't take it); `test_create_for_thread_end_time_one_shot` is the behavioral pin; `misfire_grace_time=60` matches the app's best-effort cron posture.
- **Event-log write amplification** (one row per stream event). Accepted for Phase 1 — MIGRATION §3 pre-commits to Postgres-only replay-then-tail; revisit only on *measured* latency; pruning of `rt_thread_event` rides Phase 3's sweep job, written against the D4 names.
- **Test-process contamination from e2e fakes.** Mitigated by the T12 subprocess boundary; the rule stands: unit/contract tests never import `tests/e2e/` modules in-process.
- **Scope creep toward the full Platform API.** Mitigation: D6 — routers implement only the T4–T10 route tables (the unused run-DELETE route and `rollback` were cut under this rule); anything else 404s and needs a contract-suite justification citing an app call site.

## 7. Effort summary

| Task | What | Effort |
|---|---|---|
| T1 | deps (checkpoint-postgres + APScheduler pins), skeleton, schema DDL, lifespan, D1 mount, typecheck widening, D5 convention | M |
| T2 | graph registry (config file, factories, store attachment) + 4 test graphs | M |
| T3 | wire models | S |
| T4 | threads router (incl. `if_exists`, sort/select) + state/history wrappers | M |
| T5 | store router + identity tests | M |
| T6 | run executor + event log + startup sweep | **L** |
| T7 | runs router + webhook sender | M |
| T8 | normalizers + resumable SSE endpoints | **L** |
| T9 | commands endpoint | M |
| T10 | crons + APScheduler firing loop | M |
| T11 | contract parity iteration (+ baseline addendum if pins missing) | **L** |
| T12 | real-agent subprocess test + executable LiteLLM smoke | M |
| T13 | phase gate + commit | S |

Critical path: T1 → T2 → T6 → T8 → T11. T4/T5 parallel with T6 once T1–T3 land; T10 independent after T6. Matches MIGRATION §6: "the bulk of the work if building."

---

## 8. Completion record (2026-07-17)

All thirteen tasks delivered on `feat/fastapi-runtime`. Final state:
`agent_runtime/` (app, registry, executor, streams, serializers, 6 routers,
cron_scheduler, repos, schema) + `tests/agent_runtime/` (29 tests).
Gates: contract parity `CONTRACT_RUNTIME=embedded` 27 passed + 2 ledgered
skips vs the Phase 0 goldens, `platform` still 28 passed (suite not bent);
full default suite 1584 passed (hermetic, Docker-free); lint + basedpyright
0 errors with `agent_runtime` included; real-agent subprocess test green
(the actual `traced_agent` deep-agent factory, e2e boundary patches, webapp
mounted per D1); LiteLLM smoke streams a real model through the executor.

**Deviations and discoveries, recorded:**

- **Checkpointer attachment, not config-key injection.** Plan said inject
  `CONFIG_KEY_CHECKPOINTER` per run (the langgraph-api mechanism). Measured:
  that path breaks MIT langgraph's `DeltaChannel` write-replay in
  `aget_state` — deepagents' `messages` channel reads back EMPTY while
  execution works (minimal repro: create_deep_agent + AsyncPostgresSaver;
  config-key → state 0 messages, compiled-in → correct). The registry now
  attaches the saver as `graph.checkpointer` (same pattern as the store),
  and the fallback note in T2 ("verified during T6, not assumed") is hereby
  resolved in favor of attribute attachment for BOTH checkpointer and store.
- **State reads resolve factories with the latest run's stored configurable**
  (executor `_state_graph`) — a bare thread_id config resolves `get_agent`'s
  no-execution stub, whose channel mapping returns empty state. CM factories
  (the tracing wrappers) are entered per read and exited after.
- **Event log split ordering** (extends T1's DDL): `id BIGSERIAL` is the
  global replay order (SDK streams, `Last-Event-ID`); `seq` is nullable and
  assigned only to v2-channel events — dev's v2 seqs are contiguous while
  the SDK stream also carries non-v2 modes. Phase 3's sweep SQL targets the
  same table name; the PK is now `id`.
- **Worker-pickup kwargs staging**: dev stamps `__pregel_node_finished`, env
  versions, and `run_attempt` into stored run kwargs when the worker starts,
  not at create (run_create.json vs run_complete_webhook_payload.json pin
  the split); the executor mirrors this in `_enrich_kwargs_on_start`.
- **`runs.join` on `/commands`-started runs returns the v2 envelope** (dev
  quirk, ledger item 4) — preserved via an internal `__transport__` kwargs
  marker, stripped from every wire serialization.
- **Run-level `stream_mode="tools"` 422s on SDK endpoints but passes via
  `/commands`** (dev behavior); `stream_mode="events"` is accepted-and-
  ignored (no app caller, D6) — the per-mode contract golden skips it on
  embedded, ledger item 6.
- **Assistant ids**: wire responses carry `uuid5(NAMESPACE_DNS, graph_name)`
  (dev mints system-assistant uuids deterministically); the graph NAME rides
  in the enriched config's `graph_id` and both forms are accepted on input.
- **Single-process assumption (D1) is real**: per-thread arbitration locks,
  the in-process `_thread_seq` counters, and subscriber queues all assume
  one uvicorn worker — exactly the D1 topology; `--workers > 1` is not
  supported and Phase 2 must not introduce it. A Postgres advisory lock in
  the lifespan (`pg_try_advisory_lock(hashtext('agent_runtime'))`) fails a
  second worker's boot loudly instead of letting it corrupt arbitration.

**Adversarial review (15 findings) — all addressed.** Two proven blockers:
(1) every cancel/arbitration double-finalized (duplicate webhooks +
lifecycle rows) — fixed by making the terminal transition a conditional
`UPDATE … WHERE status IN ('pending','running')` that gates all emission
(exactly-once by construction), with a regression test pinning one webhook
+ one lifecycle row per cancel; (2) v2 `seq` restarted at 1 after a process
restart, breaking `since` resume — fixed by seeding the counter from
`MAX(seq)` on a thread's first emit, pinned by a fresh-executor restart
test. One major wire corruption: `Interrupt` dataclasses serialized as repr
strings — fixed with a dataclass branch in `_dump`; the interrupt test now
asserts the structured `{"value", "id"}` shape. Also fixed: the
insert-before-lock resurrection window (`start_if_pending`), fire-and-forget
webhook tasks losing their only strong reference, the multi-worker guard
above, unbounded cancel teardown while holding the thread lock (15s bound),
silent finalize failures (now logged), `DELETE /threads/{id}` cancelling
in-flight runs first, unknown assistant ids rejected 404 at create, cron
`next_run_date` persisted after each fire + just-missed ticks firing on
boot, subscriber-map husk cleanup, and the vacuous test assertions the
review called out (exact webhook counts, exact "ignored" receiver status).
Deferred with rationale: expired-cron purge (Phase 3's sweep job),
subscriber queue bounds and lock-map eviction (bounded by thread count,
pre-production), client-disconnect limbo rows (covered by the boot sweep;
revisit under Phase 2 chaos testing). The D6 route audit and the
`cancel_many`-without-thread_id / `ThreadSearchBody.values` notes are
recorded in the divergence ledger (item 6).

**Findings disposition.** All findings tagged `phase-1` or `cross-phase` were verified against code and **folded** — none refuted: T12 e2e-import ban (both majors → subprocess via `fake_boundary_app.py`); no-Docker convention (→ D5); `sort_by`/`sort_order`/`select` (→ T4; verified `thread_api.py:506,549-551`, `agent_usage.py:498-499`, `review_api.py:296-297`, `review_chat_api.py:91-92`); `if_exists="do_nothing"` (→ T4; 9 verified sites); startup-sweep-vs-completion-receiver (→ D2; `completion.py:35,155-156`); astream tuple order (→ T6; `pregel/main.py:4237,4239,4241` — `(ns, mode, payload)` only when `stream_mode` is a list); `CronTrigger.from_crontab`/APScheduler pin (→ T1/T10); LiteLLM smoke env gating (→ T12; dotenv load, `.env:1-4` verified) and underspecification (→ T2's `model_call` graph); `cancel_many` action-as-query-param (→ T7; `langgraph_sdk/_async/runs.py:1043-1058`); basedpyright coverage (→ T1; `pyproject.toml:88`, `Makefile:57`); DELETE-run-route/rollback scope creep (both → D6, cut); cross-phase naming (→ D4; `rt_*` tables + `cron_scheduler.py` authoritative — Phase 3's draft SQL/imports must be rewritten against them); guard-mechanism consolidation (→ Phase 1 adds no guard mechanisms; noted in §3). Phase-0/2/3-tagged findings are owned by those documents. One deliberate cross-document correction is recorded in D2: phase-2.md's header wording (`status='interrupted'`) is superseded by the sweep-to-`error` design its own T3 defers to.
