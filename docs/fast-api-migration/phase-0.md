# Phase 0 — Final Implementation Plan: Test Hardening + Boundary Cleanup

**Branch:** `feat/fastapi-runtime` (current). **Source of truth:** `docs/MIGRATION.md` §4.2–4.3, §5 Phase 0. **No behavior changes** except the two sanctioned boundary cleanups (tasks 2–3).

---

## 0. Objective and non-goals

**Objective.** Before any `agent_runtime/` code exists, (a) close the two graph-execution coverage gaps (compiled `agent` and `scheduler` graphs have zero unit-level invocation tests — MIGRATION §4.1), (b) capture `langgraph dev`'s behavior as a machine-checkable golden baseline (contract suite + golden SSE transcripts) so Phase 1 parity is a diff, not an opinion, (c) make the e2e suite runtime-pluggable, (d) stand up the reproducible Postgres test infrastructure Phase 1 will build on, and (e) perform the two mandatory boundary cleanups: route all 28 bare `get_client()` sites (which import Elastic-licensed `langgraph_api` in-process when `url=None` — MIGRATION §1) through the URL-resolving helpers, and delete the dead `after_seconds` parameter.

**Non-goals.** No `agent_runtime/` code. No Postgres-backed checkpointer wiring into the app (Phase 1). No Linear-signature or `get_thread_id_from_branch` tests (explicitly excluded, MIGRATION §5). No Redis, ever. No data migration/staging/rollback machinery (pre-production). No changes to `ui/` beyond pinning `@langchain/*` versions.

---

## 1. Cross-phase name ledger (binding on Phases 1–3)

Later phases have referenced Phase-0/1 artifacts by names that were never pinned (adversarial finding: Phase 3's verify command imports `agent_runtime.scheduler` and its sweep SQL targets `runs`/`run_events`, while Phase 1 defines `agent_runtime/cron_scheduler.py` and `rt_run`/`rt_thread_event`). Rule established here: **each phase's implementation names are authoritative the moment that phase's plan is final; downstream phase documents must be rewritten against them — "adjust paths if names differ" hedges are not acceptable in verify commands or SQL.** Names Phase 0 establishes, to be used verbatim later:

| Name | What it is |
|---|---|
| `contract` pytest marker + `addopts = "-m 'not contract'"` in `pyproject.toml` | **The canonical convention for every Postgres- or server-requiring test package.** Phase 1's `tests/agent_runtime/` must either reuse this marker or add its own marker to the same `addopts` exclusion, *and* its conftest must `pytest.skip` the package when `TEST_POSTGRES_DSN` is unset and Docker is absent — so `make test` with Docker stopped stays green (this was previously undefined and load-bearing in Phase 2's acceptance). |
| `make contract-test` | Boots dockerized deps + runs `pytest -m contract tests/contract/` |
| `tests/support/postgres.py`, env `TEST_POSTGRES_DSN` | Ephemeral-Postgres fixture + escape hatch |
| `RUNTIME=platform\|embedded` | Playwright webServer switch; `embedded` = `uvicorn agent_runtime.app:app` |
| `tests/agent/test_no_bare_get_client.py` | Guard Phase 3 upgrades to a ruff/CI rule |
| **tests/e2e import ban** | Unit/contract tests **never import from `tests/e2e/`** — see task 4a. Binding on all later phases, including Phase 1's real-agent-factory test (previously drafted to call `tests/e2e/patches.apply()` — rejected, see task 4a for why and for the two sanctioned alternatives). |

---

## 2. Ordered tasks

### Task 1 — Branch + baseline
Confirm `feat/fastapi-runtime` is current and record the green baseline before any change: `make test && make lint && make typecheck`. All subsequent tasks commit onto this branch; the phase-boundary commit is task 9.
**Verification:** three commands pass on the untouched tree.

### Task 2 — Delete `after_seconds` from `create_durable_run`
No caller passes it (verified: the only occurrences in `agent/` are the parameter and its forwarding).
**Files:** `agent/dispatch.py:121` (parameter), `:138-139` (conditional `create_kwargs["after_seconds"]`). Extend `tests/agent/test_dispatch.py` — its kwarg-recording fakes (`_FakeRuns`/`_FakeClient`, `tests/agent/test_dispatch.py:51-63`) already capture every `runs.create` kwarg — with an assertion that `after_seconds` is absent from the created-run kwargs and from `inspect.signature(create_durable_run)`.
**Verification:** `grep -rn "after_seconds" agent/dispatch.py` → no output; `make test`.

### Task 3 — Route the 28 bare `get_client()` sites through URL-resolving helpers
Bare `get_client()` (no `url=`) mounts `langgraph_api`'s in-process ASGI transport — a runtime import of the Elastic-licensed package (MIGRATION §1). Helpers already exist: `agent/utils/thread_ops.py:29 langgraph_client()` and `agent/dispatch.py:89 dispatch_client()`.

Verified inventory (28 sites): `agent/review/findings.py` ×3 (`:352,:406,:632`), `agent/dashboard/workflow_approval.py` ×2, and one each in `agent/server.py:157`, `agent/utils/auth.py:29`, `agent/utils/sandbox_state.py`, `agent/tools/open_pull_request.py`, `agent/middleware/check_message_queue.py`, `agent/integrations/langsmith.py`, and 17 `agent/dashboard/` modules (`agent_instructions`, `agent_overrides`, `agent_usage`, `autofix_state`, `enabled_repos`, `eval_jobs`, `notion_oauth`, `plan_api`, `plan_store`, `profiles`, `repo_snapshots`, `review_style_jobs`, `review_styles`, `team_credentials`, `team_settings`, `user_credentials`, `user_mappings`).

- **3a** Mechanical rewrite: each bare call becomes `langgraph_client()` (or `dispatch_client()` where dispatch-adjacent). Module-level clients (`agent/server.py:157`, `agent/utils/auth.py:29`) keep their construct-once lifecycle — swap the constructor expression only.
- **3b** Wrapper modules whose tests patch the *wrapper* (e.g. `agent/dashboard/autofix_state.py:23` `_client()`) are changed body-only so existing patchers are untouched.
- **3c** New guard `tests/agent/test_no_bare_get_client.py`: scans `agent/**/*.py` for zero-argument `get_client()` calls (AST-based; allowlist = the two helper definitions). This is the test-level precursor of Phase 3's ruff/CI rule.
- **3d** New case in `tests/agent/test_import_hygiene.py` using the existing subprocess `_closure_check` (`tests/agent/test_import_hygiene.py:13-21`): importing `agent.utils.auth`, `agent.dispatch`, and `agent.utils.thread_ops` must not put `langgraph_api` in `sys.modules`.

**Verification:** `grep -rEn 'get_client\(\s*\)' agent/` → no output; new guard tests pass; **full `make test`** (≈20 existing test patchers target `agent.review.findings.get_client` — verified count 20, in `tests/reviewer/test_reviewer_findings.py`; those patch the *name in the findings module*, which survives the rewrite only if 3a rebinds via the helper *call*, so run the full suite to catch churn).

### Task 4 — Compiled-graph invocation tests (`tests/agent_graph/`)
The first tests that execute the real compiled graphs with **no `langgraph dev`** — the seed of runtime-swappability (MIGRATION §4.2).

- **4a** `tests/agent_graph/conftest.py` — the single stub seam for `get_agent` (`agent/server.py`). Stubs, each annotated with the `server.py` line it corresponds to: the settings-loader gather (`server.py:834-838`: `_cached_team_default_model_pair`, `_cached_gateway_enabled`, `_cached_profile`, `_cached_fable_enabled`), the `_prepare` dependencies (`server.py:755-800`: `resolve_github_token`, `ensure_sandbox_for_thread`, `resolve_triggering_user_identity`, `client.threads.update`, `record_agent_thread_usage`), and `make_model` → `tests/agent_graph/fake_model.py` (a scripted `BaseChatModel` emitting tool calls then a final message). Clear `ttl_cache` state and `SANDBOX_BACKENDS` (`agent/utils/sandbox_state.py:185`) per test.
  **Hard rule (binding on all phases, see §1 ledger):** these tests copy patterns from `tests/e2e/` but **never import its modules**. Reason, on record: `tests/e2e/patches.py:18` does a module-level `import e2e_env`, which mutates `os.environ` process-wide at import (`e2e_env.py:17` `E2E_TMP`, `:33-34` `E2E_PORT=2024`/`E2E_BASE`, plus its `_DEFAULTS` block), and `patches.apply()` (from `:24`) irreversibly rebinds `agent.server.make_model`, dashboard token getters, and auth token functions with a module-global `_applied` guard and **no teardown** — importing it into the shared pytest process silently fakes the model factory and OAuth store for every test collected afterward. The two sanctioned alternatives for any future test needing e2e-grade wiring: (1) run it in a **subprocess**, pattern already in-tree at `tests/agent_graph/`'s disposal (`tests/agent/test_import_hygiene.py:13-21` `_closure_check`); (2) **copy** the fake-model wiring into a local fixture using `monkeypatch`-scoped patches. Phase 1's `test_real_agent_factory_via_registry` must use one of these; "reusing the e2e fakes" via import is rejected.
- **4b** `tests/agent_graph/test_scheduler_graph.py` — compile `agent/scheduler.py:35 get_scheduler`, invoke it, and assert the reconcile-sweep dispatch happens against a `_FakeThreadsClient`, modeled on the existing fakes at `tests/reviewer/test_reconcile_sweep.py:22-54` (which already pin reconcile *internals* — no duplication; this test pins that the compiled graph *wires and invokes* them).
- **4c** `tests/support/litellm.py` + `@pytest.mark.litellm` — an opt-in variant of the agent-graph test running against the local LiteLLM proxy (`LITELLM_BASE_URL`/`LITELLM_API_KEY`/`LITELLM_MODEL=minimax-m3`, `LLM_PROVIDER=litellm` from `.env:1-4` — verified consumed by **no** `agent/` code today, so tests read it only via this support module). Excluded from `make test`; never calls paid cloud APIs.
- **4d** Store-consistency test: compile the agent graph with a real `InMemoryStore` attached and assert `check_message_queue`'s in-process `get_store()` path (MIGRATION §1's consistency constraint) reads items written to that same instance. (The existing `_FakeStore` at `tests/middleware/test_check_message_queue.py:21` stays as-is — it pins queue-drain logic; this pins compile-time store attachment.)

**Verification:** `uv run pytest -vvv tests/agent_graph/` passes offline with no `langgraph dev` process.

### Task 5 — `docker-compose.test.yml` + Postgres fixture
New `docker-compose.test.yml` (Postgres 16, ephemeral volume) and `tests/support/postgres.py`: session fixture that resolves a DSN from `TEST_POSTGRES_DSN` (escape hatch — no Docker required) or `docker compose -f docker-compose.test.yml up`. Smoke test `tests/contract/test_postgres_fixture.py` (marked `contract`) connects and round-trips a row. Today's suite uses no real database (MIGRATION §4.1); this is Phase 1's substrate (`AsyncPostgresSaver/Store .setup()` will target it).
**Verification:** `pytest -m contract tests/contract/test_postgres_fixture.py` passes with Docker; `make test` remains Docker-free.

### Task 6 — Contract harness (`tests/contract/{conftest.py,normalize.py,contract_graph.py}`)
- `conftest.py`: session fixture that boots `uv run langgraph dev` on an ephemeral port with a **dedicated deterministic contract graph** (`contract_graph.py`: fake scripted model, no sandbox, no external calls) via a minimal `langgraph.contract.json`; registers the `contract` marker; `pyproject.toml` gains `addopts = "-m \"not contract\""` (+ marker registration so `--strict-markers` stays clean). The contract suite does **not** use `tests/e2e/harness.py` (which mounts the full webapp on `agent.api.app`, `harness.py:62`) — server semantics need no webapp.
- `normalize.py`: replaces run/thread UUIDs, timestamps, checkpoint ids, and event ids with stable placeholders before golden comparison; asserts event-id monotonicity *outside* the diff.
- `Makefile`: `contract-test` target = compose up + `uv run pytest -vvv -m contract tests/contract/`.
- **This marker + addopts + skip-when-absent convention is hereby the canonical one** (§1 ledger) that Phase 1's `tests/agent_runtime/` reuses — Phase 2's "make test with Docker stopped" acceptance depends on it.

**Verification:** `make contract-test` boots dev, runs, tears down; bare `make test` collects zero contract tests.

### Task 7 — Contract tests + golden transcripts (`tests/contract/test_langgraph_api_contract.py`, `tests/contract/golden/`)
The scripted sequence from MIGRATION §4.3(1), run against `langgraph dev` as the golden baseline:

1. **Threads:** create with explicit `thread_id` → **create the same `thread_id` again with `if_exists="do_nothing"` and assert idempotent 2xx + metadata handling exactly as dev behaves** *(folded finding: idempotent create is load-bearing at **9** verified call sites — `agent/webhooks/common.py:649,:717,:832,:892`, `agent/webhooks/github.py:676`, `agent/dashboard/thread_api.py:1011`, `agent/dashboard/schedules.py:442`, `agent/dashboard/review_chat_api.py:302`, `agent/tools/slack_start_new_thread.py:239` — every webhook retrigger and thread-ensure path breaks if the new runtime 409s. Reviewer note: the finding said 7 sites; code shows 9 — the two extra `webhooks/` sites only strengthen it.)* → update metadata → get → delete.
2. **Thread search:** metadata filters (incl. nested keys), `status="busy"` semantics (reconcile depends on them — `agent/reconcile.py`), pagination (`limit`/`offset`), **and — folded finding — `sort_by` (`updated_at` and `created_at`), `sort_order="desc"`, and `select=[...]` field-subsetting**, all pinned against dev. These are used app-wide and fail *silently* if ignored: `agent/dashboard/thread_api.py:545-551` (with `select=_THREAD_LIST_SELECT`, `:506`), `agent/dashboard/agent_usage.py:494-500` (`sort_by="created_at"`), `agent/dashboard/review_api.py:292-298`, `agent/dashboard/review_chat_api.py:88-93`. Phase 1's threads-search endpoint must implement all three (ORDER BY + column projection) and list them in its route table.
3. **State:** `get_state`, then the raw wire `POST /threads/{id}/history` and `/state` (thin `graph.aget_state`/`aget_state_history` wrappers in Phase 1) captured as goldens.
4. **Runs:** create with `multitask_strategy="interrupt"`, `if_not_exists="create"`, `durability="sync"`, `webhook=` pointed at a local receiver; poll to completion; assert HMAC-signed completion callback. **Double-dispatch test**: second run on a busy thread (contract graph holds a deterministic busy window) → pin observed interrupt behavior. `runs.get/list/cancel/cancel_many` incl. `cancel_many(action="interrupt")`.
5. **Store:** `put/get/delete/search_items` incl. `filter=` metadata queries on nested keys (`schedules.py:185`, `profiles.py:363` per MIGRATION §7).
6. **Crons:** `create`, `create_for_thread(end_time=…, timezone=…)`, `search`, `delete`. If dev's inmem runtime 404s any of these, record a skip-with-reason as baseline (risk 2).
7. **Streaming goldens:** raw-wire golden SSE transcripts for `POST /threads/{id}/stream/events` (all seven dashboard stream modes, `_DASHBOARD_STREAM_MODES`, `thread_api.py:56-64`) and `/commands`, captured with `httpx` exactly as the dashboard proxies do; SDK `join_stream` with mid-stream disconnect + reconnect via `last_event_id`, asserting no loss/duplication.
8. `tests/contract/golden/README.md`: what each golden pins, the normalization rules, the divergence ledger for Phase 1, and the "run `make contract-test` (bare `pytest tests/contract` selects nothing by design)" note.

**Verification:** `make contract-test` twice; second run produces zero golden diffs (`git status --porcelain tests/contract/golden/` clean).

### Task 8 — Playwright `RUNTIME` parameterization
`tests/e2e/playwright.config.ts`: switch the `webServer.command` on `process.env.RUNTIME` — `platform` (default) keeps today's command **byte-identical** (`playwright.config.ts:36-38`: `uv run langgraph dev --config tests/e2e/langgraph.e2e.json …`); `embedded` = `uv run uvicorn agent_runtime.app:app --port ${PORT}` (fails fast until Phase 1 — that's expected and correct). All six specs (`tests/e2e/tests/`: `full_flow`, `slack_untagged_reply`, `slack_debounce`, `plan_review`, `dashboard`, `sandbox_id`) stay untouched.
**Verification:** full six-spec run green with default `RUNTIME`; `RUNTIME=embedded npx playwright test --list` resolves the config without error.

### Task 9 — Phase gate + commit
Run the full acceptance list (§5), then commit on `feat/fastapi-runtime` as the phase boundary.

---

## 3. Test rationale — why each test earns its place

Every addition pins behavior the migration can break; nothing is coverage padding:

- **Compiled agent/scheduler graph tests (task 4):** the only current executor of the real graphs is the e2e suite via `langgraph dev` — the exact runtime being removed. These are the first runtime-independent executions and Phase 1 reruns them unchanged against `agent_runtime`.
- **Contract suite + goldens (task 7):** each step is a §1-table operation the app calls today; the goldens are Phase 1's parity oracle. The `if_exists`/sort/select additions exist because those are the operations that fail *silently* (wrong ordering, missing fields, 409 on retrigger) rather than loudly.
- **Guard tests (3c/3d):** the bare-`get_client` regression is invisible at review time (the code works in dev because `langgraph_api` is installed) — only a guard makes it loud.
- **Store-consistency test (4d):** pins MIGRATION §1's compile-time-store constraint before Phase 1 makes it a Postgres invariant.
- **Deliberately not added:** Linear-signature and `get_thread_id_from_branch` tests (webapp hygiene, MIGRATION §5); duplicate reconcile-internals tests (already pinned in `tests/reviewer/test_reconcile_sweep.py`); duplicate queue-drain tests (`tests/middleware/test_check_message_queue.py`).

### Existing fixtures reused vs. new

| Existing asset | Where | Phase-0 use |
|---|---|---|
| Real-app harness + fake GitHub/Slack + control endpoints | `tests/e2e/harness.py` (mounts the real `agent.api.app` at `:62`) | Task 8 unchanged; contract suite does **not** use it (server semantics need no webapp) |
| `_FakeThreads`/`_FakeRuns`/`_FakeClient` client fakes | `tests/reviewer/test_reconcile_sweep.py:22-54` | Pattern for task 4b's `_FakeThreadsClient`; reconcile internals already pinned there — no duplication |
| `_FakeStore` | `tests/middleware/test_check_message_queue.py:21` | Left as-is; task 4d attaches a real `InMemoryStore` instead |
| `_closure_check` subprocess import guard | `tests/agent/test_import_hygiene.py:13-21` | Task 3d's new case; also the sanctioned isolation pattern for later phases' e2e-grade tests (task 4a) |
| Autouse review-enabled stub | `tests/conftest.py` | Untouched; harmless to new tests |
| Dispatch kwarg-recording fakes | `tests/agent/test_dispatch.py:51-63` | Task 2's added assertion |
| LiteLLM env (`LITELLM_BASE_URL/API_KEY/MODEL`, `LLM_PROVIDER`) | `.env:1-4` (consumed by **no** `agent/` code — verified) | Only via `tests/support/litellm.py` (task 4c), only under `@pytest.mark.litellm` |

New shared fixtures, deliberately placed for Phase 1 reuse: `tests/support/litellm.py`, `tests/support/postgres.py`, `tests/agent_graph/fake_model.py`, `tests/agent_graph/conftest.py` (the graph-stub seam), `tests/contract/{conftest.py,normalize.py,contract_graph.py}`.

---

## 4. Acceptance criteria for the phase

All of the following must pass, in this order, on `feat/fastapi-runtime`:

```bash
# 1. Default suite stays hermetic (no docker, no server, no network LLM):
make test
make lint
make typecheck

# 2. Boundary cleanup is total and guarded:
grep -rEn 'get_client\(\s*\)' agent/                 # → no output
uv run pytest -vvv tests/agent/test_no_bare_get_client.py tests/agent/test_import_hygiene.py

# 3. Compiled graphs run with no langgraph-api:
uv run pytest -vvv tests/agent_graph/

# 4. Contract baseline captured and stable (twice = flake check):
make contract-test
make contract-test
git status --porcelain tests/contract/golden/        # → clean (goldens committed, replay is pure)

# 5. e2e unchanged on the platform runtime; embedded switch resolves:
cd tests/e2e && npx playwright test                  # all six specs green
RUNTIME=embedded npx playwright test --list          # resolves without error

# 6. Dead parameter gone:
grep -rn "after_seconds" agent/dispatch.py           # → no output
```

Plus: goldens for `stream_events` / `commands` / `history` exist under `tests/contract/golden/` with the README; **the contract run's test IDs include the folded steps — `test_thread_create_if_exists_do_nothing`, `test_thread_search_sort_by_updated_at_desc`, `test_thread_search_sort_by_created_at`, `test_thread_search_select_subset` — and each either passes or is a recorded skip-with-reason in the divergence ledger**; `docker-compose.test.yml` exists and `pytest -m contract tests/contract/test_postgres_fixture.py` passes; the §1 name ledger appears in `tests/contract/golden/README.md`; the phase commit exists on `feat/fastapi-runtime`.

---

## 5. Risks and mitigations (phase-specific)

1. **Golden-transcript flakiness** (timestamps, UUIDs, interleaving). *Mitigation:* dedicated deterministic contract graph (no sandbox, fake model), `normalize.py` placeholders, event-id monotonicity asserted outside the diff, and the "run replay twice" acceptance step. If an event *ordering* proves genuinely nondeterministic in `langgraph-api` itself, normalize it into a set-comparison for that event class and document it in `golden/README.md` — better an honest weaker pin than a flaky strong one.
2. **`langgraph dev` (inmem runtime) may not implement the full platform surface** — crons are the known suspect; loopback `webhook=` rejection is another (the comment at `agent/dispatch.py:34-43` describes platform behavior; dev may accept loopback); `select=` subsetting and `sort_by` semantics on the inmem backend are a third. *Mitigation:* the contract suite records *actual* dev behavior; skips-with-reason where dev 404s or diverges; the webhook test targets the receiver by the host's non-loopback address first, falling back to recording the rejection as baseline. Phase 1 implements the SDK surface the *app* uses (which definitively includes `sort_by`/`sort_order`/`select` and `if_exists="do_nothing"` — task 7 citations), with these notes as the divergence ledger (MIGRATION §4.3's pre-production liberty).
3. **The bare-`get_client` refactor changes transport** (in-process `/noauth` ASGI → HTTP to `LANGGRAPH_URL`), which could surface auth/latency differences under `langgraph dev`. *Mitigation:* this is the transport most of the app already uses (webhooks, dispatch, reconcile, all proxies); the full e2e run in task 8's verification is the end-to-end regression net; module-level clients (`server.py:157`, `auth.py:29`) keep their construct-once lifecycle.
4. **Test churn from patched names** (`agent.review.findings.get_client` ×20 in `tests/reviewer/test_reviewer_findings.py` — verified count; `autofix_state`'s `_client`). *Mitigation:* mechanical rename verified by re-grep + full `make test`; `_client()`-wrapper modules are changed body-only so their patchers are untouched.
5. **Compiled-agent test brittleness** — `get_agent` gathers several settings loaders (`server.py:834-838`) and `_prepare` touches auth/usage (`server.py:755-800`); a future new dependency in the factory breaks the conftest. *Mitigation:* all stubs live in one fixture with a comment mapping each stub to its `server.py` line; the failure mode is a loud `AttributeError`/network error, not a silent pass. `ttl_cache` and `SANDBOX_BACKENDS` cleared per test to prevent cross-test leakage.
6. **Env pollution from e2e modules** — importing `tests/e2e/e2e_env.py` mutates `os.environ` process-wide, and `patches.apply()` rebinds app internals irreversibly (`patches.py:18` module-level `import e2e_env`; `apply()` from `:24` with the permanent `_applied` guard). *Mitigation:* the hard rule in task 4a — unit/contract tests never import from `tests/e2e/`; patterns are copied, not imported — now stated with its evidence and with the two sanctioned alternatives (subprocess or copied monkeypatch fixture), and entered in the §1 ledger so Phase 1's real-agent test cannot relitigate it.
7. **Docker unavailable** on a dev machine or CI runner. *Mitigation:* `TEST_POSTGRES_DSN` escape hatch; contract marker keeps the default suite docker-free; the compose file is the only docker touchpoint. The same skip/marker convention is mandated (§1 ledger) for Phase 1's `tests/agent_runtime/` so Phase 2's "Docker stopped" acceptance holds.
8. **Playwright platform-path drift** — the parameterization must not change today's boot. *Mitigation:* platform command kept byte-identical (asserted by review against `playwright.config.ts:36-38`) and the full six-spec run is in the acceptance list.
9. **`addopts` marker exclusion surprises** — someone runs `pytest tests/contract` bare and sees "0 selected". *Mitigation:* `make contract-test` target + a note in `tests/contract/golden/README.md`; markers registered so `--strict-markers` (if ever enabled) stays clean. (Verified: `pyproject.toml` `[tool.pytest.ini_options]` currently has only `asyncio_mode` + `testpaths` — adding `addopts` collides with nothing.)

---

## 6. Effort per task

| Task | Effort |
|---|---|
| 1. Branch + baseline | S |
| 2. Delete `after_seconds` | S |
| 3. Route 28 bare `get_client()` sites + guards | M |
| 4. Compiled agent + scheduler graph tests | L |
| 5. docker-compose.test.yml + Postgres fixture | S |
| 6. Contract harness (boot, markers, make target) | M |
| 7. Contract tests + golden transcripts (incl. sort/select + if_exists pinning) | L |
| 8. Playwright `RUNTIME` parameterization | S |
| 9. Phase gate + commit | S |

Total shape matches MIGRATION §6: "small — mostly additive tests, days not weeks", with tasks 4 and 7 the two real chunks of work; the folded findings add steps to task 7's scripted sequence but no new machinery.

---

## 7. Completion record (2026-07-17)

All nine tasks are done on `feat/fastapi-runtime`. Where implementation
diverged from this plan, the divergence was deliberate and is recorded here.

**Delivered as planned.** Tasks 1–3 (baseline; `after_seconds` deleted;
28-site bare-`get_client()` rewrite + AST guard + import-hygiene case),
task 4 (`tests/agent_graph/`: 8 offline tests incl. store-consistency and
scheduler-graph; opt-in LiteLLM smoke passes against the local proxy — the
proxy serves OpenAI wire under `/v1`), task 5 (`docker-compose.test.yml` +
`tests/support/postgres.py` + smoke test), task 6 (contract harness, marker +
`addopts`, `make contract-test`), task 7 (28 contract tests + 25 goldens +
`golden/README.md` with the divergence ledger), task 8 (Playwright `RUNTIME`
switch, platform command byte-identical), task 9 (this record + phase commit).
Final state: `make test` 1547+ hermetic tests green; contract suite
28 passed + 1 recorded skip, goldens byte-stable across 4 consecutive runs.

**Deviations from the plan, with reasons.**

- **Contract server boots in a session tmpdir, not repo root** — the inmem
  runtime resolves graph paths AND its persistent `.langgraph_api/` state dir
  against cwd, so booting at repo root both destroyed developer `make dev`
  state and leaked state across sessions (golden drift). The conftest
  generates an absolute-path config into the tmpdir; the static
  `langgraph.contract.json` was dropped.
- **Golden recording is explicit** (`CONTRACT_RECORD=1`), not
  record-on-missing — a lost golden must fail, not self-certify Phase 1.
- **Adversarial test review (13 findings) folded**, most notably: the AST
  guard now catches aliased/kwarg/`url=None`/rebound/module-qualified call
  forms and tests its own detector; `agent.server` added to the
  import-hygiene closure check; resume-with-`since` asserts seq contiguity
  (loss-blind before); version/license-tier keys normalized out of goldens;
  the dev server's stdout goes to a file (pipe-buffer deadlock);
  `tests/support/postgres.py` only tears down a compose stack it started.
- **Measured dev semantics that contradicted plan assumptions** (now pinned
  in goldens instead): `multitask_strategy="interrupt"` cancels at the next
  STEP BOUNDARY — the in-flight tool step runs to completion first — not a
  preemptive kill; `store.get_item` returns `None` after delete (no 404);
  run-level `stream_mode="tools"` is 422-rejected ("tools" is only a v2
  event-stream channel); `runs.join` on a `/commands`-started run returns a
  protocol envelope, not final values; finished runs replay nothing on
  `join_stream` (live-tail-only world).
- **e2e**: `RUNTIME=embedded --list` resolves; the full 14-test six-spec
  platform run is green. One PRE-EXISTING spec race was found and fixed
  (reproduced on clean HEAD too, so not a migration regression):
  `plan_review`'s "PR opened" gate polled the global bot-message list for
  `/pull/`, which full_flow's breakout thread — whose implement run outlives
  its 2.3s test by ~15s — could satisfy with its own late PR reply, making
  the subsequent one-shot `docstring` assert fire before the plan thread's
  reply existed. The spec now polls on the feedback echo (`docstring`)
  itself. Note for Phase 2's chaos/validation work: e2e specs share one dev
  server and `/mock/slack/messages` is global, so any spec that leaves an
  async run in flight can bleed into its successors' assertions.
