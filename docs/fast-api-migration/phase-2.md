# Phase 2 Implementation Plan — Validate `agent_runtime` and Adopt It as the Default Dev Runtime

Source of truth: `docs/MIGRATION.md` §5 Phase 2. Inputs: Phase 0's contract suite + parameterized e2e boot, Phase 1's `agent_runtime/` package. All names of Phase-1 artifacts used below follow Phase 1's authoritative naming — tables `rt_thread` / `rt_run` / `rt_cron` / `rt_thread_event`, cron loop `agent_runtime/cron_scheduler.py`, startup orphan sweep in `agent_runtime`'s app lifespan (`UPDATE rt_run SET status='interrupted' ...`) — not the generic `runs`/`run_events`/`agent_runtime.scheduler` names that appeared in earlier drafts (cross-phase naming finding; Phase 3's pre-flight and sweep SQL are being corrected to match the same names).

## 1. Objective and non-goals

**Objective.** Prove the app works on `agent_runtime`, provably and repeatably, then make it the default dev runtime:

1. The six-spec Playwright e2e suite green with `RUNTIME=embedded` — the hard gate (MIGRATION.md:452-453).
2. A SIGKILL chaos suite pinning the durability floor: no state corruption, and the reconcile sweep frees the stuck-`busy` thread (MIGRATION.md:454-460).
3. A manual smoke pass of the real flows on the LiteLLM proxy (never paid cloud APIs).
4. Flip the default: `make dev` boots `agent_runtime` + Postgres; `langgraph dev` stays available as `make dev-platform`.

**Non-goals.** No new runtime features (parity gaps found here are fixed in `agent_runtime` against the Phase 0/1 contract suite, not worked around in specs); no deployment/staging/rollback machinery (pre-production); no data migration; no lint guard / TTL sweep / INSTALLATION.md rewrite (Phase 3); no touching the LangSmith sandbox/tracing SaaS dependencies (out of scope per MIGRATION.md:543-546).

## 2. Decisions

- **D1 — Chaos floor pinned to Phase 1's queue decision.** Phase 1 chose `asyncio.create_task` execution with **no auto-resume**: a startup orphan sweep marks orphaned `rt_run` rows `interrupted` on boot, and recovery relies on the reconcile sweep + user re-trigger (MIGRATION.md:525-533 frames exactly this choice). The chaos tests therefore assert: consistent checkpoint prefix (never a corrupt/partial checkpoint), no `rt_run` left `running` after restart, thread freed, and **no** silent re-execution. Assertions accept "prefix of steps", never exact counts. If a corruption is ever observed it is a Phase 1 bug to fix, not a test to loosen.
- **D2 — Single-process dev topology.** Phase 1's topology decision (MIGRATION.md:441-444) is exercised here as: one uvicorn process serving `agent_runtime` with the webapp/harness mounted as a sub-app on one origin. The e2e suite hard-requires this: `slack_debounce.spec.ts:37` does a same-origin `GET /threads/{id}` against the same base URL the mock-Slack endpoints live on, and `tests/e2e/e2e_env.py:61` points `LANGGRAPH_URL` at that same origin. `dispatch.py`'s loopback-webhook rejection (`agent/dispatch.py:48-53`) is satisfied the same way it is under `langgraph dev` today (`127.0.0.1` URLs degrade to a warning-and-None path or the harness overrides — T1 verifies which and keeps behavior identical to the platform leg).
- **D3 — Automated tests keep the scripted fake LLM; LiteLLM is for the manual smoke only.** The six specs stay deterministic on `FakeScriptedChatModel` (`tests/e2e/patches.py:50-55`). The LiteLLM proxy (`LITELLM_BASE_URL`/`LITELLM_API_KEY`/`LITELLM_MODEL=minimax-m3`, already in `.env`) backs only the T5 smoke via the `E2E_REAL_LLM=1` path (`patches.py:47-48`). No test or smoke step calls a paid cloud LLM API.
- **D4 — The no-Docker skip convention is verified phase-wide, not assumed.** Phase 1 pinned the convention for Postgres-requiring suites (marker + skip when the test DSN/Docker is absent — the cross-phase finding forced Phase 1 T1 to state it explicitly). Phase 2's acceptance runs `make test` with Docker stopped and requires **exit 0 with `tests/agent_runtime/`, `tests/contract/`, and `tests/chaos/` all collected-and-skipped cleanly** — not merely "the chaos guard works". `tests/chaos/` adopts the identical convention plus an explicit `RUN_CHAOS=1` opt-in gate (chaos runs are slow and process-killing; they never ride `make test`).
- **D5 — No test outside `tests/e2e/` ever imports a `tests/e2e/` module.** Phase 0's hard rule, restated because it binds T3: `tests/e2e/patches.py:18` does `import e2e_env` at module import, which mutates `os.environ` process-wide (`e2e_env.py:17,33-34,85-86`), and `apply()` irreversibly rebinds `agent.server.make_model` and the OAuth token accessors with a permanent `_applied` guard (`patches.py:22-85`). Chaos tests copy patterns (fake graph, webhook receiver) into `tests/chaos/`; the runtime under chaos runs as a **subprocess** (same isolation idea as `tests/agent/test_import_hygiene.py:13-21`'s `_closure_check`).

## 3. Ordered tasks

### T1 — Embedded e2e boot path

Make Phase 0's `RUNTIME=embedded` switch in `tests/e2e/playwright.config.ts` actually boot a working stack.

**Files:**
- `tests/e2e/run-embedded.sh` (new) — the embedded `webServer` command: `docker compose -f docker-compose.test.yml up -d --wait` (Phase 0 compose file), **drop/recreate the dedicated e2e database** (hermeticity — `langgraph dev` forgot everything between runs; Postgres won't), then `exec uv run uvicorn` on `agent_runtime`'s app with `E2E_PORT` (default 2024, `playwright.config.ts:5`).
- `tests/e2e/playwright.config.ts` — point the `RUNTIME=embedded` leg's `webServer.command` at `run-embedded.sh`; keep the readiness `url` probe (`/mock/github/data`, line 40) and the `E2E_BUSY_HOLD_SECONDS: "20"` env (line 45) identical across both legs.
- `agent_runtime` config loader (only if Phase 1 didn't ship one general enough): `tests/e2e/langgraph.e2e.json` registers the e2e graph entrypoint (`./tests/e2e/agent_entrypoint.py:traced_agent`) and the harness http app (`./tests/e2e/harness.py:app`). The embedded boot must honor the same two hooks — graph registry + mounted sub-app (D2) — via whatever mechanism Phase 1 built for `langgraph.json` (e.g. `AGENT_RUNTIME_CONFIG=tests/e2e/langgraph.e2e.json`). If Phase 1's loader can't mount an `http.app`, T1 grows a small mounting shim in `agent_runtime`'s app factory.

The UI build in `tests/e2e/global-setup.ts` is runtime-agnostic (it bakes `VITE_DASHBOARD_API_BASE_URL=http://127.0.0.1:$E2E_PORT`, same origin either way) — no changes.

**Verify:** `RUNTIME=embedded npx playwright test tests/full_flow.spec.ts` boots within the `webServer` timeout and the readiness probe passes; run it **twice back-to-back** to prove the DB reset makes runs hermetic; `RUNTIME=platform` leg still boots unchanged.

### T2 — Six specs green on `RUNTIME=embedded` (the hard gate)

Debug order chosen so the cheapest-signal specs localize failures first, and the wire-protocol spec runs mid-sequence, not last:

1. `full_flow.spec.ts` — webhook → run → PR happy path (threads/runs/webhook basics).
2. `slack_untagged_reply.spec.ts` — message-queue store path (in-process `get_store()` vs HTTP store consistency, MIGRATION.md:102-114).
3. `dashboard.spec.ts` — real built `ui/` + `@langchain/react` `useStream` against `/stream/events`, `/commands`, `/history`, `/state` (the v2 wire protocol end-to-end). Also exercises the dashboard thread list, which depends on `threads.search` honoring `sort_by="updated_at"`, `sort_order="desc"`, and `select=` field-subsetting (`agent/dashboard/thread_api.py:506,545-551`) — implemented in Phase 1 T4 and pinned by the Phase 0 contract goldens per the cross-phase sort/select finding; a wrong-ordered or missing-field thread list here is an `agent_runtime` bug, not a spec problem.
4. `slack_debounce.spec.ts` — multitask-interrupt arbitration mid-run against the deterministic busy window; also re-hits `threads.create(..., if_exists="do_nothing")` on an existing thread via the follow-up webhook path (`agent/webhooks/common.py:649,717,832,892`) — the idempotent-create semantics the cross-phase `if_exists` finding added to the Phase 0 contract sequence. A 409/500 on the follow-up message is an `agent_runtime` bug.
5. `plan_review.spec.ts` — interrupt/resume + WebSocket collaboration through the mounted harness.
6. `sandbox_id.spec.ts` — thread-metadata persistence across runs.

**Files:** fixes land in `agent_runtime/` only. Any *intentional* divergence from `langgraph-api` behavior updates the Phase 0 golden transcripts in the same commit (the baseline is "a tool for catching accidental divergence, not a compatibility oath" — MIGRATION.md:363-367). Specs and harness are not modified except for genuine spec bugs, called out individually in review.

**Verify:** all six green on `RUNTIME=embedded`; all six still green on `RUNTIME=platform`; `uv run pytest -vvv tests/contract/` still at parity after every `agent_runtime` fix.

### T3 — SIGKILL chaos suite (`tests/chaos/`, new)

Pins D1's floor. Everything lives in `tests/chaos/`; nothing imports from `tests/e2e/` (D5).

**Files:**
- `tests/chaos/slow_graph.py` — a no-LLM graph of ~30 sequential steps that checkpoints every ~0.5 s (each step appends its index to state), plus a minimal entrypoint/config so `agent_runtime` can register it. Copied pattern from `tests/e2e/agent_entrypoint.py`, not imported.
- `tests/chaos/receiver.py` — tiny local HTTP receiver recording HMAC-signed completion-webhook deliveries; modeled on the Phase 0 contract suite's receiver (copied, not imported).
- `tests/chaos/conftest.py` — Docker Postgres via the Phase 0 compose fixture; launches `uvicorn` serving `agent_runtime` as a **subprocess** (pipes captured); `RUN_CHAOS=1` opt-in gate plus the Phase-1 Postgres marker/skip convention (D4) so `make test` collects-and-skips it with or without Docker.
- `tests/chaos/test_sigkill.py` — three tests:
  1. **`test_sigkill_mid_run_no_corruption`** — start a run on the slow graph, `SIGKILL` the uvicorn subprocess ~40% through, restart. Assert: checkpoint state for the thread is a consistent *prefix* of steps (via `/threads/{id}/state`); no `rt_run` row remains `running` after the startup orphan sweep (implement the sweep in `agent_runtime`'s lifespan if Phase 1 didn't ship it — it's Phase 1 T6's `UPDATE rt_run SET status='interrupted'` behavior); the run did **not** silently re-execute (step count never exceeds the pre-kill prefix without a new run being created). First action of this task: read what Phase 1 actually shipped for the sweep and for webhook-on-sweep, and pin the webhook assertion to it (if the sweep emits the terminal-state completion webhook — MIGRATION.md:429 says "every terminal state" — assert exactly one HMAC-valid delivery at the receiver; if it deliberately doesn't, assert zero and document why in the test).
  2. **`test_reconcile_frees_stuck_busy_thread`** — after kill+restart, drive the real sweep: point `LANGGRAPH_URL` at the restarted runtime and call `agent.reconcile.reconcile_stale_runs(max_age_seconds=0)` (`agent/reconcile.py:39` — walks `busy` threads, lists `pending` runs, `cancel_many(action="interrupt")`, lines 58-100). Assert the thread's status is no longer `busy` and the counters report the cancellation. Importing `agent.reconcile` into the pytest process is fine — it pulls webapp-side modules only, no `tests/e2e/` code and no env mutation.
  3. **`test_kill_during_checkpoint_write`** — loop 3 iterations killing at randomized offsets (0.3–0.8 s past a step boundary) to land kills near/inside checkpoint writes; each iteration asserts the same floor as test 1.

**Verify:** `RUN_CHAOS=1 uv run pytest -vvv tests/chaos/` green **three consecutive runs**; `uv run pytest tests/chaos/` without `RUN_CHAOS` (and without Docker) exits 0 with everything skipped.

### T4 — LiteLLM wiring for the smoke pass

`LLM_PROVIDER=litellm` / `LITELLM_*` exist only in `.env` today — nothing in `agent/` reads them (verified: zero grep hits in `agent/`), so this is genuinely new, small wiring:

**Files:**
- `agent/utils/model.py` — a guarded `litellm` provider branch in `make_model` (`model.py:114`) / `provider_model_kwargs`: when `LLM_PROVIDER=litellm`, build the OpenAI-compatible chat model against `LITELLM_BASE_URL`/`LITELLM_API_KEY` with `LITELLM_MODEL` (default `minimax-m3`). `fallback_model_id_for` returns `None` for it (self-hosted providers don't silently route off-host — `model.py:162-163`).
- `agent/dashboard/options.py` — register the litellm model id so per-thread/profile resolution accepts it (or scope it behind the env flag; follow whichever pattern `options.py` uses for provider gating).
- `tests/e2e/dev-mock.sh` — add an `LLM_PROVIDER=litellm` branch that skips the OpenAI `sk-…` key sniffing/warning (lines 22-35 today) and exports the `LITELLM_*` vars into the harness; and add an embedded-runtime variant of the boot line (the script currently hardcodes `langgraph dev`, last stanza) selected by the same `RUNTIME` switch as Playwright.

**Reviewer note (patches.py reuse here is legitimate):** `patches.py` in this task runs only *inside the dev-server process* — exactly as it does today via `langgraph.e2e.json`'s entrypoints — never inside the unit-test pytest process. The Phase-0 hard rule (and the Phase-1 T12 finding about `patches.py:18`'s process-wide env mutation) bans importing it into the **shared pytest process**; a dedicated server subprocess is the intended host for it.

**Verify:** `RUNTIME=embedded LLM_PROVIDER=litellm tests/e2e/dev-mock.sh` boots; one mock-Slack message produces a real model completion through the proxy; no `OPENAI_API_KEY`/`ANTHROPIC_API_KEY` present in the environment (unset them for the check).

### T5 — Manual smoke pass (checklist, executed on the embedded runtime)

Run via T4's harness (`dev-mock.sh`, LiteLLM). The checklist's purpose is **runtime behavior** — streaming, status transitions, webhooks, crons — not model quality (D3): a flaky model answer with correct runtime mechanics is a pass.

1. **Dashboard chat streaming** (real `ui/` build): send a message, watch tokens stream; force a disconnect mid-stream (devtools offline toggle) and confirm reconnect resumes from `last_event_id` with no gap/duplicate.
2. **Slack-triggered run to completion**: mock-Slack mention → run → completion reply; confirm one HMAC-valid completion-webhook delivery in the runtime log.
3. **Follow-up on the same Slack thread** (pins `if_exists="do_nothing"` retrigger in anger — `webhooks/common.py:649,717,832,892`): second tagged message routes to the same thread, no duplicate thread, no error.
4. **Busy-thread behavior**: untagged follow-up mid-run queues (no interrupt); tagged follow-up interrupts.
5. **Crons**: create a near-term thread wakeup (`crons.create_for_thread` with `end_time`/`timezone`) and a schedule-based cron; watch `agent_runtime/cron_scheduler.py` fire both and the wakeup respect `end_time`.
6. **Plan review flow**: two mock users (Alice/Bob), request-changes round trip over the collaboration WebSocket.
7. **Thread-list ordering and fields** (pins the sort/select finding at the UI level): dashboard thread list newest-`updated_at` first with all rendered fields present (`thread_api.py:506`); reviewer/usage views ordered per `agent_usage.py:494-500` (`created_at` desc) and `review_api.py:292-298` / `review_chat_api.py:88-93` (`updated_at` desc).
8. **Cancel from the UI**: `/runs/{run_id}/cancel` mid-run; thread status recovers.
9. **GitHub-triggered path**: mock-GitHub PR-comment trigger through the harness. **Linear**: no harness fake exists — covered by the unit webhook tests; do a real-token spot check only if credentials are on hand (not a gate).

**Exit condition:** at the end, `threads.search(status="busy")` is empty (zero stuck-busy threads), every terminal run has a logged completion webhook, and the runtime log shows no unexplained errors. Runtime defects found here are fixed in `agent_runtime` and, where a contract-visible behavior changed, back-filled into the contract suite.

### T6 — Flip the default dev runtime

**Files:**
- `Makefile` — `dev:` becomes compose-up Postgres (`--wait`) + uvicorn boot of `agent_runtime` on `:2024` (the app-wide default `LANGGRAPH_URL` target, `agent/dispatch.py:85`); add `dev-platform:` preserving `uv run langgraph dev`; update `help` text.
- `CLAUDE.md` + `AGENTS.md` — Commands section: `make dev` description, `make dev-platform`, the Postgres prerequisite, and the `RUNTIME` e2e switch.
- `docs/INSTALLATION.md` — touch only the local-dev run instructions to match the new default (the full production-path rewrite stays Phase 3).
- `.github/workflows/ci.yml` — turn the e2e job (lines 50-81) into a matrix `runtime: [platform, embedded]`; the embedded leg adds a Postgres service container (or the compose file) before `npx playwright test`, passing `RUNTIME=embedded`.
- `langgraph.json` — unchanged (platform path stays for `make dev-platform`; its `checkpointer.ttl` replacement is Phase 3).

**Verify:** fresh-clone flow on this branch: `make install && make dev` serves the stack; `make dev-platform` still works; CI green on both matrix legs.

### T7 — Phase-boundary commit

Single commit (or small stack) on `feat/fastapi-runtime` at the phase boundary, after the full §5 acceptance block passes locally and CI is green.

## 4. Test rationale

Every new test pins a behavior this migration could silently break; nothing is written for coverage's sake:

- **The six e2e specs (unchanged)** are the acceptance instrument, not new work — they were built/parameterized in Phase 0 precisely so that "passes unmodified on the new runtime" is the cutover bar (MIGRATION.md:368-384). `slack_debounce` pins the hardest platform semantic (multitask-interrupt on a busy thread) and, incidentally, idempotent thread create; `dashboard` pins the v2 wire protocol against the real browser SDK, including thread-search `sort_by`/`sort_order`/`select`.
- **Chaos tests (new)** pin the one property no request/response contract test can: what survives process death. The floor is exactly what `durability="sync"` + Phase 1's queue decision promise — nothing more (no auto-resume assertion, per D1) and nothing less (no corruption, sweep frees the thread).
- **No new unit tests** are added in this phase by default: runtime bugs T2/T5 surface get fixed in `agent_runtime` with a regression test **in the Phase 1 suites** (`tests/agent_runtime/`, `tests/contract/`) where the behavior lives, keeping this phase's own test surface to the two instruments above.

Fixtures:

| Fixture | Disposition in Phase 2 |
|---|---|
| `docker-compose.test.yml` Postgres (Phase 0) | reused by `run-embedded.sh` and the chaos conftest; no changes |
| Phase 0 contract-suite webhook receiver | pattern **copied** into `tests/chaos/receiver.py` (never imported — D5) |
| `tests/e2e/` harness, fakes, `patches.py`, fake LLM | unchanged; run inside the webServer/dev-server subprocess for both runtimes, never in the pytest process |
| `tests/conftest.py` autouse `is_review_repo_enabled` stub | harmless under the chaos tests (they import `agent.reconcile`, which pulls webapp modules); no changes needed |

New fixtures are limited to what nothing existing covers: the chaos `slow` graph + entrypoint (T3) and the tiny webhook receiver (T3, modeled on the Phase 0 contract suite's receiver).

## 5. Acceptance criteria for the phase

All of the following pass at the phase-boundary commit on `feat/fastapi-runtime`:

```bash
# The hard gate — six specs, both runtimes
cd tests/e2e && RUNTIME=embedded npx playwright test     # 6/6 green (run twice; hermetic)
cd tests/e2e && RUNTIME=platform npx playwright test     # 6/6 green (no regression)

# Chaos floor (D1 pinned)
RUN_CHAOS=1 uv run pytest -vvv tests/chaos/              # green, 3 consecutive runs

# Contract parity survived T2 fixes — including the Phase-0 additions for
# threads.search sort_by/sort_order/select and threads.create if_exists="do_nothing"
uv run pytest -vvv tests/contract/

# Nothing else broke; the no-Docker convention holds phase-wide (D4):
# with Docker stopped, make test exits 0 and tests/agent_runtime/,
# tests/contract/, tests/chaos/ are all collected-and-skipped — zero
# collection errors, not just "the chaos guard works"
make test && make lint && make typecheck                 # (Docker stopped for make test)

# The flip is real
make dev            # boots agent_runtime on :2024, serves threads API + webapp
make dev-platform   # langgraph dev still available
```

Plus: T5 smoke checklist executed in full with zero stuck-`busy` threads and a logged completion webhook per terminal run; CI matrix (platform + embedded e2e) green on the branch.

## 6. Risks specific to this phase

- **Wire-protocol residue only visible to the real UI.** The contract suite compares transcripts, but `@langchain/react`'s `useStream` may depend on details a transcript diff tolerates (event ordering across modes, heartbeat/comment lines, exact `id:` continuity on reconnect). *Mitigation:* `dashboard.spec.ts` is scheduled mid-T2 (not last), and its Playwright trace/video capture (`tests/e2e/playwright.config.ts:23-25`) localizes divergences; fix in `agent_runtime`, re-run the contract suite to keep the transcript in sync.
- **Silent-parity gaps that don't 4xx.** Two known-by-finding cases are now explicitly instrumented — thread-list ordering/field-subsetting (`thread_api.py:545-551`; T2 step 3 + smoke item 7) and `if_exists="do_nothing"` idempotent create (`webhooks/common.py:649` etc.; T2 step 4 + smoke item 3) — but the class remains: behavior only *our* callers depend on can pass a lax assertion. *Mitigation:* every T2/T5 fix lands with a contract- or `tests/agent_runtime/`-level regression test, tightening the net as gaps surface.
- **Chaos-test flakiness from SIGKILL timing.** Killing at unlucky moments (e.g. during the checkpoint write) must still satisfy the floor. *Mitigation:* the slow graph checkpoints every 0.5 s so the kill always lands near a boundary; assertions accept "prefix of steps", never an exact count; T3's verify step mandates three consecutive green runs; if a corruption is ever observed it is a Phase 1 bug to fix, not a test to loosen.
- **Postgres persistence breaks e2e hermeticity.** `langgraph dev` forgot everything on restart; the embedded runtime doesn't. *Mitigation:* `run-embedded.sh` drops/recreates the e2e database before boot (T1), verified by back-to-back runs.
- **Single-origin assumption vs. Phase 1 topology.** If Phase 1 actually shipped two processes, `slack_debounce.spec.ts:37`'s same-origin `GET /threads/{id}` fails immediately. *Mitigation:* D2 pins single-process for dev; if the runtime app can't mount the webapp, T1 grows a small mounting shim in `agent_runtime`'s app factory — surfaced on the first spec run, cheap to fix then.
- **LiteLLM proxy model quality derails the smoke pass.** `minimax-m3` may loop or emit malformed tool calls, wasting smoke time on non-migration issues. *Mitigation:* the smoke checklist's purpose is runtime behavior (streaming, status, webhooks, crons), not model quality — a flaky model answer with correct runtime mechanics is a pass; the deterministic gates (T2/T3) carry the correctness burden.
- **CI embedded leg is slower/flakier than platform.** Docker pull + DB init + uvicorn boot inside the Playwright `webServer` timeout (180 s, `tests/e2e/playwright.config.ts:42`). *Mitigation:* `--wait` on compose, keep the readiness URL probe (`playwright.config.ts:40`); bump `webServer.timeout` only for the embedded leg if measured necessary.

## 7. Effort summary

| Task | Effort |
|---|---|
| T1 — embedded e2e boot path (`run-embedded.sh`, config switch, config loader/mount shim if missing) | M |
| T2 — six specs green on `RUNTIME=embedded` (runtime debugging to the hard gate) | L |
| T3 — SIGKILL chaos suite (orphan sweep if missing, 3 tests, receiver, slow graph) | L |
| T4 — LiteLLM wiring for smoke (`model.py` provider branch, `options.py`, `dev-mock.sh`) | S |
| T5 — manual smoke pass (checklist execution + runtime fixes it surfaces) | M |
| T6 — default flip (Makefile, CLAUDE.md/AGENTS.md, INSTALLATION.md touch, CI matrix) | S |
| T7 — phase-boundary commit | S |

---

## 8. Completion record (2026-07-17)

**The hard gate holds:** all six specs (14 tests) green on `RUNTIME=embedded`,
run twice back-to-back (the run-embedded.sh DB drop/recreate proves
hermeticity), and still green on `RUNTIME=platform`. Chaos suite green three
consecutive runs; skipped cleanly without `RUN_CHAOS=1`. T5 smoke executed on
the embedded runtime with the real LiteLLM model (minimax-m3): Slack →
completion reply, same-thread follow-up, live v2 streaming + `since` resume
with zero loss, mid-run cancel → `interrupted`, one-shot cron wakeup fired
and stopped, thread-list sort/select fields correct, zero stuck-busy threads
at exit. `make dev` boots agent_runtime + Postgres on :2024 (webapp mounted,
threads API live); `make dev-platform` preserves `langgraph dev`; CI e2e is
a platform/embedded matrix.

**Runtime gaps T2/T5 surfaced — all fixed in `agent_runtime` (per plan):**

- **`__is_for_execution__` injection.** langgraph-api injects the execution
  gate at run pickup; without it, gated factories (get_agent) returned their
  inert registration stub and the run executed a default-model agent with no
  tools. The executor now passes the flag in the FACTORY config only — it
  must not reach the astream config, or it leaks into checkpoint
  configurables (run_stream_mode_checkpoints.json pins dev's shape).
- **Worker-pickup delay** (`AGENT_RUNTIME_PICKUP_DELAY_MS`, default 500).
  The platform's run queue has pickup latency; instant `create_task`
  execution removed it, and `slack_debounce` proved it load-bearing: an
  untagged follow-up posted ~170ms after dispatch was drained by the run's
  FIRST before_model call before the second follow-up arrived, so the queue
  never showed 2. The delay restores the platform's coalescing window
  (chaos suite runs with it set to 0).
- **Config `"env"` file honored** (langgraph.json contract): the runtime now
  loads the config-declared dotenv (existing env wins) before the webapp
  import — `make dev` parity with `langgraph dev`'s env handling.
- **`.py`-path `http.app` targets** (`./tests/e2e/harness.py:app`) load with
  the entry file's directory importable, matching dev's resolution.

**T4 wiring:** `litellm:<model>` provider branch in `make_model` (plain
chat-completions against `LITELLM_BASE_URL`, no Responses API, no gateway,
`fallback_model_id_for` → None); env-gated model entry in `options.py`
(`LLM_PROVIDER=litellm`) and env-overridable `DEFAULT_MODEL_ID`/`EFFORT`;
`dev-mock.sh` gained the litellm and RUNTIME branches. No paid cloud API is
reachable through any of it.

**Smoke item 9 (GitHub/Linear):** GitHub PR flows are covered by the
harness-driven e2e (mock GitHub) and webhook unit tests; Linear has no
harness fake — unit webhook tests only, per plan (not a gate).

**Chaos infra note:** the runtime-under-chaos subprocess must run in its own
process group (`start_new_session=True`) — killpg on the shared group
SIGKILLs pytest itself.

**Adversarial review (13 findings) — all addressed.** The one significant
hole: the `litellm:` model branch could silently fall back to
api.openai.com with the shell's real `OPENAI_API_KEY` when `LITELLM_BASE_URL`
was unset (empty base_url/api_key engage langchain's cloud defaults) — now
fail-closed on BOTH gates (`LLM_PROVIDER=litellm` required, `LITELLM_BASE_URL`
required, placeholder api_key so the env fallback can never engage), and the
env-overridable `DEFAULT_MODEL_ID` is validated against `SUPPORTED_MODEL_IDS`
at import. Also fixed: the env-file claim (langgraph dev's `.env` OVERRIDES
the shell; `make dev` deliberately keeps shell-wins — documented in
CLAUDE.md), pickup-delay clamped finite (≤30s), advisory-lock boot retry
(post-SIGKILL release latency), chaos test 3's vacuous-pass hole + database
leaks, `.py` graph entrypoints get their own sys.path insertion (no longer
riding the webapp's), zero pickup delay in the unit/contract suites with a
dedicated test pinning the delay, `make dev` gained `--reload` + INFO app
logs + tmpfs/override caveats, and the "HMAC-signed" wording corrected to
bearer-token. Re-verified after all fixes: runtime suite 31 passed, contract
embedded 27 passed, chaos 3/3, embedded e2e 14/14.
