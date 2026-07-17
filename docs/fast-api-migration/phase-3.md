# Phase 3 — Final Implementation Plan: Hygiene

**Branch:** `feat/fastapi-runtime`. **Source of truth:** `docs/MIGRATION.md` §5 Phase 3. Smallest phase: one lint guard, one sweep job, doc corrections. Prerequisites: Phases 0–2 complete.

---

## 0. Objective and non-goals

**Objective.** (1) Upgrade the bare-`get_client()` protection from Phase 0's test to a ruff rule that fires in `make lint` and editors; (2) reimplement the checkpoint TTL sweep that `langgraph.json`'s `checkpointer.ttl` block *intended* (it enforces nothing today — see T2 facts) as a periodic `agent_runtime` job; (3) rewrite `docs/INSTALLATION.md`'s production section off LangGraph Cloud; (4) record LangSmith sandbox + tracing as separate, out-of-scope SaaS dependencies.

**Non-goals.** No dependency-group surgery: `langgraph-cli[inmem]` **stays a normal dependency** (decided — ELv2 permits dev use; the lint guard is the protection, not dep groups). No new guard mechanisms beyond the two named in T1 (the guard was previously triple-built across phase drafts; this plan consolidates). No `tests/hygiene/` tree, no standalone AST lint script, no tests-for-the-linter. No deployment/infra work (MIGRATION §5 "Deferred"). No Redis.

## 0a. Named assumptions (reconcile against final `phase-1.md` before starting T2)

`docs/fast-api-migration/phase-1.md` was a truncated fragment when this plan was written. Every Phase-1 name below is taken from that fragment and the cross-phase review findings; **verify each against the final phase-1.md and fix this document's SQL/imports on mismatch — do not adjust ad hoc in code**:

- **A1** — owned tables are `rt_thread`, `rt_run`, `rt_cron`, `rt_thread_event` (fragment §6: "pruning of `rt_thread_event` rides the Phase 3 sweep job … written against the `rt_*` names").
- **A2** — scheduler module is `agent_runtime/cron_scheduler.py` (APScheduler `>=3.10,<4`); app is `agent_runtime/app.py`; Postgres DSN env is `DATABASE_URL`.
- **A3** — in-flight `rt_run.status` values are `'pending'` and `'running'`.
- **A4** — `rt_thread.updated_at` (timestamptz) exists and is touched on every run/state write (implied by Phase 1 T4's `sort_by="updated_at"` support).
- **A5** — `tests/agent_runtime/conftest.py` provides the Postgres fixture and the D5 skip convention (clean SKIP when Docker/`TEST_POSTGRES_DSN` is absent).
- **A6** — `langgraph-checkpoint-postgres` is pinned at 3.x. Verified against 3.1.0 (uv cache): **no native TTL/sweep API exists** (zero `ttl`/`sweep` hits in the package source), but `AsyncPostgresSaver.adelete_thread(thread_id)` (`aio.py:340`) deletes a thread's rows from all three checkpoint tables (`checkpoints`, `checkpoint_blobs`, `checkpoint_writes`) in one pipelined transaction. Re-verify if the pin moves.

---

## 1. Ordered tasks

### T1 — Ruff guard for `langgraph_sdk.get_client` (upgrade + consolidate)

Ruff **cannot express call-arity bans** ("no *zero-arg* `get_client()`") — no ruff rule inspects a banned call's arguments. TID251 (`flake8-tidy-imports` banned-api) bans the *qualified name*, which is strictly stronger: outside the two URL-resolving helpers, no `agent/` or `agent_runtime/` module may reference `get_client` at all, so arity is moot. Adopt that; no CI grep needed.

**Files:** `pyproject.toml` only, plus one test deletion and one test addition.

1. `pyproject.toml` `[tool.ruff.lint]` — add `"TID251"` to `select` (current select is E/W/F/I/B/C4/UP; banned-api never fires without it).
2. New stanzas:
   ```toml
   [tool.ruff.lint.flake8-tidy-imports.banned-api]
   "langgraph_sdk.get_client".msg = "Use thread_ops.langgraph_client() or dispatch.dispatch_client(); get_client(url=None) imports Elastic-licensed langgraph_api in-process."
   "langgraph_sdk.get_sync_client".msg = "Same hazard as get_client; use the URL-resolving helpers."

   [tool.ruff.lint.per-file-ignores]
   "agent/utils/thread_ops.py" = ["TID251"]   # langgraph_client() — the sanctioned wrapper
   "agent/dispatch.py" = ["TID251"]           # dispatch_client() — the sanctioned wrapper
   "tests/**" = ["TID251"]                    # contract suite legitimately calls get_client(url=...)
   ```
   (`get_sync_client` has zero current uses in `agent/` — verified — banned to close the sibling escape hatch.)
3. **Consolidation (one static guard + one dynamic pin, total):** delete `tests/agent/test_no_bare_get_client.py` — Phase 0's ledger designated it "the guard Phase 3 upgrades to a ruff/CI rule", and TID251 subsumes it (same helper-file allowlist, wider net, fires in `make lint` and editors instead of only at pytest time). The dynamic pin stays where Phase 0 task 3d put it: `tests/agent/test_import_hygiene.py` (`_closure_check` subprocess probes). Add **one** case there: importing `agent_runtime.app` must not put `langgraph_api` in `sys.modules` (assumption: final phase-1.md did not already add it; if it did, this sub-step is a no-op).

**Verification:**
```bash
uv run ruff check .                                   # clean
printf 'from langgraph_sdk import get_client\n' >> agent/utils/slack.py \
  && uv run ruff check agent/utils/slack.py; git checkout agent/utils/slack.py   # TID251 fired, then reverted
ls tests/agent/test_no_bare_get_client.py 2>&1        # No such file
uv run pytest -vvv tests/agent/test_import_hygiene.py
make lint && make test
```

### T2 — Checkpoint TTL sweep job in `agent_runtime`

**Facts (all verified in-tree):** `langgraph.json` sets `checkpointer.ttl = {strategy: "delete", sweep_interval_minutes: 60, default_ttl: 43200}`. `default_ttl` is **minutes** ⇒ 43200 = **30 days** (MIGRATION §3's "12 h TTL" is wrong — T3 fixes it). Under `langgraph dev` this block is **dead config**: the inmem runtime's `sweep_ttl` is an explicit no-op (`langgraph_runtime_inmem/ops.py:1493` — "Not implemented for inmem server", returns `(0, 0)`). So this sweep is **new enforcement of the config's platform intent, not preservation of observed behavior** — which is also why Phase 0 correctly captured no TTL golden baseline. Platform `strategy="delete"` semantics: "Remove the thread and all its data entirely" (`langgraph_api/config/_parse.py:45-48`).

**Deliberate divergence (recorded in the ledger, T3):** we do **not** drop the thread. `rt_thread.metadata` is load-bearing app state (sandbox id, encrypted GitHub token, Slack/PR links — the thread row is the app's KV record for a conversation) and `rt_run` rows feed usage/history. The sweep deletes only checkpoint data + the run-event log; `get_state` on a swept thread returns empty, like a never-run thread.

**Files:** new `agent_runtime/ttl_sweep.py`; registration in `agent_runtime/cron_scheduler.py` (A2); new `tests/agent_runtime/test_ttl_sweep.py`.

1. Config (env, defaults mirroring `langgraph.json`): `CHECKPOINT_TTL_MINUTES=43200`, `CHECKPOINT_SWEEP_INTERVAL_MINUTES=60`, `CHECKPOINT_SWEEP_LIMIT=500`. `CHECKPOINT_TTL_MINUTES=0` disables the job entirely.
2. `async def sweep_expired_checkpoints(pool, saver) -> int` — victim selection against **our own schema only** (A1/A3/A4), never against the checkpoint package's tables:
   ```sql
   SELECT t.thread_id FROM rt_thread t
   WHERE t.updated_at < now() - make_interval(mins => %(ttl)s)
     AND NOT EXISTS (SELECT 1 FROM rt_run r
                     WHERE r.thread_id = t.thread_id
                       AND r.status IN ('pending', 'running'))
   LIMIT %(limit)s
   ```
3. Per victim: `await saver.adelete_thread(thread_id)` (A6 — the package's supported cross-table delete; **no hand-rolled SQL against `checkpoints`/`checkpoint_blobs`/`checkpoint_writes`**), then `DELETE FROM rt_thread_event WHERE thread_id = %s`. `rt_thread` and `rt_run` rows untouched. Log and return the swept count.
4. Register on the existing APScheduler instance during `cron_scheduler.py` startup (interval trigger every `CHECKPOINT_SWEEP_INTERVAL_MINUTES`); skip registration when TTL is 0.
5. Tests (Postgres-backed, D5 skip convention per A5) — four, no more:
   - expired thread, no in-flight run → swept: `await saver.aget_tuple(cfg) is None`, zero `rt_thread_event` rows, `rt_thread` row + metadata intact (the divergence pin);
   - thread with a `running` run → untouched despite expired `updated_at`;
   - fresh thread → untouched;
   - second sweep → returns 0 (idempotent).

**Verification:**
```bash
docker compose -f docker-compose.test.yml up -d
uv run pytest -vvv tests/agent_runtime/test_ttl_sweep.py
docker compose -f docker-compose.test.yml down
uv run pytest -vvv tests/agent_runtime/test_ttl_sweep.py   # clean SKIP, no errors
make typecheck                                             # agent_runtime/ already included (Phase 1 T1)
```

### T3 — Documentation corrections + divergence ledger

**Files:** `docs/MIGRATION.md`, `tests/contract/golden/README.md`.

1. MIGRATION §3 checkpoint-TTL bullet: replace "12 h TTL" with "43200 minutes = 30 days (`default_ttl` is minutes)" and add: the block is dead config under `langgraph dev` (inmem `sweep_ttl` no-op), so the Phase 3 sweep is new enforcement of the config's intent.
2. Divergence ledger (`tests/contract/golden/README.md`, the home Phase 0 established): entry for the thread-retention divergence — platform `strategy="delete"` drops the thread entirely; `agent_runtime` keeps `rt_thread`/`rt_run` and deletes only checkpoint data + `rt_thread_event`, because thread metadata is app state.
3. `langgraph.json` is left untouched — it is the local-dev-only path and its TTL block is inert there.

**Verification:** `grep -n "12 h" docs/MIGRATION.md` → no output; ledger entry present.

### T4 — `docs/INSTALLATION.md` production section

**File:** `docs/INSTALLATION.md` §10 ("Production deployment", currently lines ~636–664) plus the stale snippet it embeds.

1. Replace the "deploy on LangGraph Cloud / Platform" backend instructions with the self-hosted path: provision Postgres, set `DATABASE_URL`, run `uv run uvicorn agent_runtime.app:app` (or the equivalent of Phase 1's topology decision), point `LANGGRAPH_URL` at it; note `COMPLETION_WEBHOOK_URL` must be a non-loopback reachable address (`agent/dispatch.py` refuses loopback). Reference the T2 TTL env knobs.
2. Fix the embedded `langgraph.json` snippet (it shows three graphs under stale `agent.server:*` paths; the real file has five graphs under `agent.graphs.*`) and reframe it: `langgraph.json` / `make dev` is the **local-dev-only** path, permitted under ELv2's non-production terms — never the production runtime.
3. Add a short **"Separate SaaS dependencies (out of scope)"** note: `SANDBOX_TYPE=langsmith` sandboxes (§4c) and LangSmith tracing (§4a) are paid SaaS dependencies **untouched by this migration** — each has its own follow-up; self-hosters can switch `SANDBOX_TYPE` per `docs/CUSTOMIZATION.md` today.
4. Dashboard/Vercel instructions stay; update only the rewrite-destination wording from "hosted LangGraph deployment" to "your backend deployment".

**Verification:** production section contains no LangGraph Cloud deploy steps; `grep -n "agent.server:traced_agent" docs/INSTALLATION.md` → no output; SaaS note present.

### T5 — Phase gate + commit

Run the acceptance list (§2), then commit: `chore: hygiene — lint guard, checkpoint TTL sweep, docs (phase 3)` with trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

## 2. Acceptance criteria

```bash
make lint && make typecheck && make test                     # hermetic, Docker stopped
uv run ruff check .                                          # TID251 active, zero findings
uv run pytest -vvv tests/agent/test_import_hygiene.py        # incl. agent_runtime.app probe
docker compose -f docker-compose.test.yml up -d
uv run pytest -vvv tests/agent_runtime/test_ttl_sweep.py     # 4 tests green
CONTRACT_RUNTIME=embedded uv run pytest -vvv tests/contract/ # sweep changed no wire behavior
docker compose -f docker-compose.test.yml down
```
Plus: `tests/agent/test_no_bare_get_client.py` deleted; MIGRATION "12 h" corrected; divergence-ledger entry exists; INSTALLATION §10 rewritten with the SaaS-deps note; `langgraph-cli[inmem]` still present in `pyproject.toml` dependencies (unchanged, by decision); phase commit on `feat/fastapi-runtime`.

## 3. Risks

- **Phase-1 name drift (A1–A5).** The sweep SQL and scheduler import fail loudly against wrong names. *Mitigation:* §0a reconciliation is a blocking pre-step of T2; the names appear only in `ttl_sweep.py` and its test module.
- **Sweeping a thread that is about to run.** Victim selection excludes in-flight runs, but a run created between the SELECT and `adelete_thread` races the sweep. *Mitigation:* accepted — a 30-day-idle thread losing checkpoints to a same-second new run is benign (the run starts from empty state, identical to post-sweep); not worth locking. Note the race in `ttl_sweep.py`.
- **`per-file-ignores` breadth.** Ignoring TID251 in `tests/**` means a test could call bare `get_client()`. *Mitigation:* tests run in dev context where that is licensed; the production invariant covers `agent/` + `agent_runtime/`, which the ban does police.
- **checkpoint-postgres pin moves past 3.x.** `adelete_thread`'s signature or table set could change; a future version may grow native TTL (none in 3.1.0 — verified). *Mitigation:* A6's re-verify note; the four sweep tests fail immediately on any wiring change; if native TTL appears, replace `ttl_sweep.py`'s deletion body with it and keep the tests as the behavioral pin.

## 4. Effort

| Task | What | Effort |
|---|---|---|
| T1 | TID251 stanza + per-file-ignores; delete subsumed test; one import-hygiene case | S |
| T2 | `ttl_sweep.py` + scheduler wiring + 4 Postgres tests | M |
| T3 | MIGRATION correction + divergence-ledger entry | S |
| T4 | INSTALLATION §10 rewrite + SaaS note | S |
| T5 | Gate + commit | S |

Matches MIGRATION §6: "small — validation and dependency hygiene, not new application logic." T2 is the only code of substance.
