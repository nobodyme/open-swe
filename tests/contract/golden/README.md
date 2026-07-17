# Golden transcripts — the `langgraph dev` baseline

Captured by `tests/contract/test_langgraph_api_contract.py` against `langgraph dev`
(the Elastic-licensed `langgraph-api` inmem runtime) serving the deterministic
contract graph (`tests/contract/contract_graph.py`). They are Phase 1's parity
oracle: `agent_runtime` must reproduce these responses byte-for-byte after
normalization, or record an entry in the divergence ledger below.

**Run via `make contract-test`** — bare `pytest tests/contract` selects nothing
by design (`addopts = "-m 'not contract and not litellm'"` in `pyproject.toml`).
A missing golden is a HARD FAILURE, never silently re-recorded — recording
requires an explicit `CONTRACT_RECORD=1 make contract-test` (delete the stale
file first for a deliberate re-record). The server boots in a session tmpdir
(cwd controls both graph-path resolution and the inmem `.langgraph_api` state
dir), so every session starts from an empty baseline without touching the
developer's repo-root dev state.

## Normalization rules (`tests/contract/normalize.py`)

- UUIDs → `<uuid-N>` in order of first appearance (identity relationships
  survive within one payload).
- Redis-style stream event ids (`<ms>-<seq>`) → `<event-N>`.
- ISO-8601 timestamps → `<ts>`; 13-digit millisecond-epoch JSON numbers → `<ts-ms>`.
- Loopback `host:port` → `127.0.0.1:<port>`.
- Dict keys are sorted (server JSON key order is not part of the contract).
- Version/license-tier env keys (`langgraph_version`, `langgraph_api_version`,
  `langgraph_plan`, `langgraph_host`, `langgraph_api_url`) → `<env>` — they
  track dependency versions, not contract semantics, and won't exist on the
  Phase 1 server.
- SSE `seq` monotonicity is asserted **outside** the golden diff
  (`assert_event_ids_monotonic`); `parse_sse` only parses `\n\n`-terminated
  blocks so a mid-delivery partial event is never acted on.

## What each golden pins

| Golden | Contract fact |
|---|---|
| `thread_create.json`, `thread_get_after_update.json` | Thread shape; metadata **merge** semantics on update. |
| `thread_create_if_exists_do_nothing.json` | Idempotent create keeps the FIRST create's metadata (9 webhook/thread-ensure call sites depend on the no-409). |
| `thread_search_select_subset.json` | `select=["thread_id","status","metadata"]` column projection (dashboard list view). |
| `thread_get_state.json`, `thread_history_wire.json` | SDK `get_state` + raw `POST /threads/{id}/history` — the wrappers Phase 1 builds over `aget_state`/`aget_state_history`. |
| `run_create.json` | Run record for the exact `create_durable_run` kwargs (multitask interrupt, if_not_exists create, durability sync, stream_resumable, webhook). |
| `run_complete_webhook_payload.json` | Completion-webhook POST body (see ledger: dev delivers to loopback). |
| `run_double_dispatch_statuses.json` | Busy thread + second interrupt run → first `"interrupted"`, second `"success"` (`completion.py` treats interrupted as healthy). Measured semantics: interrupt cancels at the next STEP BOUNDARY (the in-flight tool step finishes first, ~the full busy window), not a preemptive mid-tool kill; the superseded run never emits its final message (asserted against thread state). |
| `run_stream_mode_*.json` (7 files) | One golden per dashboard run-level stream mode — `values`, `updates`, `messages`, `messages-tuple`, `checkpoints`, `events` (protocol envelope only) — the exact modes Phase 1 must map onto MIT `astream`. `run_stream_mode_tools.json` pins that "tools" is REJECTED (422) as a run-level mode: it is only a v2 event-stream channel. |
| `run_cancel_raw_route.json` | `POST /threads/{id}/runs/{run_id}/cancel?wait=0&action=interrupt` (the route thread_api.py:1979 proxies) → status code; terminal run status `"interrupted"` asserted alongside. |
| `thread_state_wire.json` | Raw `GET /threads/{id}/state` (review_chat_api.py:510 proxies it verbatim). |
| `runs_cancel_many_response.json` | `cancel_many(action="interrupt")` returns `null` (not the id list). |
| `run_status_after_cancel.json` | Cancelled pending run terminal status: `"interrupted"`. |
| `store_get_item.json` | Store item envelope (namespace/key/value/timestamps). |
| `commands_run_start_response.json` | v2 `POST /threads/{id}/commands` `run.start` response: `{type, id, result.run_id, meta.applied_through_seq}`. |
| `stream_events_transcript.json` | Live SSE transcript of `POST /threads/{id}/stream/events` (channels: values/updates/messages/tools/lifecycle): lifecycle running → values ×2 → lifecycle completed; SSE `id:` carries the session `seq`, body `event_id` carries the durable id. |
| `join_stream_after_completion.json` | `runs.join_stream` on a finished run replays nothing (see ledger). |

## Contract runtime switch (Phase 1)

`CONTRACT_RUNTIME=platform` (default) boots `langgraph dev` — the golden
baseline. `CONTRACT_RUNTIME=embedded` boots `uvicorn agent_runtime.app:app`
over a fresh Postgres database (compose) and must match the same goldens.
Both are Phase 1 acceptance gates; the platform run proves the suite wasn't
bent to fit the new runtime.

## Divergence ledger (dev behavior Phase 1 may deliberately differ from)

1. **Store nested filters**: dev's inmem store does NOT match dotted nested
   filters (`filter={"prefs.model": ...}`). The flat top-level filter form —
   the one the app actually uses (`{"created_by": login}`, schedules.py) — is
   asserted unconditionally and passes; the dotted probe is recorded as a
   skip. Also pinned: `store.get_item` on a missing/deleted item returns
   `None` (200), unlike `threads.get` which 404s — preserve the asymmetry.
2. **Crons**: dev implements `crons.create/create_for_thread(end_time=…,
   timezone=…)/search/delete` (both tests pass against dev) — Phase 1
   implements the same surface (`schedule_thread_wakeup.py` depends on
   `create_for_thread`).
3. **Loopback completion webhooks**: platform docs say loopback webhook URLs
   are rejected at run create (`dispatch.py:34-43`), but dev happily delivers
   them (`run_complete_webhook_payload.json` was captured via
   `http://127.0.0.1:<port>/...?token=...`). Phase 1 should deliver like dev;
   the `?token=` auth is appended by open-swe, not platform signing.
4. **`runs.join` on a `/commands`-started run** returns a v2 protocol event
   envelope (`{type, method, params, seq}`), not the final-values dict it
   returns for SDK-created runs. The dashboard never joins such runs; Phase 1
   must not accidentally "fix" this into an app-visible change.
5. **No post-completion event replay**: `/threads/{id}/stream/events` is a
   live tail (reconnect-with-`since` only covers the session buffer), and
   `runs.join_stream` on a finished run ends immediately. The dashboard only
   live-tails, so Phase 1's run-event log must match the live path; anything
   stronger (full history replay) would be a superset, recorded here if built.
6. **Routes beyond the phase-1.md T4–T10 tables (D6 audit):** the runtime
   also answers `GET /ok` (boot probe), `GET /threads/{t}/runs/{r}/join`,
   `POST /threads/{t}/runs/stream`, and `GET /threads/{t}/runs/{r}/stream` —
   each exists because THIS contract suite drives it (the D6-sanctioned
   justification); no agent/ code calls them. Also recorded: `POST
   /runs/cancel` with `run_ids` but no `thread_id` is a no-op (both app
   callers always send thread_id), and `ThreadSearchBody.values` filtering
   is accepted-and-ignored (no app caller filters threads by values).
7. **Embedded runtime deltas (Phase 1, both deliberate):**
   - `agent_runtime` SUPPORTS dotted nested store filters (Postgres store) —
     a superset of dev's inmem store (ledger item 1); the app's flat filters
     behave identically on both.
   - Run-level `stream_mode="events"` (the `astream_events` firehose) is
     accepted-and-ignored by `agent_runtime` (no app caller, no v2 channel —
     D6); the per-mode golden test skips it under `CONTRACT_RUNTIME=embedded`.
   - The v2 `messages` channel carries only streamed token chunks
     (`messages/partial`); whole-message completes stay on the SDK stream —
     matching the dev transcript's lifecycle+values-only shape for
     non-streaming models.

## Cross-phase name ledger (binding, from docs/fast-api-migration/phase-0.md §1)

- `contract` pytest marker + `addopts` exclusion — the canonical convention for
  every Postgres-/server-requiring test package (Phase 1's `tests/agent_runtime/`
  reuses it or adds its own marker to the same exclusion, with skip-when-absent).
- `make contract-test` — the only supported way to run this suite.
- `tests/support/postgres.py` + `TEST_POSTGRES_DSN` — ephemeral-Postgres fixture.
- `RUNTIME=platform|embedded` — Playwright webServer switch
  (`embedded` = `uvicorn agent_runtime.app:app`).
- `tests/agent/test_no_bare_get_client.py` — guard that Phase 3 upgrades to a
  ruff/CI rule.
- tests/e2e import ban — unit/contract tests never import `tests/e2e` modules.
