# LangSmith decoupling: observability-only, never load-bearing

**Problem.** `LANGSMITH_API_KEY_PROD` — nominally a tracing/observability key —
currently *selects the GitHub auth mode* and gates whether triggered runs
execute at all. A fresh self-hosted install that sets `LANGSMITH_API_KEY` (the
standard tracing var) but not the `_PROD` variant silently drops every
GitHub-triggered run: the webhook is accepted, the handler logs
"No GitHub token for thread, skipping" to the dev terminal, and nothing else
happens. (This exact failure cost a real onboarding session on 2026-07-19.)

**Principle.** LangSmith is an observability vendor to this codebase, plus two
clearly-labeled opt-in services (LLM Gateway, LangSmith sandboxes). No
auth decision, run-execution decision, or user-identity lookup may depend on a
LangSmith credential being present or absent.

---

## 1. Inventory: every path that reads a LangSmith credential

Ranked by criticality. "Key" below means `LANGSMITH_API_KEY_PROD` unless noted.

### C1 — Auth-mode selection (the load-bearing bug)

`agent/utils/auth.py:64-72`:

```python
def is_bot_token_only_mode() -> bool:
    return bool(LANGSMITH_API_KEY and not X_SERVICE_AUTH_JWT_SECRET and not USER_ID_API_KEY_MAP)
```

`LANGSMITH_API_KEY` here is `os.environ.get("LANGSMITH_API_KEY_PROD")`
(`auth.py:47`) — the tracing key is a proxy for "we're deployed", and its
presence/absence flips between three behaviors at every call site:

| Call site | Key set (bot mode) | Key missing |
|---|---|---|
| `agent/webhooks/common.py:1339` `_get_or_resolve_thread_github_token` — GitHub comment webhook | GitHub App installation token → run proceeds | LangSmith email→user→token chain (C2) → returns `None` → run **silently skipped** |
| `agent/webhooks/slack.py:503` — Slack mention pre-run gate | exempt from the user-token requirement → run proceeds | run blocked, "link your GitHub account" prompt posted |
| `agent/utils/auth.py:425-500` `resolve_github_token` — in-graph (server.py, sandbox git/proxy) | falls back to installation token when no dashboard user token | raises `GitHubUserAuthRequired` |

### C2 — Per-user token brokering via LangSmith SaaS

`agent/utils/auth.py`: `get_ls_user_id_from_email` (`:117`),
`get_github_token_for_user` (via `LANGSMITH_HOST_API_URL` +
`GITHUB_OAUTH_PROVIDER_ID`), `get_secret_key_for_user`
(`X_SERVICE_AUTH_JWT_SECRET` service JWTs), chained by
`resolve_github_token_from_email` (`:212`) and `resolve_token_from_email`
(`:314`). Resolves work-email → LangSmith user → LangSmith-brokered GitHub
OAuth token. On failure it posts user-facing comments that instruct people to
get "invited to the main LangSmith organization" (`auth.py:334-346`) — a SaaS
tenancy requirement leaking into end-user UX.

Callers: webhook-side `_get_or_resolve_thread_github_token`
(`webhooks/common.py:1360`) and in-graph `resolve_github_token` for
`source == "github"` and the email fallback (`auth.py:481-494`).

**The replacement already exists.** `_resolve_dashboard_user_token`
(`auth.py:388-409`) resolves per-user tokens from the dashboard's own GitHub
OAuth store (`agent/dashboard/profiles.py`, `oauth_tokens` namespace,
refresh handled by `get_valid_access_token`) with zero LangSmith involvement —
but it is only wired for `source in ("slack", "linear", "dashboard",
"schedule")`, not for `source == "github"` and not in the webhook-side
resolver.

### C3 — LLM Gateway auth fallback

`agent/utils/gateway.py:50`: gateway calls authenticate with
`LANGSMITH_GATEWAY_API_KEY` **falling back to `LANGSMITH_API_KEY_PROD`**.
The gateway itself is a LangSmith product and inherently opt-in
(`LANGSMITH_GATEWAY_ENABLED` / `use_gateway`, `agent/utils/model.py:159`), so
using a LangSmith key *when the gateway is enabled* is correct — the problem
is only the silent fallback coupling the tracing key to model access.

### C4 — Analyzer finding-outcomes dataset

`agent/utils/reviewer_outcomes.py:67` builds a `LangSmithClient` (prefers the
prod key) and stores reviewer finding outcomes in a **LangSmith dataset**.
Consumed by the analyzer's continual-learning mode
(`agent/tools/read_finding_outcomes.py`, `agent/analyzer.py`) and fed by
feedback paths (`update_finding`, `slack_feedback`, `github_feedback`,
`resolve_finding_thread`). Without a key, continual learning silently loses
its data source. This is *application state living in an observability
vendor's product*.

### C5 — LangSmith sandbox provider

`agent/integrations/langsmith.py:56-59` (`SANDBOX_TYPE=langsmith`) uses
`LANGSMITH_API_KEY` → `LANGSMITH_API_KEY_PROD`. Legitimately LangSmith-bound —
it *is* their sandbox service — and provider selection is explicit via
`SANDBOX_TYPE`, with five non-LangSmith providers available. No decoupling
needed beyond documentation; keep it out of any auth logic.

### C6 — Observability proper (correct usage, keep)

- Tracing wrappers `traced_agent` / `traced_reviewer_agent` / `traced_analyzer`
  and `LANGCHAIN_TRACING_V2` — degrade gracefully when unset.
- "View trace" links: `agent/utils/langsmith.py` (project-id resolution),
  `agent/dashboard/thread_api.py` trace-link fields.
- `agent/dashboard/thread_api.py:105` `_langgraph_proxy_headers` — attaches
  `X-API-Key` to `LANGGRAPH_URL` proxy calls. Needed only by the removed
  LangGraph Platform deploy path; `agent_runtime` ignores it.

---

## 2. Target state

| Concern | Today | Target |
|---|---|---|
| Auth mode | inferred from `LANGSMITH_API_KEY_PROD` presence | explicit `GITHUB_AUTH_MODE=bot\|per-user\|auto` (default `auto`) |
| Per-user GitHub tokens | LangSmith OAuth broker (C2) | dashboard OAuth store only (`_resolve_dashboard_user_token` path) |
| Missing auth at trigger time | silent skip (log line only) | surfaced to the user in-channel (comment/DM with dashboard login link) |
| Finding outcomes | LangSmith dataset | Postgres store namespace; LangSmith mirror optional |
| Gateway / sandbox | fall back to `_PROD` key | their own explicit keys; opt-in services |
| Tracing / trace links | `_PROD` key | unchanged — the only *required* consumer of the key |

`auto` mode: `bot` when GitHub App creds exist (`GITHUB_APP_ID` +
`GITHUB_APP_PRIVATE_KEY` + installation) — the self-hosted default; `per-user`
adds nothing implicit: dashboard-store user tokens are *always* preferred when
present (same precedence `resolve_github_token` has today), the mode only
decides the fallback (installation token vs. block-and-prompt).

## 3. Workstreams

### W1 — Explicit auth mode (small, unblocks everything)

1. Add `GITHUB_AUTH_MODE` to config; reimplement `is_bot_token_only_mode()`
   as `github_auth_mode() == "bot"` with the `auto` resolution above. Delete
   every read of `LANGSMITH_API_KEY_PROD` / `X_SERVICE_AUTH_JWT_SECRET` /
   `USER_ID_API_KEY_MAP` from the decision.
2. Sweep the three C1 call sites; keep their *shape* (the middleware and
   webhook contracts don't change), only the predicate.
3. **Fail loudly**: in `_get_or_resolve_thread_github_token`'s `None` path and
   the `email_for_login` miss in `process_github_comment`
   (`agent/webhooks/github.py:685-692`), react ❌ / reply on the PR with the
   reason and the dashboard-login (or admin-mapping) fix, instead of
   `logger.warning(...); return`. A dropped trigger must be visible where it
   was issued.
4. Tests: with **zero** LangSmith env vars set and GitHub App creds present, a
   GitHub `issue_comment` webhook produces a run on the installation token;
   with `GITHUB_AUTH_MODE=per-user` and no stored token, the PR gets an
   actionable auth comment, not silence.

### W2 — Retire the LangSmith token broker (C2)

1. Wire `source == "github"` into the `_resolve_dashboard_user_token` branch
   of `resolve_github_token` (`github_login` is in `configurable` for GitHub
   runs too) and use the same store lookup in webhook-side
   `_get_or_resolve_thread_github_token`: thread cache → dashboard store by
   login → mode fallback (bot token, or prompt).
2. Delete `get_ls_user_id_from_email`, `get_github_token_for_user`,
   `get_secret_key_for_user`, `resolve_github_token_from_email`,
   `resolve_token_from_email`, and the `GITHUB_OAUTH_PROVIDER_ID` /
   `LANGSMITH_HOST_API_URL` / `X_SERVICE_AUTH_JWT_SECRET` /
   `USER_ID_API_KEY_MAP` config. Rewrite the auth-failure comments
   (`auth.py:322-366`) to point at `DASHBOARD_BASE_URL` login — the org-gated
   GitHub OAuth self-onboard flow (`_post_account_link_prompt`) already does
   exactly this for Slack; reuse its copy.
3. INSTALLATION.md: fold step 4b ("GitHub OAuth via LangSmith") into the
   dashboard-OAuth story; move all `LANGSMITH_*` env vars into an
   "Observability (optional)" section. `LANGSMITH_API_KEY_PROD` stays *only*
   as the tracing/trace-links key (or is folded into `LANGSMITH_API_KEY` —
   decide once W1-W4 remove every non-tracing reader).

### W3 — Finding outcomes to Postgres (C4)

1. Store outcomes in the LangGraph Store (e.g. namespace
   `["reviewer_outcomes", owner_repo]`) — the store is already Postgres-backed
   and shared HTTP/in-graph (flow-change.md Flow 3). Same record shape the
   dataset rows carry today.
2. `read_finding_outcomes` reads the store; `reviewer_outcomes.py` keeps an
   *optional* LangSmith-dataset mirror when a key is configured (it's genuinely
   useful for eval tooling) behind a `try/except` that can never fail the
   feedback path.
3. One-shot backfill script for existing dataset rows; run before flipping the
   reader.

### W4 — Fallback hygiene (C3, C6)

1. `gateway.py`: authenticate with `LANGSMITH_GATEWAY_API_KEY` only; if the
   gateway is enabled and the key is missing, raise at model-construction time
   with a clear message (never fall back to the tracing key).
2. Drop `_langgraph_proxy_headers`' API-key injection (`thread_api.py:99-107`)
   — dead weight against `agent_runtime`.
3. `integrations/langsmith.py` may keep its key lookup (provider-scoped); add
   a comment marking it the *only* sanctioned non-observability LangSmith key
   reader besides the gateway.

### W5 — Regression pins

- Lint-style guard (mirroring the migration's license guard): outside
  `agent/utils/langsmith.py`, `agent/utils/gateway.py`,
  `agent/integrations/langsmith.py`, and tracing wrappers, no module may read
  a `LANGSMITH_*` env var. A grep-based test keeps the coupling from creeping
  back.
- e2e: the fake-boundary boot (`tests/e2e`) already unsets LangSmith vars;
  add an assertion that the GitHub webhook → run path completes under that
  environment (this is the exact scenario that failed silently).

## 4. Sequencing & risk

W1 is independent and should land first — it fixes the silent-drop failure
mode outright. W2 depends on W1's mode plumbing. W3 and W4 are independent of
each other and of W2. W5 lands last.

Risks: (a) deployments currently relying on the LangSmith broker for per-user
tokens must onboard users through the dashboard OAuth flow before W2 —
mitigate with a release note and the W1 `auto` default keeping them on bot
tokens rather than breaking; (b) outcome-data continuity for the analyzer —
mitigated by the W3 backfill; (c) any external tooling reading the LangSmith
outcomes dataset keeps working via the optional mirror.
