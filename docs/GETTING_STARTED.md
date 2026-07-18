# Getting Started — run Open SWE for real, locally

This is the shortest path from a fresh clone to *using* the product with
real integrations. The full reference (screenshots, Slack/Linear, every env
var) is `docs/INSTALLATION.md`; this page is the condensed operator's path.

**Mental model:** `http://localhost:2024` is the *backend* (API + webhooks —
`/` is intentionally not a page). The product UI is the dashboard app in
`ui/`, served separately on `http://localhost:3000`. Users interact through
the dashboard, or through Slack/GitHub/Linear once those webhooks are wired.

---

## 1. What you must set up (one-time)

### a. A GitHub App — the agent's identity (required)

The agent clones your repos, pushes branches, and opens PRs as this app;
the dashboard's "Sign in with GitHub" also runs through it. Follow
`INSTALLATION.md` §3 (a–d). You come away with six values:

- `GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY`, `GITHUB_APP_INSTALLATION_ID`
  (install the app on the repos you want it to work on)
- `GITHUB_APP_CLIENT_ID`, `GITHUB_APP_CLIENT_SECRET` (dashboard login)
- `GITHUB_WEBHOOK_SECRET` (only used once you wire GitHub webhooks, but set
  it at creation time)

For **local dashboard use, no ngrok is needed** — register the OAuth
callback as `http://localhost:2024/dashboard/api/auth/callback`. ngrok
becomes necessary only for *inbound* webhooks (step 4).

### b. An LLM

Any one of: `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` / `GOOGLE_API_KEY` /
`FIREWORKS_API_KEY` — or a **local LiteLLM proxy** (no cloud spend):

```bash
LLM_PROVIDER="litellm"
LITELLM_BASE_URL="…"        # your proxy
LITELLM_API_KEY="…"
LITELLM_MODEL="minimax-m3"
DEFAULT_MODEL_ID="litellm:minimax-m3"
DEFAULT_MODEL_EFFORT="none"
```

### c. Local secrets + defaults

Append to `.env` (project root):

```bash
# Where the agent works when you don't name a repo
DEFAULT_REPO_OWNER="your-github-org-or-user"
DEFAULT_REPO_NAME="your-repo"

# Dashboard session + token encryption
DASHBOARD_JWT_SECRET="$(openssl rand -hex 32)"
TOKEN_ENCRYPTION_KEY="$(openssl rand -base64 32)"
DASHBOARD_API_BASE_URL="http://localhost:2024"
DASHBOARD_BASE_URL="http://localhost:3000"
DASHBOARD_ALLOWED_ORIGINS="http://localhost:3000"
CONFIGURED_ADMINS="your-github-login"     # unlocks the admin pages

# Sandbox: where the agent runs shell commands.
# "local" = directly on your machine, no isolation — fine for trying it out.
# For isolated sandboxes see INSTALLATION.md §4c (LangSmith) or CUSTOMIZATION.md.
SANDBOX_TYPE="local"
```

Notes: `SANDBOX_TYPE=local` means the agent executes git/shell commands on
your machine as you. `make dev` defaults to it automatically when nothing is
configured. Tracing (`LANGSMITH_*`) is optional — leave it out to start.

## 2. Boot it

Two terminals:

```bash
# Terminal 1 — backend (Docker must be running; Postgres is bundled)
make install
make dev                      # → http://localhost:2024  (API; /ok is the health probe)

# Terminal 2 — dashboard UI
cd ui
pnpm install                  # (or bun install)
echo 'VITE_DASHBOARD_API_BASE_URL="http://localhost:2024"' > .env
pnpm run dev                  # → http://localhost:3000
```

Open **http://localhost:3000** → *Sign in with GitHub*.

## 3. Using it — the flows

### Flow 1: Dashboard chat (works immediately, no webhooks)

1. **New Agent** (left sidebar) → pick the repo → describe the task
   ("add input validation to the signup endpoint and open a PR").
2. Watch it stream: the agent clones the repo into its sandbox, edits,
   commits, pushes a branch, opens a **draft PR**, and links it in the
   thread.
3. **Follow-ups:** keep typing in the same thread — while it's busy your
   message queues and is picked up at its next step; the agent continues
   with your new context.
4. **Plan mode:** ask it to "plan first" — it writes a reviewable plan, you
   comment/approve in the plan view, then it implements.
5. **Cancel:** the stop control on a running thread interrupts the run;
   progress up to the last step is checkpointed.
6. **Settings** (`/my-settings`): pick your default model/effort, toggle
   Always-Create-PRs, manage enabled repos; admin pages appear if you're in
   `CONFIGURED_ADMINS`.

### Flow 2: GitHub triggers (needs a public webhook URL)

1. `ngrok http 2024`, then set the GitHub App's webhook URL to
   `https://<your-ngrok>/webhooks/github` (INSTALLATION.md §5-GitHub).
2. Now: **@mention the app in an issue or PR comment** with an instruction —
   it starts a run on that context and replies on the thread.
   Opening a PR (or marking ready-for-review) on an enabled repo triggers an
   automatic **code review** with inline findings.

### Flow 3: Slack (optional; INSTALLATION.md §5-Slack)

Create the Slack app, fill the `SLACK_*` env block, point its events URL at
`https://<your-ngrok>/webhooks/slack`. Then, in any channel the bot is in:

- `@OpenSWE fix the flaky retry test and open a PR` → it acks, works, and
  replies with the PR link in-thread.
- Reply in the same thread (no @ needed once it has participated) — while
  it's busy, untagged follow-ups queue and coalesce; a tagged message
  interrupts and redirects it.
- Ask it to "check back tomorrow at 9am" — it schedules a one-shot wakeup.

### Flow 4: Linear (optional; INSTALLATION.md §5-Linear)

Mention the agent in a Linear issue comment; it works the issue and posts
progress back.

## 4. Sanity checks when something's off

```bash
curl http://localhost:2024/ok                  # backend up → {"ok":true}
curl http://localhost:2024/health              # webapp mounted
docker compose -f docker-compose.test.yml ps   # Postgres healthy?
```

- Dashboard login loops → check `GITHUB_APP_CLIENT_ID/SECRET`,
  `DASHBOARD_JWT_SECRET`, `DASHBOARD_ALLOWED_ORIGINS`, and that the App's
  callback URL is exactly `http://localhost:2024/dashboard/api/auth/callback`.
- Agent can't clone/push → the GitHub App isn't installed on that repo
  (§3d), or `GITHUB_APP_INSTALLATION_ID` is wrong.
- Webhook silence → ngrok URL changed; update the app's webhook URL.
- More: INSTALLATION.md §Troubleshooting.
