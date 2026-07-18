#!/bin/bash
# Embedded-runtime e2e webServer command (docs/fast-api-migration/phase-2.md T1).
#
# Boots Postgres (compose), drops/recreates the dedicated e2e database —
# langgraph dev forgot everything between runs and the specs assume that —
# then execs uvicorn serving agent_runtime with the e2e graph + harness
# registered via AGENT_RUNTIME_CONFIG.
set -euo pipefail

cd "$(dirname "$0")/../.."   # repo root: the e2e config uses ./-relative paths

E2E_PORT="${E2E_PORT:-2024}"
E2E_DB="openswe_e2e"
# Coupled to docker-compose.test.yml by design; the TEST_POSTGRES_DSN
# escape hatch does not apply to this leg (compose is a prerequisite).
# Hermeticity note: with reuseExistingServer (local runs) a leftover server
# on :2024 skips this reset — kill it first for a from-scratch run.
ADMIN_DSN="postgresql://openswe:openswe@localhost:54329/openswe_test"

docker compose -f docker-compose.test.yml up -d --wait

uv run python - <<EOF
import psycopg
with psycopg.connect("${ADMIN_DSN}", autocommit=True) as conn:
    conn.execute('DROP DATABASE IF EXISTS ${E2E_DB} WITH (FORCE)')
    conn.execute('CREATE DATABASE ${E2E_DB}')
EOF

export DATABASE_URL="postgresql://openswe:openswe@localhost:54329/${E2E_DB}"
export AGENT_RUNTIME_CONFIG="tests/e2e/langgraph.e2e.json"
export LANGGRAPH_URL="http://127.0.0.1:${E2E_PORT}"
export LANGSMITH_TRACING=false
export LANGCHAIN_TRACING_V2=false

exec uv run uvicorn agent_runtime.app:app --host 127.0.0.1 --port "${E2E_PORT}" --log-level info
