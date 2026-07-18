.PHONY: all format format-check lint typecheck test tests integration_tests contract-test help run dev

# Default target executed when no arguments are given to make.
all: help

######################
# DEVELOPMENT
######################

# Default dev runtime: the self-hosted MIT agent_runtime over Postgres
# (docs/fast-api-migration/phase-2.md T6). Serves all graphs + the webapp on
# :2024 — the same origin LANGGRAPH_URL points at.
# NOTE: the bundled Postgres (docker-compose.test.yml) is tmpfs-backed — dev
# state evaporates when the container restarts, matching langgraph dev's
# forget-everything behavior. Overriding DEV_DATABASE_URL points uvicorn at
# your own Postgres; you then own creating that database (the compose-up and
# CREATE DATABASE lines below still target the bundled instance).
DEV_DATABASE_URL ?= postgresql://openswe:openswe@localhost:54329/openswe_dev

dev:
	docker compose -f docker-compose.test.yml up -d --wait
	uv run python -c "import psycopg; c = psycopg.connect('postgresql://openswe:openswe@localhost:54329/openswe_test', autocommit=True); c.execute('CREATE DATABASE openswe_dev') if not c.execute(\"SELECT 1 FROM pg_database WHERE datname='openswe_dev'\").fetchone() else None"
	@if [ -z "$${SANDBOX_TYPE:-}" ] && ! grep -qE '^SANDBOX_TYPE=' .env 2>/dev/null; then \
		echo "SANDBOX_TYPE not configured — defaulting to 'local' for dev (commands run on this host, no isolation)."; \
	fi
	SANDBOX_TYPE=$${SANDBOX_TYPE:-$$(grep -E '^SANDBOX_TYPE=' .env 2>/dev/null | head -1 | cut -d= -f2- | tr -d "\"'" )} ; \
	DATABASE_URL=$(DEV_DATABASE_URL) SANDBOX_TYPE=$${SANDBOX_TYPE:-local} \
	AGENT_RUNTIME_LOG_LEVEL=$${AGENT_RUNTIME_LOG_LEVEL:-info} \
		uv run uvicorn agent_runtime.app:app --host 127.0.0.1 --port 2024 --reload \
			--reload-dir agent --reload-dir agent_runtime \
			--timeout-graceful-shutdown 5  # open SSE streams never drain; without a cap a reload wedges forever

run:
	uv run uvicorn agent.webapp:app --reload --port 8000

install:
	uv sync --extra dev

######################
# TESTING
######################

TEST_FILE ?= tests/

test tests:
	@if [ -d "$(TEST_FILE)" ] || [ -f "$(TEST_FILE)" ]; then \
		uv run pytest -vvv $(TEST_FILE); \
	else \
		echo "Skipping tests: path not found: $(TEST_FILE)"; \
	fi

# Contract suite: needs Docker (ephemeral Postgres) and boots agent_runtime on
# an ephemeral port. Excluded from `make test` via pyproject addopts.
# Missing goldens fail hard; record new ones with CONTRACT_RECORD=1 make contract-test.
contract-test:
	uv run pytest -vvv -m contract tests/contract/

integration_tests:
	@if [ -d "tests/integration_tests/" ] || [ -f "tests/integration_tests/" ]; then \
		uv run pytest -vvv tests/integration_tests/; \
	else \
		echo "Skipping integration tests: path not found: tests/integration_tests/"; \
	fi

######################
# LINTING AND FORMATTING
######################

PYTHON_FILES=.

lint:
	uv run ruff check $(PYTHON_FILES)
	uv run ruff format $(PYTHON_FILES) --diff

format:
	uv run ruff format $(PYTHON_FILES)
	uv run ruff check --fix $(PYTHON_FILES)

format-check:
	uv run ruff format $(PYTHON_FILES) --check

typecheck:
	npx --yes basedpyright agent agent_runtime tests

######################
# HELP
######################

help:
	@echo '----'
	@echo 'dev                          - run agent_runtime + Postgres (default dev runtime)'
	@echo 'run                          - run webhook server'
	@echo 'install                      - install dependencies (incl. dev extras)'
	@echo 'format                       - run code formatters'
	@echo 'lint                         - run linters'
	@echo 'typecheck                    - run basedpyright on agent/ and tests/'
	@echo 'test                         - run unit tests'
	@echo 'contract-test                - run contract suite (Docker Postgres + agent_runtime)'
	@echo 'integration_tests            - run integration tests'
	@echo '----'
