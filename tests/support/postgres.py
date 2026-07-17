"""Ephemeral Postgres for tests that need a real database.

Resolution order:

1. ``TEST_POSTGRES_DSN`` env var — escape hatch for machines without Docker
   (point it at any scratch database; tests may create/drop tables in it).
2. ``docker compose -f docker-compose.test.yml up -d --wait`` — the default
   path; tmpfs-backed Postgres 16 on localhost:54329, torn down at session end
   only if this fixture started it.

The default ``make test`` run never imports this module: every test that uses
it must carry the ``contract`` (or a later phase's equivalent) marker, which
``pyproject.toml``'s ``addopts`` excludes by default (docs/fast-api-migration/
phase-0.md §1 name ledger).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.test.yml"
COMPOSE_DSN = "postgresql://openswe:openswe@localhost:54329/openswe_test"


def _docker_available() -> bool:
    docker = shutil.which("docker")
    if not docker:
        return False
    probe = subprocess.run(
        [docker, "info"], capture_output=True, text=True, timeout=30, check=False
    )
    return probe.returncode == 0


def _wait_ready(dsn: str, timeout_s: float = 30.0) -> None:
    import psycopg

    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(dsn, connect_timeout=3):
                return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.5)
    raise RuntimeError(f"Postgres at {dsn} not ready after {timeout_s}s: {last_error}")


@pytest.fixture(scope="session")
def postgres_dsn() -> Iterator[str]:
    """Session-scoped DSN for an ephemeral (or user-provided) Postgres."""
    explicit = os.environ.get("TEST_POSTGRES_DSN")
    if explicit:
        _wait_ready(explicit)
        yield explicit
        return

    if not _docker_available():
        pytest.skip("No TEST_POSTGRES_DSN and Docker is unavailable")

    # Only tear down what this fixture started: if the stack is already up
    # (developer left it running), reuse it and leave its volume alone.
    ps = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "ps", "-q"],
        capture_output=True,
        text=True,
        check=False,
    )
    preexisting = bool(ps.stdout.strip())

    up = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "--wait"],
        capture_output=True,
        text=True,
        check=False,
    )
    if up.returncode != 0:
        pytest.skip(f"docker compose up failed: {up.stderr.strip()[:500]}")
    try:
        _wait_ready(COMPOSE_DSN)
        yield COMPOSE_DSN
    finally:
        if not preexisting:
            subprocess.run(
                ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"],
                capture_output=True,
                text=True,
                check=False,
            )
