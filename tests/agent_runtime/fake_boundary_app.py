"""Subprocess boot module for the real-agent-factory test (phase-1.md T12).

Runs ONLY in its own interpreter (never imported by pytest): applies the e2e
boundary patches (fake model, fake tokens, SANDBOX_TYPE=local) exactly the way
``langgraph dev`` does for the e2e suite, then serves ``agent_runtime`` with
the REAL ``langgraph.json`` registry. This is sanctioned alternative (1) from
the Phase 0 ledger — tests/e2e modules are process-poisonous, so the poison
stays in this process.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    port = int(os.environ["BOUNDARY_APP_PORT"])
    # BEFORE any agent import: agent.server binds its module-level client to
    # LANGGRAPH_URL at import time — and it must point back at this server.
    os.environ["LANGGRAPH_URL"] = f"http://127.0.0.1:{port}"
    os.environ["AGENT_RUNTIME_CONFIG"] = str(REPO_ROOT / "langgraph.json")
    # The runtime-test session sets this for its own server; this boot wants
    # the real webapp mounted (D1 acceptance).
    os.environ.pop("AGENT_RUNTIME_NO_WEBAPP", None)

    sys.path.insert(0, str(REPO_ROOT / "tests" / "e2e"))
    import patches  # type: ignore[reportMissingImports]  # noqa: PLC0415  (e2e module)

    patches.apply()

    import uvicorn

    from agent_runtime.app import create_app

    uvicorn.run(create_app(), host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    main()
