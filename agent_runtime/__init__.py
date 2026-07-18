"""Self-hosted MIT runtime for Open SWE (docs/fast-api-migration/phase-1.md).

Serves the exact ``langgraph-api`` surface the app consumes — threads, runs,
store, crons, and the v2 dashboard wire — over FastAPI + Postgres, backed by
MIT ``langgraph`` and ``langgraph-checkpoint-postgres``. Parity is judged by
the Phase 0 contract suite (``tests/contract/``), not by opinion.
"""
