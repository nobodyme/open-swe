"""Smoke test for the ephemeral-Postgres substrate (Phase 0, task 5).

Pins that the docker-compose/TEST_POSTGRES_DSN fixture yields a usable
database — the substrate Phase 1's AsyncPostgresSaver/AsyncPostgresStore
``.setup()`` will target.
"""

from __future__ import annotations


def test_postgres_round_trip(postgres_dsn: str) -> None:
    import psycopg

    with psycopg.connect(postgres_dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS phase0_smoke (id int, note text)")
        cur.execute("INSERT INTO phase0_smoke VALUES (%s, %s)", (1, "hello"))
        cur.execute("SELECT note FROM phase0_smoke WHERE id = %s", (1,))
        row = cur.fetchone()
        assert row is not None and row[0] == "hello"
        cur.execute("DROP TABLE phase0_smoke")
