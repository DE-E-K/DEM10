"""
test_db_connection.py – Integration test: PostgreSQL connectivity and schema.

This test is intentionally skipped if PostgreSQL is not reachable (e.g. when
running in CI without Docker Compose).  It only runs when the DB is live.

Coverage
--------
* Basic connectivity (SELECT 1).
* All three expected schema tables are present.
* Expected indexes exist (by querying pg_indexes).
"""

import os

import psycopg
import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Helper: attempt to connect, skip if not reachable
# ─────────────────────────────────────────────────────────────────────────────

def _get_conn() -> psycopg.Connection:
    """
    Open a psycopg connection using env vars (or defaults).
    Raises ``psycopg.OperationalError`` if the DB is not reachable,
    which the tests convert into a ``pytest.skip``.
    """
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "55432"),
        dbname=os.getenv("POSTGRES_DB", "heartbeat"),
        user=os.getenv("POSTGRES_USER", "heartbeat_user"),
        password=os.getenv("POSTGRES_PASSWORD", "heartbeat_pass"),
        connect_timeout=3,
    )


@pytest.fixture(scope="module")
def db_conn():
    """Module-scoped fixture: one connection shared across all tests in this file."""
    try:
        conn = _get_conn()
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable — skipping integration tests: {exc}")
    yield conn
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_basic_connectivity(db_conn):
    """Smoke test: the connection is live and responds to a trivial query."""
    with db_conn.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1


def test_heartbeat_events_table_exists(db_conn):
    """``heartbeat_events`` table must exist (created by 01_schema.sql)."""
    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                AND   table_name   = 'heartbeat_events'
            );
            """
        )
        assert cur.fetchone()[0] is True, "Table 'heartbeat_events' not found"


def test_anomalies_table_exists(db_conn):
    """``anomalies`` table must exist."""
    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                AND   table_name   = 'anomalies'
            );
            """
        )
        assert cur.fetchone()[0] is True, "Table 'anomalies' not found"


def test_ingest_checkpoint_table_exists(db_conn):
    """``ingest_checkpoint`` table must exist."""
    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                AND   table_name   = 'ingest_checkpoint'
            );
            """
        )
        assert cur.fetchone()[0] is True, "Table 'ingest_checkpoint' not found"


def test_heartbeat_customer_time_index_exists(db_conn):
    """Composite customer+time index on heartbeat_events must exist."""
    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE tablename = 'heartbeat_events'
                AND   indexname = 'idx_heartbeat_customer_time'
            );
            """
        )
        assert cur.fetchone()[0] is True, "Index 'idx_heartbeat_customer_time' not found"


def test_anomaly_type_index_exists(db_conn):
    """Anomaly type+time index must exist (powers the Grafana bar-chart query)."""
    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE tablename = 'anomalies'
                AND   indexname = 'idx_anomalies_type_time'
            );
            """
        )
        assert cur.fetchone()[0] is True, "Index 'idx_anomalies_type_time' not found"
