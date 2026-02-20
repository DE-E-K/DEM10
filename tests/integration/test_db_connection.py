import os
import pytest
import psycopg


def test_postgres_connection_smoke() -> None:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "heartbeat")
    user = os.getenv("POSTGRES_USER", "heartbeat_user")
    password = os.getenv("POSTGRES_PASSWORD", "heartbeat_pass")

    try:
        conn = psycopg.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password,
            connect_timeout=2,
        )
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable for integration test: {exc}")

    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1
