"""
test_analytics_queries.py — Runs every SQL file in
sql/analytics/queries/ against the real local Postgres instance and
asserts each one executes without error and returns at least one row
(none of these business questions should legitimately return an
empty result against real pipeline data).

This does not re-verify every number (the underlying tables already
have dedicated correctness tests in test_customer_360.py and
test_rfm_clv_retention.py) — it verifies the queries themselves are
valid SQL against the real schema and produce sane, non-empty output,
which is what actually catches things like a wrong column name that
only surfaces when the query runs for real.

Run with:  pytest tests/test_analytics_queries.py -v
"""
from __future__ import annotations

import os
from pathlib import Path

import psycopg2
import pytest

QUERIES_DIR = Path(__file__).resolve().parent.parent / "sql" / "analytics" / "queries"


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "customer360"),
        user=os.environ.get("POSTGRES_USER", "customer360_app"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


def _pg_available() -> bool:
    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="Postgres not reachable at POSTGRES_HOST/PORT")

QUERY_FILES = sorted(QUERIES_DIR.glob("*.sql"))


def test_exactly_twenty_query_files_exist():
    assert len(QUERY_FILES) == 20


@pytest.mark.parametrize("query_file", QUERY_FILES, ids=[f.stem for f in QUERY_FILES])
def test_query_runs_and_returns_rows(query_file):
    sql = query_file.read_text()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        assert len(rows) > 0, f"{query_file.name} returned zero rows against real pipeline data"
    finally:
        conn.close()


def test_no_query_result_has_unexpected_nulls_in_key_columns():
    """A targeted regression test for the exact bug found while
    building this phase: query 19's profitability ranking had a
    customer with NULL revenue (from a cleaned/nulled unit_price)
    sorting to #1 due to Postgres's NULLS-FIRST-on-DESC default."""
    query_file = QUERIES_DIR / "19_customer_profitability.sql"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query_file.read_text())
            cols = [d.name for d in cur.description]
            rows = cur.fetchall()
        revenue_idx = cols.index("revenue")
        for row in rows:
            assert row[revenue_idx] is not None
    finally:
        conn.close()


def test_powerbi_customer_360_view_joins_correctly():
    """The Power BI Page 2 convenience view (Phase 11) should expose
    both historical and estimated CLV for the same customer, joined
    correctly on customer_id."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM analytics.vw_powerbi_customer_360")
            (view_count,) = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM analytics.customer_360")
            (c360_count,) = cur.fetchone()
        assert view_count == c360_count  # LEFT JOIN must not drop or duplicate rows
    finally:
        conn.close()


def test_powerbi_reader_role_is_read_only_and_excludes_staging():
    """Phase 12's least-privilege design (aws/iam.md): powerbi_reader
    can SELECT from warehouse/analytics but cannot write anywhere, and
    cannot even see the staging schema (which can hold dirty,
    not-yet-quality-checked data with no business on a dashboard)."""
    import psycopg2

    admin_conn = get_connection()
    with admin_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'powerbi_reader'")
        role_exists = cur.fetchone() is not None
    admin_conn.close()

    if not role_exists:
        pytest.skip("powerbi_reader role not deployed in this environment (run sql/ddl_deploy.py)")

    reader_conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "customer360"),
        user="powerbi_reader",
        password=os.environ.get("POWERBI_READER_PASSWORD", "change_me_immediately"),
    )
    try:
        with reader_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM warehouse.dim_customer")  # should succeed

        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            with reader_conn.cursor() as cur:
                cur.execute("DELETE FROM warehouse.dim_customer")
        reader_conn.rollback()

        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            with reader_conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM staging.stg_customers")
        reader_conn.rollback()
    finally:
        reader_conn.close()
