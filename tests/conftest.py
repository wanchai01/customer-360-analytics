"""
conftest.py — Shared pytest configuration.

The one thing this guarantees: every test file starts the session
with a fully-populated, consistent pipeline state (warehouse dims/
facts + all Phase 8/9 analytics tables), regardless of what any
*previous* pytest invocation left behind.

Why this exists: several test files use function-scoped fixtures that
wipe warehouse/analytics tables clean at the end of each test (for
isolation — each test wants to assert against data it controls, not
leftovers). That's correct within those files, but it means the
tables can be genuinely empty by the time a full suite run ends — and
the *next* run would start from that empty state unless something
rebuilds it first. test_analytics_queries.py in particular has no
reason to know or care about any other file's fixtures; it just needs
real data to exist. Centralizing "make sure real data exists" here,
once, at session start, is more robust than coordinating restoration
logic across every individual destructive test.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _pg_available() -> bool:
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            dbname=os.environ.get("POSTGRES_DB", "customer360"),
            user=os.environ.get("POSTGRES_USER", "customer360_app"),
            password=os.environ.get("POSTGRES_PASSWORD", ""),
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session", autouse=True)
def ensure_full_pipeline_data():
    """Runs once per test session, before any test. Regenerates raw
    data and re-runs the whole pipeline through to Phase 9 only if
    Postgres is reachable — if it isn't, every DB-backed test's own
    skip logic handles that, and this fixture has nothing to do."""
    if not _pg_available():
        yield
        return

    os.chdir(PROJECT_ROOT)
    batch_date = "2026-09-06"

    from data_generator.generate_all import run as generate_all_run
    from spark.jobs.run_all import run_all as spark_run_all
    from spark.jobs.load_warehouse import load_warehouse
    from spark.jobs.build_customer_360 import build_customer_360
    from spark.jobs.build_rfm import build_rfm
    from spark.jobs.build_retention import build_retention
    from spark.jobs.build_clv import build_clv

    raw_dir = PROJECT_ROOT / "data" / "raw" / "customers" / f"batch_date={batch_date}"
    if not raw_dir.exists():
        generate_all_run("test", "data", batch_date, seed=42, dirty_rate=0.02)

    processed_dir = PROJECT_ROOT / "data" / "processed" / "customers"
    if not processed_dir.exists():
        spark_run_all(batch_date, base_dir="data")

    load_warehouse(base_dir="data")
    build_customer_360()
    build_rfm()
    build_retention()
    build_clv()

    yield
