"""
test_monitoring.py — Tests for Phase 14's pipeline monitoring
(monitoring/pipeline_monitor.py) and its integration into the Airflow
DAG's on_success_callback/on_failure_callback.

Run with:  pytest tests/test_monitoring.py -v
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

from monitoring.pipeline_monitor import complete_run, get_connection, get_recent_runs, record_step, start_run


def _pg_available() -> bool:
    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="Postgres not reachable at POSTGRES_HOST/PORT")

TEST_BATCH = "2099-02-01"  # a throwaway batch_date this suite fully owns


@pytest.fixture(autouse=True)
def clean_test_runs():
    def _wipe():
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM analytics.pipeline_runs WHERE batch_date = %s", (TEST_BATCH,))
            cur.execute("DELETE FROM analytics.data_quality_results WHERE batch_date = %s", (TEST_BATCH,))
        conn.commit()
        conn.close()

    _wipe()
    yield
    _wipe()


def _insert_dq_result(dataset: str, check_name: str, total: int, failed: int, status: str):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO analytics.data_quality_results (dataset, check_name, total_records, failed_records, status, execution_time, batch_date) "
            "VALUES (%s, %s, %s, %s, %s, 0.1, %s)",
            (dataset, check_name, total, failed, status, TEST_BATCH),
        )
    conn.commit()
    conn.close()


# --------------------------------------------------------------------
def test_start_run_creates_a_running_row():
    run_id = start_run(TEST_BATCH, dag_run_id="test_dag_run_1")
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT status, dag_run_id FROM analytics.pipeline_runs WHERE run_id = %s", (run_id,))
        status, dag_run_id = cur.fetchone()
    conn.close()
    assert status == "RUNNING"
    assert dag_run_id == "test_dag_run_1"


def test_record_step_computes_duration_correctly():
    run_id = start_run(TEST_BATCH)
    t0 = datetime(2026, 1, 1, 10, 0, 0)
    t1 = datetime(2026, 1, 1, 10, 0, 45)
    record_step(run_id, "spark_transform", t0, t1, status="SUCCESS", records_processed=1000)

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT duration_seconds, records_processed, status FROM analytics.pipeline_run_steps WHERE run_id = %s", (run_id,))
        duration, records, status = cur.fetchone()
    conn.close()
    assert float(duration) == 45.0
    assert records == 1000
    assert status == "SUCCESS"


def test_record_step_handles_mixed_timezone_aware_and_naive_datetimes():
    """Regression test for the exact bug found while wiring this into
    the Airflow DAG: ti.start_date is timezone-aware (Airflow uses UTC
    throughout) but a fallback of naive datetime.now() for a missing
    ti.end_date caused 'can't subtract offset-naive and offset-aware
    datetimes'. record_step must not crash when given a tz-aware
    started_at and a tz-aware ended_at computed independently (the
    actual fix: both sides of the subtraction must consistently be
    tz-aware, not that record_step itself special-cases naive input)."""
    run_id = start_run(TEST_BATCH)
    t0 = datetime.now(timezone.utc)
    t1 = t0 + timedelta(seconds=5)
    record_step(run_id, "check_source_data", t0, t1, status="SUCCESS")

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT duration_seconds FROM analytics.pipeline_run_steps WHERE run_id = %s", (run_id,))
        (duration,) = cur.fetchone()
    conn.close()
    assert float(duration) == pytest.approx(5.0, abs=0.1)


def test_record_step_captures_error_message_on_failure():
    run_id = start_run(TEST_BATCH)
    t0 = datetime.now()
    record_step(run_id, "load_postgresql", t0, t0 + timedelta(seconds=1), status="FAILED", error_message="connection reset by peer")

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT status, error_message FROM analytics.pipeline_run_steps WHERE run_id = %s", (run_id,))
        status, error_message = cur.fetchone()
    conn.close()
    assert status == "FAILED"
    assert error_message == "connection reset by peer"


def test_complete_run_rolls_up_records_and_dq_status():
    run_id = start_run(TEST_BATCH)
    t0 = datetime.now()
    record_step(run_id, "step_a", t0, t0 + timedelta(seconds=1), status="SUCCESS", records_processed=100)
    record_step(run_id, "step_b", t0, t0 + timedelta(seconds=1), status="SUCCESS", records_processed=250)
    _insert_dq_result("customers", "email_valid", total=100, failed=3, status="PASS")

    summary = complete_run(run_id, status="SUCCESS")

    assert summary["total_records_processed"] == 350
    assert summary["total_records_failed"] == 3
    assert summary["dq_overall_status"] == "PASS"
    assert summary["failed_steps"] == 0
    assert summary["status"] == "SUCCESS"


def test_complete_run_reflects_dq_failure():
    run_id = start_run(TEST_BATCH)
    _insert_dq_result("orders", "total_amount_non_negative", total=100, failed=50, status="FAIL")

    summary = complete_run(run_id, status="SUCCESS")  # pipeline steps all succeeded...

    assert summary["dq_overall_status"] == "FAIL"  # ...but the DQ gate result is still visible in the summary


def test_complete_run_counts_failed_steps():
    run_id = start_run(TEST_BATCH)
    t0 = datetime.now()
    record_step(run_id, "step_a", t0, t0 + timedelta(seconds=1), status="SUCCESS")
    record_step(run_id, "step_b", t0, t0 + timedelta(seconds=1), status="FAILED", error_message="boom")

    summary = complete_run(run_id, status="FAILED")
    assert summary["failed_steps"] == 1


def test_complete_run_marks_ended_at_and_status():
    run_id = start_run(TEST_BATCH)
    complete_run(run_id, status="SUCCESS")

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT status, ended_at, duration_seconds FROM analytics.pipeline_runs WHERE run_id = %s", (run_id,))
        status, ended_at, duration = cur.fetchone()
    conn.close()
    assert status == "SUCCESS"
    assert ended_at is not None
    assert duration is not None and float(duration) >= 0


def test_get_recent_runs_orders_newest_first_and_respects_limit():
    ids = [start_run(TEST_BATCH) for _ in range(3)]
    for run_id in ids:
        complete_run(run_id, status="SUCCESS")

    recent = get_recent_runs(limit=2)
    assert len(recent) == 2
    assert recent[0]["run_id"] > recent[1]["run_id"]  # newest first


def test_record_step_cascades_on_run_delete():
    """FK ON DELETE CASCADE (schema design) should clean up step rows
    automatically if a run row is ever deleted — verified rather than
    assumed from reading the DDL."""
    run_id = start_run(TEST_BATCH)
    t0 = datetime.now()
    record_step(run_id, "step_a", t0, t0 + timedelta(seconds=1), status="SUCCESS")

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM analytics.pipeline_runs WHERE run_id = %s", (run_id,))
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM analytics.pipeline_run_steps WHERE run_id = %s", (run_id,))
        (count,) = cur.fetchone()
    conn.close()
    assert count == 0
