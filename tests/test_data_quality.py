"""
test_data_quality.py — Integration tests for the Phase 6 Data Quality
framework. Runs against the real local Postgres instance (same
pattern as test_database_schema.py) and skips gracefully if it isn't
reachable, so this suite doesn't block environments without a live DB.

Run with:  pytest tests/test_data_quality.py -v
"""
from __future__ import annotations

import os

import psycopg2
import pytest

from data_quality.checks import CHECKS, get_connection, run_all_checks, run_check


def _pg_available() -> bool:
    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="Postgres not reachable at POSTGRES_HOST/PORT")

TEST_BATCH = "2099-01-01"  # a throwaway batch_date this suite fully owns, never touched by real pipeline runs


@pytest.fixture(autouse=True)
def clean_test_batch():
    """Ensure the test batch_date's staging + results rows are empty before and after each test."""
    def _wipe():
        conn = get_connection()
        with conn.cursor() as cur:
            for table in [
                "staging.stg_customers", "staging.stg_orders", "staging.stg_order_items",
                "staging.stg_products", "staging.stg_payments",
            ]:
                cur.execute(f"DELETE FROM {table} WHERE _batch_date = %s", (TEST_BATCH,))
            cur.execute("DELETE FROM analytics.data_quality_results WHERE batch_date = %s", (TEST_BATCH,))
        conn.commit()
        conn.close()

    _wipe()
    yield
    _wipe()


def _insert_customers(rows: list[tuple]):
    """rows: list of (customer_id, email)"""
    conn = get_connection()
    with conn.cursor() as cur:
        for customer_id, email in rows:
            cur.execute(
                "INSERT INTO staging.stg_customers (customer_id, email, customer_status, _batch_date) "
                "VALUES (%s, %s, 'active', %s)",
                (customer_id, email, TEST_BATCH),
            )
    conn.commit()
    conn.close()


def _insert_orders(rows: list[tuple]):
    """rows: list of (order_id, customer_id, total_amount, order_status)"""
    conn = get_connection()
    with conn.cursor() as cur:
        for order_id, customer_id, total_amount, order_status in rows:
            cur.execute(
                "INSERT INTO staging.stg_orders (order_id, customer_id, total_amount, order_status, _batch_date) "
                "VALUES (%s, %s, %s, %s, %s)",
                (order_id, customer_id, total_amount, order_status, TEST_BATCH),
            )
    conn.commit()
    conn.close()


# --------------------------------------------------------------------
# Check registry sanity
# --------------------------------------------------------------------
def test_all_six_dimensions_are_represented():
    dimensions = {c.dimension for c in CHECKS}
    assert dimensions == {"completeness", "uniqueness", "validity", "accuracy", "referential_integrity", "consistency"}


def test_check_names_are_unique_per_dataset():
    seen = set()
    for c in CHECKS:
        key = (c.dataset, c.check_name)
        assert key not in seen, f"duplicate check: {key}"
        seen.add(key)


# --------------------------------------------------------------------
# Individual check correctness (known-bad data, exact numbers)
# --------------------------------------------------------------------
def test_completeness_check_counts_nulls_correctly():
    _insert_customers([("C1", "a@example.com"), (None, "b@example.com"), (None, "c@example.com")])
    conn = get_connection()
    check = next(c for c in CHECKS if c.dataset == "customers" and c.check_name == "customer_id_not_null")
    result = run_check(conn, check, TEST_BATCH)
    conn.close()
    assert result.total_records == 3
    assert result.failed_records == 2
    assert result.pass_rate == pytest.approx(1 / 3, abs=0.01)


def test_uniqueness_check_counts_duplicates_correctly():
    _insert_customers([("C1", "a@example.com"), ("C1", "a2@example.com"), ("C2", "b@example.com")])
    conn = get_connection()
    check = next(c for c in CHECKS if c.dataset == "customers" and c.check_name == "customer_id_unique")
    result = run_check(conn, check, TEST_BATCH)
    conn.close()
    assert result.total_records == 3
    assert result.failed_records == 1  # one "extra" row beyond the 2 distinct ids


def test_validity_check_flags_bad_email_format():
    _insert_customers([("C1", "valid@example.com"), ("C2", "not_an_email"), ("C3", None)])
    conn = get_connection()
    check = next(c for c in CHECKS if c.dataset == "customers" and c.check_name == "email_format_valid")
    result = run_check(conn, check, TEST_BATCH)
    conn.close()
    assert result.total_records == 3
    assert result.failed_records == 2  # "not_an_email" and NULL both fail


def test_validity_check_flags_negative_amount():
    _insert_customers([("C1", "a@example.com")])
    _insert_orders([("O1", "C1", 100.0, "completed"), ("O2", "C1", -50.0, "completed")])
    conn = get_connection()
    check = next(c for c in CHECKS if c.dataset == "orders" and c.check_name == "total_amount_non_negative")
    result = run_check(conn, check, TEST_BATCH)
    conn.close()
    assert result.total_records == 2
    assert result.failed_records == 1


def test_referential_integrity_check_flags_orphan_customer():
    _insert_customers([("C1", "a@example.com")])
    _insert_orders([("O1", "C1", 100.0, "completed"), ("O2", "C_MISSING", 50.0, "completed")])
    conn = get_connection()
    check = next(c for c in CHECKS if c.dataset == "orders" and c.check_name == "customer_id_exists_in_customers")
    result = run_check(conn, check, TEST_BATCH)
    conn.close()
    assert result.total_records == 2
    assert result.failed_records == 1


def test_perfect_data_scores_100_percent():
    _insert_customers([("C1", "a@example.com"), ("C2", "b@example.com")])
    conn = get_connection()
    check = next(c for c in CHECKS if c.dataset == "customers" and c.check_name == "customer_id_not_null")
    result = run_check(conn, check, TEST_BATCH)
    conn.close()
    assert result.pass_rate == 1.0
    assert result.status == "PASS"


# --------------------------------------------------------------------
# The pipeline-failing gate (Section 22 requirement)
# --------------------------------------------------------------------
def test_gate_fails_when_critical_check_below_threshold(tmp_path):
    """3 customers, 2 with NULL customer_id -> 33% pass rate on a
    critical completeness check -> the overall gate must report FAIL
    and the caller (run_checks.py) must exit non-zero."""
    _insert_customers([("C1", "a@example.com"), (None, "b@example.com"), (None, "c@example.com")])
    results, overall_passed = run_all_checks(TEST_BATCH, min_pass_rate=0.95, report_dir=str(tmp_path))
    assert overall_passed is False
    completeness_result = next(r for r in results if r.dataset == "customers" and r.check_name == "customer_id_not_null")
    assert completeness_result.status == "FAIL"


def test_gate_passes_when_all_critical_checks_meet_threshold(tmp_path):
    _insert_customers([("C1", "a@example.com"), ("C2", "b@example.com")])
    _insert_orders([("O1", "C1", 100.0, "completed")])
    results, overall_passed = run_all_checks(TEST_BATCH, min_pass_rate=0.95, report_dir=str(tmp_path))
    assert overall_passed is True


def test_non_critical_failure_does_not_fail_the_gate(tmp_path):
    """A non-critical check (accuracy/consistency) failing badly should
    still surface in the report but must not flip overall_passed."""
    _insert_customers([("C1", "a@example.com")])
    # order total_amount wildly disagrees with its (nonexistent) order_items -> the
    # non-critical 'total_amount_matches_order_items' check will show a large mismatch,
    # but every *critical* check here is clean, so the gate should still pass.
    _insert_orders([("O1", "C1", 999999.0, "completed")])
    results, overall_passed = run_all_checks(TEST_BATCH, min_pass_rate=0.95, report_dir=str(tmp_path))
    assert overall_passed is True


def test_report_file_is_written_with_expected_shape(tmp_path):
    _insert_customers([("C1", "a@example.com")])
    run_all_checks(TEST_BATCH, min_pass_rate=0.95, report_dir=str(tmp_path))
    report_file = tmp_path / f"dq_report_{TEST_BATCH}.json"
    assert report_file.exists()

    import json
    payload = json.loads(report_file.read_text())
    assert payload["batch_date"] == TEST_BATCH
    assert payload["overall_status"] in ("PASS", "FAIL")
    assert len(payload["results"]) == len(CHECKS)


def test_results_are_persisted_to_postgres(tmp_path):
    _insert_customers([("C1", "a@example.com")])
    run_all_checks(TEST_BATCH, min_pass_rate=0.95, report_dir=str(tmp_path))

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM analytics.data_quality_results WHERE batch_date = %s", (TEST_BATCH,))
        count = cur.fetchone()[0]
    conn.close()
    assert count == len(CHECKS)


def test_rerun_is_idempotent_not_additive(tmp_path):
    """Running the same batch_date twice should replace, not double, the stored results."""
    _insert_customers([("C1", "a@example.com")])
    run_all_checks(TEST_BATCH, min_pass_rate=0.95, report_dir=str(tmp_path))
    run_all_checks(TEST_BATCH, min_pass_rate=0.95, report_dir=str(tmp_path))

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM analytics.data_quality_results WHERE batch_date = %s", (TEST_BATCH,))
        count = cur.fetchone()[0]
    conn.close()
    assert count == len(CHECKS)  # not 2x


# --------------------------------------------------------------------
# load_staging.py — real coverage gap found via `pytest --cov`: this
# module was only ever exercised manually (bash `python -m
# data_quality.load_staging`), never through an actual pytest test.
# --------------------------------------------------------------------
def test_load_dataset_preserves_dirty_data_from_raw(tmp_path):
    """Loading raw customers into staging must preserve nulls/dupes
    exactly as generated — staging exists specifically to hold dirty
    data for these checks to measure (see load_staging.py's docstring)."""
    import pandas as pd
    from data_quality.load_staging import load_dataset

    batch_date = "2099-02-01"
    raw_dir = tmp_path / "raw" / "customers" / f"batch_date={batch_date}"
    raw_dir.mkdir(parents=True)
    pd.DataFrame({
        "customer_id": ["C1", None, "C1"],  # one null key, one exact duplicate
        "first_name": ["A", "B", "A"],
        "last_name": ["X", "Y", "X"],
        "email": ["a@example.com", "bad_email", "a@example.com"],
        "phone": ["0800000000"] * 3,
        "gender": ["Male"] * 3,
        "date_of_birth": ["1990-01-01"] * 3,
        "city": ["Bangkok"] * 3,
        "province": ["Bangkok"] * 3,
        "country": ["Thailand"] * 3,
        "registration_date": ["2024-01-01"] * 3,
        "customer_status": ["active"] * 3,
    }).to_csv(raw_dir / "customers.csv", index=False)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.stg_customers WHERE _batch_date = %s", (batch_date,))
        conn.commit()

        row_count = load_dataset(conn, "customers", str(tmp_path), batch_date)
        assert row_count == 3

        with conn.cursor() as cur:
            cur.execute("SELECT customer_id, email FROM staging.stg_customers WHERE _batch_date = %s ORDER BY email NULLS LAST", (batch_date,))
            rows = cur.fetchall()
        assert len(rows) == 3
        assert sum(1 for r in rows if r[0] is None) == 1  # the null key survived the load
        assert sum(1 for r in rows if r[0] == "C1") == 2  # the duplicate survived the load

        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.stg_customers WHERE _batch_date = %s", (batch_date,))
        conn.commit()
    finally:
        conn.close()


def test_load_dataset_populates_source_file_for_provenance(tmp_path):
    """Regression test for a real gap found during Phase 15
    documentation work: sql/staging/01_staging_tables.sql already had
    a `_source_file` column, but load_staging.py never populated it —
    every row's provenance was silently NULL. Now it should record
    exactly which raw file a row came from."""
    import pandas as pd
    from data_quality.load_staging import load_dataset

    batch_date = "2099-03-01"
    raw_dir = tmp_path / "raw" / "products" / f"batch_date={batch_date}"
    raw_dir.mkdir(parents=True)
    pd.DataFrame({
        "product_id": ["P1"], "product_name": ["Widget"], "category": ["Cat"],
        "subcategory": ["Sub"], "brand": ["Brand"], "cost": [10.0], "selling_price": [20.0],
    }).to_csv(raw_dir / "products.csv", index=False)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.stg_products WHERE _batch_date = %s", (batch_date,))
        conn.commit()

        load_dataset(conn, "products", str(tmp_path), batch_date)

        with conn.cursor() as cur:
            cur.execute("SELECT _source_file FROM staging.stg_products WHERE _batch_date = %s", (batch_date,))
            (source_file,) = cur.fetchone()
        assert source_file == f"{tmp_path}/raw/products/batch_date={batch_date}/products.csv"

        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.stg_products WHERE _batch_date = %s", (batch_date,))
        conn.commit()
    finally:
        conn.close()


def test_load_all_is_idempotent_per_batch_date(tmp_path):
    """Re-loading the same batch_date must replace rows, not duplicate them."""
    import pandas as pd
    from data_quality.load_staging import STAGING_TABLE, load_dataset

    batch_date = "2099-03-01"
    raw_dir = tmp_path / "raw" / "products" / f"batch_date={batch_date}"
    raw_dir.mkdir(parents=True)
    pd.DataFrame({
        "product_id": ["P1", "P2"], "product_name": ["A", "B"], "category": ["Cat"] * 2,
        "subcategory": ["Sub"] * 2, "brand": ["Brand"] * 2, "cost": [10, 20], "selling_price": [20, 40],
    }).to_csv(raw_dir / "products.csv", index=False)

    conn = get_connection()
    try:
        table, _ = STAGING_TABLE["products"]
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table} WHERE _batch_date = %s", (batch_date,))
        conn.commit()

        load_dataset(conn, "products", str(tmp_path), batch_date)
        load_dataset(conn, "products", str(tmp_path), batch_date)  # re-run

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE _batch_date = %s", (batch_date,))
            (count,) = cur.fetchone()
        assert count == 2  # not 4

        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table} WHERE _batch_date = %s", (batch_date,))
        conn.commit()
    finally:
        conn.close()


def test_load_all_orchestrator_loads_every_dataset(tmp_path):
    """load_all() itself (the actual function called from the
    'ingest'/staging step) was previously untested — only its
    per-dataset helper load_dataset() was, leaving the loop-over-every-
    RAW_DATASETS wiring unverified."""
    from data_generator.generate_all import run as generate_all_run
    from data_quality.load_staging import load_all
    from data_lake.lake_paths import RAW_DATASETS

    batch_date = "2099-04-01"
    generate_all_run("test", str(tmp_path), batch_date, seed=2, dirty_rate=0.0)

    row_counts = load_all(str(tmp_path), batch_date)

    assert set(row_counts.keys()) == set(RAW_DATASETS)
    assert all(count > 0 for count in row_counts.values())

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM staging.stg_customers WHERE _batch_date = %s", (batch_date,))
            (count,) = cur.fetchone()
        assert count == row_counts["customers"]

        # Clean up this test's own staging rows across every table it loaded.
        for dataset in RAW_DATASETS:
            from data_quality.load_staging import STAGING_TABLE
            table, _ = STAGING_TABLE[dataset]
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table} WHERE _batch_date = %s", (batch_date,))
        conn.commit()
    finally:
        conn.close()


# --------------------------------------------------------------------
# run_checks.py CLI — exit codes matter for Airflow/CI integration
# --------------------------------------------------------------------
def test_cli_exits_zero_on_pass(tmp_path, monkeypatch, capsys):
    from data_quality.run_checks import main

    _insert_customers([("C1", "a@example.com")])
    monkeypatch.setattr(
        "sys.argv",
        ["run_checks.py", "--batch-date", TEST_BATCH, "--min-pass-rate", "0.5", "--report-dir", str(tmp_path)],
    )
    exit_code = main()
    assert exit_code == 0
    assert "Overall: PASS" in capsys.readouterr().out


def test_cli_exits_nonzero_on_fail(tmp_path, monkeypatch, capsys):
    from data_quality.run_checks import main

    _insert_customers([("C1", "a@example.com"), (None, "b@example.com")])
    monkeypatch.setattr(
        "sys.argv",
        ["run_checks.py", "--batch-date", TEST_BATCH, "--min-pass-rate", "0.99", "--report-dir", str(tmp_path)],
    )
    exit_code = main()
    assert exit_code == 1
    assert "Overall: FAIL" in capsys.readouterr().out
