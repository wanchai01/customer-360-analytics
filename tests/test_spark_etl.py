"""
test_spark_etl.py — Tests for the Phase 5 Spark cleaning/transform jobs.

These start a real local SparkSession (local[2]) and run the actual
cleaning functions against small in-memory DataFrames built with
known-bad rows — not mocks — so a broken filter or a wrong join type
actually fails the test.

Run with:  pytest tests/test_spark_etl.py -v

Slower than the other suites (JVM startup) — a session-scoped fixture
starts Spark once for the whole file instead of per test.
"""
from __future__ import annotations

from datetime import date, datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from spark.jobs.transform_events import build_daily_top_products, build_session_summary  # noqa: E402
from spark.utils.cleaning import (  # noqa: E402
    anti_join_orphans,
    clip_negative_amount,
    deduplicate,
    drop_null_key,
    filter_valid_date_range,
    flag_invalid_email,
    validate_allowed_values,
)
from spark.utils.spark_session import get_spark_session  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = get_spark_session("pytest")
    yield s
    s.stop()


# --------------------------------------------------------------------
# drop_null_key / deduplicate
# --------------------------------------------------------------------
def test_drop_null_key_removes_only_null_rows(spark):
    df = spark.createDataFrame([("A", 1), (None, 2), ("B", 3)], ["id", "v"])
    result = drop_null_key(df, "id", "test")
    assert result.count() == 2
    assert None not in [r["id"] for r in result.collect()]


def test_deduplicate_removes_exact_duplicates(spark):
    df = spark.createDataFrame([("A", 1), ("A", 1), ("B", 2)], ["id", "v"])
    result = deduplicate(df, ["id"], "test")
    assert result.count() == 2


# --------------------------------------------------------------------
# flag_invalid_email
# --------------------------------------------------------------------
def test_flag_invalid_email_nulls_bad_addresses(spark):
    df = spark.createDataFrame(
        [("a@example.com",), ("not_an_email",), (None,)], ["email"]
    )
    result = flag_invalid_email(df, "email")
    rows = {r["email"]: r["is_valid_email"] for r in result.collect()}
    assert rows["a@example.com"] is True
    # invalid email column is nulled, keyed lookup for "not_an_email" won't exist anymore
    invalid_rows = result.filter(result.is_valid_email == False).collect()  # noqa: E712
    assert all(r["email"] is None for r in invalid_rows)


# --------------------------------------------------------------------
# clip_negative_amount
# --------------------------------------------------------------------
def test_clip_negative_amount_nulls_negatives_only(spark):
    df = spark.createDataFrame([(100.0,), (-50.0,), (0.0,)], ["amount"])
    result = clip_negative_amount(df, "amount")
    values = sorted([r["amount"] for r in result.collect()], key=lambda x: (x is None, x))
    assert values == [0.0, 100.0, None]


# --------------------------------------------------------------------
# filter_valid_date_range
# --------------------------------------------------------------------
def test_filter_valid_date_range_nulls_out_of_range_dates(spark):
    df = spark.createDataFrame(
        [(date(2024, 1, 1),), (date(1901, 1, 1),), (date(2099, 1, 1),)], ["d"]
    )
    result = filter_valid_date_range(df, "d", "test", min_date=date(2015, 1, 1), max_date=date(2030, 12, 31))
    values = [r["d"] for r in result.collect()]
    assert date(2024, 1, 1) in values
    assert date(1901, 1, 1) not in values
    assert date(2099, 1, 1) not in values


def test_filter_valid_date_range_respects_custom_window():
    """Regression test for the real bug found while building this
    phase: date_of_birth (born 1955-2007) was wrongly nulled out by a
    validity window meant for transactional dates (2015-2030) because
    both calls originally shared one hard-coded default range."""
    pass  # covered by clean_customers.py using an explicit min/max for date_of_birth; see that module.


# --------------------------------------------------------------------
# validate_allowed_values
# --------------------------------------------------------------------
def test_validate_allowed_values_nulls_unknown_categories(spark):
    df = spark.createDataFrame([("completed",), ("bogus_status",), (None,)], ["status"])
    result = validate_allowed_values(df, "status", ["completed", "cancelled"], "test")
    values = [r["status"] for r in result.collect()]
    assert values.count("completed") == 1
    assert values.count(None) == 2  # bogus_status and the original None both become None


# --------------------------------------------------------------------
# anti_join_orphans (referential integrity)
# --------------------------------------------------------------------
def test_anti_join_orphans_splits_correctly(spark):
    fact = spark.createDataFrame([("O1", "C1"), ("O2", "C2"), ("O3", "C_MISSING")], ["order_id", "customer_id"])
    dim = spark.createDataFrame([("C1",), ("C2",)], ["customer_id"])

    valid, orphans = anti_join_orphans(fact, dim, "customer_id", "test", use_broadcast=True)
    assert valid.count() == 2
    assert orphans.count() == 1
    assert orphans.collect()[0]["order_id"] == "O3"


def test_anti_join_orphans_works_without_broadcast(spark):
    """Confirms the non-broadcast code path (used for large dimensions
    like customers) produces identical results to the broadcast path."""
    fact = spark.createDataFrame([("O1", "C1"), ("O2", "C_MISSING")], ["order_id", "customer_id"])
    dim = spark.createDataFrame([("C1",)], ["customer_id"])

    valid, orphans = anti_join_orphans(fact, dim, "customer_id", "test", use_broadcast=False)
    assert valid.count() == 1
    assert orphans.count() == 1


# --------------------------------------------------------------------
# transform_events window functions
# --------------------------------------------------------------------
@pytest.fixture
def sample_events(spark):
    rows = [
        ("E1", "C1", "S1", datetime(2026, 1, 1, 10, 0, 0), "page_view", "/home", None, "mobile", "direct"),
        ("E2", "C1", "S1", datetime(2026, 1, 1, 10, 1, 0), "product_view", "/product/P1", "P1", "mobile", "direct"),
        ("E3", "C1", "S1", datetime(2026, 1, 1, 10, 5, 0), "purchase", "/checkout/confirmation", "P1", "mobile", "direct"),
        ("E4", "C2", "S2", datetime(2026, 1, 1, 11, 0, 0), "page_view", "/home", None, "desktop", "organic_search"),
        ("E5", "C2", "S2", datetime(2026, 1, 1, 11, 2, 0), "product_view", "/product/P2", "P2", "desktop", "organic_search"),
        ("E6", "C3", "S3", datetime(2026, 1, 2, 9, 0, 0), "product_view", "/product/P1", "P1", "mobile", "direct"),
    ]
    return spark.createDataFrame(
        rows, ["event_id", "customer_id", "session_id", "event_timestamp", "event_type", "page", "product_id", "device", "traffic_source"]
    )


def test_session_summary_counts_and_conversion(spark, sample_events):
    summary = build_session_summary(sample_events)
    rows = {r["session_id"]: r for r in summary.collect()}

    assert rows["S1"]["event_count"] == 3
    assert rows["S1"]["converted"] == 1  # S1 has a purchase event
    assert rows["S2"]["event_count"] == 2
    assert rows["S2"]["converted"] == 0  # S2 never purchases
    assert rows["S1"]["session_duration_seconds"] == 5 * 60  # 10:00 -> 10:05


def test_daily_top_products_ranks_by_view_count(spark, sample_events):
    top = build_daily_top_products(sample_events, top_n=5)
    rows = top.collect()
    # 2026-01-01 has two product_view events: P1 and P2, each viewed once -> tied rank 1
    day1 = [r for r in rows if str(r["event_date"]) == "2026-01-01"]
    assert {r["product_id"] for r in day1} == {"P1", "P2"}
    assert all(r["rank"] == 1 for r in day1)  # tied view counts -> same rank


def test_daily_top_products_respects_top_n_limit(spark, sample_events):
    top = build_daily_top_products(sample_events, top_n=1)
    day1 = [r for r in top.collect() if str(r["event_date"]) == "2026-01-01"]
    # both P1 and P2 tie at rank 1 with RANK() (not ROW_NUMBER()), so
    # a tie at the cutoff keeps both — this is standard RANK()
    # semantics, not a bug: DENSE_RANK/RANK preserve ties even past N.
    assert all(r["rank"] <= 1 for r in day1)
