"""
test_spark_jobs_integration.py — Runs each Spark cleaning job's actual
orchestration function (not just the shared primitives in
spark/utils/cleaning.py, already unit-tested in test_spark_etl.py)
against small real CSV/JSON files on disk.

Found via `pytest --cov`: the shared cleaning primitives had ~98%
coverage, but the individual job functions (clean_customers,
clean_products, etc.) that call them were only ever exercised
manually via `python -m spark.jobs.X` during development — invisible
to pytest's coverage tracking and to anyone reading the test suite as
the record of what's verified. These tests close that gap for real.

Run with:  pytest tests/test_spark_jobs_integration.py -v
"""
from __future__ import annotations

import pandas as pd
import pytest

pyspark = pytest.importorskip("pyspark")

from spark.jobs.clean_customer_support import clean_customer_support  # noqa: E402
from spark.jobs.clean_customers import clean_customers  # noqa: E402
from spark.jobs.clean_marketing import clean_marketing  # noqa: E402
from spark.jobs.clean_payments import clean_payments  # noqa: E402
from spark.jobs.clean_products import clean_products  # noqa: E402
from spark.utils.spark_session import get_spark_session  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = get_spark_session("pytest_integration")
    yield s
    s.stop()


def _write_csv(path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def test_clean_customers_end_to_end(spark, tmp_path):
    raw = tmp_path / "customers.csv"
    _write_csv(raw, [
        {"customer_id": "C1", "first_name": "A", "last_name": "X", "email": "a@example.com",
         "phone": "0800000000", "gender": "Male", "date_of_birth": "1990-01-01", "city": "Bangkok",
         "province": "Bangkok", "country": "Thailand", "registration_date": "2024-01-01", "customer_status": "active"},
        {"customer_id": None, "first_name": "B", "last_name": "Y", "email": "bad_email",
         "phone": "0800000001", "gender": "Female", "date_of_birth": "1985-05-05", "city": "Bangkok",
         "province": "Bangkok", "country": "Thailand", "registration_date": "2023-01-01", "customer_status": "bogus"},
    ])

    df, target_files = clean_customers(spark, str(raw))
    rows = df.collect()

    assert len(rows) == 1  # the NULL-customer_id row is dropped
    assert rows[0]["customer_id"] == "C1"
    assert "_processed_at" in df.columns


def test_clean_products_end_to_end(spark, tmp_path):
    raw = tmp_path / "products.csv"
    _write_csv(raw, [
        {"product_id": "P1", "product_name": "Good", "category": "Cat", "subcategory": "Sub",
         "brand": "Brand", "cost": 10.0, "selling_price": 20.0},
        {"product_id": "P2", "product_name": "BadMargin", "category": "Cat", "subcategory": "Sub",
         "brand": "Brand", "cost": 50.0, "selling_price": 20.0},  # cost > selling_price
    ])

    df = clean_products(spark, str(raw))
    rows = {r["product_id"]: r for r in df.collect()}

    assert rows["P1"]["cost"] == 10.0
    assert rows["P2"]["cost"] is None  # nulled due to cost > selling_price
    assert rows["P2"]["selling_price"] is None


def test_clean_payments_end_to_end(spark, tmp_path):
    orders_dir = tmp_path / "clean_orders"
    spark.createDataFrame([("O1", "completed")], ["order_id", "order_status"]).write.mode("overwrite").parquet(str(orders_dir))

    raw = tmp_path / "payments.csv"
    _write_csv(raw, [
        {"payment_id": "PAY1", "order_id": "O1", "payment_date": "2024-01-02T10:00:00", "payment_method": "cod",
         "payment_status": "success", "amount": 100.0},
        {"payment_id": "PAY2", "order_id": "O_MISSING", "payment_date": "2024-01-02T10:00:00", "payment_method": "cod",
         "payment_status": "success", "amount": 50.0},  # orphan order_id
    ])

    df = clean_payments(spark, str(raw), str(orders_dir))
    rows = df.collect()

    assert len(rows) == 1  # orphan payment dropped
    assert rows[0]["payment_id"] == "PAY1"


def test_clean_customer_support_end_to_end(spark, tmp_path):
    customers_dir = tmp_path / "clean_customers"
    spark.createDataFrame([("C1",)], ["customer_id"]).write.mode("overwrite").parquet(str(customers_dir))

    raw = tmp_path / "customer_support.csv"
    _write_csv(raw, [
        {"ticket_id": "T1", "customer_id": "C1", "created_at": "2024-01-01T00:00:00", "issue_type": "delivery_delay",
         "priority": "high", "status": "resolved", "resolution_time": 5.0},
        {"ticket_id": "T2", "customer_id": "C_MISSING", "created_at": "2024-01-01T00:00:00", "issue_type": "refund_request",
         "priority": "low", "status": "open", "resolution_time": None},
    ])

    df = clean_customer_support(spark, str(raw), str(customers_dir))
    rows = df.collect()

    assert len(rows) == 1  # orphan customer_id dropped
    assert rows[0]["ticket_id"] == "T1"


def test_clean_marketing_end_to_end(spark, tmp_path):
    customers_dir = tmp_path / "clean_customers"
    spark.createDataFrame([("C1",)], ["customer_id"]).write.mode("overwrite").parquet(str(customers_dir))

    campaigns_raw = tmp_path / "marketing_campaigns.csv"
    _write_csv(campaigns_raw, [
        {"campaign_id": "CMP1", "campaign_name": "Sale", "channel": "email",
         "start_date": "2024-01-01", "end_date": "2024-01-31", "budget": 1000.0},
    ])

    interactions_raw = tmp_path / "campaign_interactions.csv"
    _write_csv(interactions_raw, [
        {"interaction_id": "I1", "campaign_id": "CMP1", "customer_id": "C1",
         "interaction_type": "click", "interaction_timestamp": "2024-01-05T00:00:00"},
        {"interaction_id": "I2", "campaign_id": "CMP_MISSING", "customer_id": "C1",
         "interaction_type": "click", "interaction_timestamp": "2024-01-05T00:00:00"},
    ])

    campaigns_df, interactions_df = clean_marketing(spark, str(campaigns_raw), str(interactions_raw), str(customers_dir))

    assert campaigns_df.count() == 1
    interaction_rows = interactions_df.collect()
    assert len(interaction_rows) == 1  # orphan campaign_id dropped
    assert interaction_rows[0]["interaction_id"] == "I1"


def test_clean_orders_end_to_end(spark, tmp_path):
    """clean_orders.py is the most complex cleaning job (two raw
    inputs, two dimension lookups) — previously only covered
    indirectly (and inconsistently, depending on incidental local
    disk state) via conftest.py's conditional full-pipeline
    regeneration. A dedicated test makes this deterministic."""
    from spark.jobs.clean_orders import clean_orders

    customers_dir = tmp_path / "clean_customers"
    spark.createDataFrame([("C1",)], ["customer_id"]).write.mode("overwrite").parquet(str(customers_dir))

    products_dir = tmp_path / "clean_products"
    spark.createDataFrame([("P1",)], ["product_id"]).write.mode("overwrite").parquet(str(products_dir))

    orders_raw = tmp_path / "orders.csv"
    _write_csv(orders_raw, [
        {"order_id": "O1", "customer_id": "C1", "order_date": "2024-01-01T00:00:00",
         "order_status": "completed", "payment_method": "cod", "total_amount": 100.0},
        {"order_id": "O2", "customer_id": "C_MISSING", "order_date": "2024-01-01T00:00:00",
         "order_status": "completed", "payment_method": "cod", "total_amount": -50.0},
    ])

    items_raw = tmp_path / "order_items.csv"
    _write_csv(items_raw, [
        {"order_item_id": "I1", "order_id": "O1", "product_id": "P1", "quantity": 2, "unit_price": 50.0, "discount": 0.0},
        {"order_item_id": "I2", "order_id": "O1", "product_id": "P_MISSING", "quantity": 1, "unit_price": 10.0, "discount": 0.0},
        {"order_item_id": "I3", "order_id": "O_MISSING", "product_id": "P1", "quantity": 1, "unit_price": -5.0, "discount": 0.0},
    ])

    orders_df, items_df = clean_orders(spark, str(orders_raw), str(items_raw), str(customers_dir), str(products_dir))

    order_rows = orders_df.collect()
    assert len(order_rows) == 1  # O2's orphan customer_id drops it entirely
    assert order_rows[0]["order_id"] == "O1"

    item_rows = items_df.collect()
    assert len(item_rows) == 1  # I2 (orphan product) and I3 (orphan order) both drop
    assert item_rows[0]["order_item_id"] == "I1"


def test_clean_events_end_to_end(spark, tmp_path):
    """clean_events() itself was previously untested — only its
    downstream window-function builders (build_session_summary,
    build_daily_top_products) were, using an in-memory DataFrame that
    skipped the actual read-raw-JSON + orphan-drop logic entirely."""
    from spark.jobs.transform_events import clean_events

    customers_dir = tmp_path / "clean_customers"
    spark.createDataFrame([("C1",)], ["customer_id"]).write.mode("overwrite").parquet(str(customers_dir))

    raw = tmp_path / "website_events.json"
    raw.write_text(
        '{"event_id":"E1","customer_id":"C1","session_id":"S1","event_timestamp":"2024-01-01T10:00:00",'
        '"event_type":"page_view","page":"/home","product_id":null,"device":"mobile","traffic_source":"direct"}\n'
        '{"event_id":"E2","customer_id":"C_MISSING","session_id":"S2","event_timestamp":"2024-01-01T10:00:00",'
        '"event_type":"page_view","page":"/home","product_id":null,"device":"mobile","traffic_source":"direct"}\n'
        '{"event_id":"E1","customer_id":"C1","session_id":"S1","event_timestamp":"2024-01-01T10:00:00",'
        '"event_type":"page_view","page":"/home","product_id":null,"device":"mobile","traffic_source":"direct"}\n'
    )

    df = clean_events(spark, str(raw), str(customers_dir))
    rows = df.collect()

    assert len(rows) == 1  # E2 (orphan customer) dropped, duplicate E1 collapsed to one
    assert rows[0]["event_id"] == "E1"
    assert "_processed_at" in df.columns


def test_run_all_orchestrates_every_job_correctly(tmp_path):
    """spark.jobs.run_all.run_all() is the actual function Airflow's
    spark_transform task calls (Phase 7) — previously only exercised
    indirectly and inconsistently via conftest.py's conditional
    pipeline regeneration (which skips re-running Spark entirely if
    data/processed already exists on disk from an earlier session).
    This test is self-contained: it generates its own tiny raw dataset
    in a temp directory so it doesn't depend on any prior state."""
    from data_generator.generate_all import run as generate_all_run
    from spark.jobs.run_all import run_all

    batch_date = "2099-06-01"
    generate_all_run("test", str(tmp_path), batch_date, seed=1, dirty_rate=0.02)

    row_counts = run_all(batch_date, base_dir=str(tmp_path))

    expected_datasets = {
        "customers", "products", "orders", "order_items", "payments",
        "customer_support", "marketing_campaigns", "campaign_interactions",
        "website_events", "session_summary", "daily_top_products",
    }
    assert set(row_counts.keys()) == expected_datasets
    assert all(count >= 0 for count in row_counts.values())
    assert row_counts["customers"] > 0  # the happy-path datasets should have real rows, not just valid zero-counts

    # Every processed dataset should have actually been written to disk.
    # Note: the on-disk folder for website_events is named "events"
    # (matching data_lake/lake_paths.py's PROCESSED_DATASETS naming
    # from Phase 4), while the row_counts dict key above is
    # "website_events" (matching the raw dataset name) — deliberately
    # different names for the same data, not a bug to "fix" into matching.
    disk_folder_for = {"website_events": "events"}
    for dataset in expected_datasets:
        folder = disk_folder_for.get(dataset, dataset)
        assert (tmp_path / "processed" / folder / f"batch_date={batch_date}").exists()
