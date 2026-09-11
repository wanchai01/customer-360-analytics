"""
test_database_schema.py — Integration tests for the PostgreSQL DDL
(staging / warehouse / analytics schemas).

These tests need a real PostgreSQL connection (they exercise actual
constraints, generated columns, and partitioning — things you cannot
verify by reading the SQL file alone). They read connection info from
the same POSTGRES_* environment variables as the rest of the platform
and are skipped automatically if no database is reachable, so they
don't break a plain `pytest tests/` run on a machine without Postgres.

Run for real with:
    docker compose up -d postgres
    docker compose exec etl python -m sql.ddl_deploy   # or psql -f ...
    docker compose exec etl pytest tests/test_database_schema.py -v
"""
from __future__ import annotations

import os

import pytest

psycopg2 = pytest.importorskip("psycopg2")


def _connect():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "customer360"),
        user=os.environ.get("POSTGRES_USER", "c360_app"),
        password=os.environ.get("POSTGRES_PASSWORD", "devpass123"),
        connect_timeout=3,
    )


@pytest.fixture(scope="module")
def conn():
    try:
        c = _connect()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"PostgreSQL not reachable, skipping schema tests: {exc}")
    yield c
    c.close()


@pytest.fixture
def cur(conn):
    conn.rollback()
    c = conn.cursor()
    yield c
    conn.rollback()
    c.close()


EXPECTED_TABLES = {
    "staging": {
        "stg_customers", "stg_products", "stg_orders", "stg_order_items",
        "stg_payments", "stg_website_events", "stg_customer_support",
        "stg_marketing_campaigns", "stg_campaign_interactions",
    },
    "warehouse": {
        "dim_date", "dim_location", "dim_device", "dim_customer",
        "dim_product", "dim_campaign", "fact_orders", "fact_order_items",
        "fact_payments", "fact_support_tickets", "fact_campaign_interactions",
        "fact_website_events",
    },
    "analytics": {
        "customer_360", "customer_rfm", "customer_segments",
        "customer_monthly_metrics", "customer_cohort", "data_quality_results",
    },
}


@pytest.mark.parametrize("schema,tables", EXPECTED_TABLES.items())
def test_expected_tables_exist(cur, schema, tables):
    cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
        (schema,),
    )
    found = {row[0] for row in cur.fetchall()}
    missing = tables - found
    assert not missing, f"Missing tables in schema {schema}: {missing}"


def test_dim_date_is_populated(cur):
    cur.execute("SELECT count(*) FROM warehouse.dim_date")
    (count,) = cur.fetchone()
    assert count > 3000  # 2019-01-01 .. 2027-12-31 is ~3287 days


def test_fact_orders_rejects_unknown_customer(cur, conn):
    with pytest.raises(Exception):
        cur.execute(
            "INSERT INTO warehouse.fact_orders "
            "(order_id, customer_key, order_date, order_status, payment_method, total_amount) "
            "VALUES ('TEST_ORPHAN', 999999, now(), 'completed', 'cod', 100)"
        )
        conn.commit()
    conn.rollback()


@pytest.fixture
def sample_customer_key(cur, conn):
    """Ensure at least one dim_customer row exists and return its key,
    so downstream tests don't depend on data left over from other runs."""
    cur.execute(
        "INSERT INTO warehouse.dim_location (city, province, country) "
        "VALUES ('Bangkok', 'Bangkok', 'Thailand') "
        "ON CONFLICT (city, province, country) DO UPDATE SET city = EXCLUDED.city "
        "RETURNING location_key"
    )
    (location_key,) = cur.fetchone()
    cur.execute(
        "INSERT INTO warehouse.dim_customer (customer_id, first_name, last_name, location_key, registration_date, customer_status) "
        "VALUES ('TEST_CUST_FIXTURE', 'Test', 'Fixture', %s, current_date, 'active') "
        "ON CONFLICT (customer_id) DO UPDATE SET first_name = EXCLUDED.first_name "
        "RETURNING customer_key",
        (location_key,),
    )
    (customer_key,) = cur.fetchone()
    conn.commit()
    return customer_key


def test_dim_product_margin_is_generated(cur, conn):
    cur.execute(
        "INSERT INTO warehouse.dim_product (product_id, product_name, category, subcategory, brand, cost, selling_price) "
        "VALUES ('TEST_PROD_1', 'Test Product', 'Electronics', 'Mobile Phones', 'TestBrand', 60, 100) "
        "RETURNING margin_pct"
    )
    (margin_pct,) = cur.fetchone()
    assert float(margin_pct) == pytest.approx(0.40, abs=0.001)


def test_fact_order_items_line_amount_is_generated(cur, conn, sample_customer_key):
    cur.execute(
        "INSERT INTO warehouse.fact_orders (order_id, customer_key, order_date, order_status, payment_method, total_amount) "
        "VALUES ('TEST_ORDER_LINE', %s, now(), 'completed', 'cod', 190) "
        "ON CONFLICT (order_id) DO UPDATE SET total_amount = EXCLUDED.total_amount "
        "RETURNING order_key",
        (sample_customer_key,),
    )
    (order_key,) = cur.fetchone()
    cur.execute("DELETE FROM warehouse.fact_order_items WHERE order_item_id = 'TEST_ITEM_1'")
    cur.execute(
        "INSERT INTO warehouse.fact_order_items (order_item_id, order_key, quantity, unit_price, discount) "
        "VALUES ('TEST_ITEM_1', %s, 2, 100, 0.05) RETURNING line_amount",
        (order_key,),
    )
    (line_amount,) = cur.fetchone()
    assert float(line_amount) == pytest.approx(190.00, abs=0.01)


def test_website_events_route_to_correct_year_partition(cur, conn, sample_customer_key):
    cur.execute("DELETE FROM warehouse.fact_website_events WHERE event_id = 'TEST_EVT_2025'")
    cur.execute(
        "INSERT INTO warehouse.fact_website_events "
        "(event_id, customer_key, session_id, event_timestamp, event_type, page, traffic_source) "
        "VALUES ('TEST_EVT_2025', %s, 'TEST_SESS', '2025-06-15 10:00:00', 'page_view', '/home', 'direct')",
        (sample_customer_key,),
    )
    cur.execute(
        "SELECT tableoid::regclass::text FROM warehouse.fact_website_events WHERE event_id = 'TEST_EVT_2025'"
    )
    (partition,) = cur.fetchone()
    assert partition == "warehouse.fact_website_events_y2025" or partition == "fact_website_events_y2025"


def test_data_quality_results_pass_rate_is_generated(cur, conn):
    cur.execute(
        "INSERT INTO analytics.data_quality_results "
        "(dataset, check_name, total_records, failed_records, status, execution_time, batch_date) "
        "VALUES ('customers', 'not_null_customer_id', 1000, 20, 'FAIL', 0.42, current_date) "
        "RETURNING pass_rate"
    )
    (pass_rate,) = cur.fetchone()
    assert float(pass_rate) == pytest.approx(0.98, abs=0.001)
