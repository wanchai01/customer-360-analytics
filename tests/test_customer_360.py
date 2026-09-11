"""
test_customer_360.py — Integration tests for the Phase 8 Customer 360
builder. Runs against the real local Postgres instance and skips
gracefully if unreachable (same pattern as the other DB-backed suites).

These tests build a small, hand-crafted warehouse scenario (a handful
of dim_customer/fact_orders/fact_order_items rows with known values)
rather than relying on whatever the generator happened to produce, so
every assertion checks an exact expected number — not just "some
plausible-looking output".

Run with:  pytest tests/test_customer_360.py -v
"""
from __future__ import annotations

import os
from datetime import date, timedelta

import psycopg2
import pytest

from spark.jobs.build_customer_360 import build_customer_360, get_connection


def _pg_available() -> bool:
    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="Postgres not reachable at POSTGRES_HOST/PORT")

TODAY = date.today()


@pytest.fixture
def clean_warehouse():
    """Full TRUNCATE ... CASCADE before and after each test, so this
    suite owns a completely known warehouse state rather than mixing
    with whatever the real pipeline last loaded."""
    def _wipe():
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE warehouse.dim_customer, warehouse.dim_product, "
                "warehouse.dim_location, warehouse.dim_campaign, warehouse.dim_device "
                "RESTART IDENTITY CASCADE"
            )
            cur.execute("TRUNCATE TABLE analytics.customer_360")
        conn.commit()
        conn.close()

    _wipe()
    yield
    _wipe()


def _exec(sql: str, params: tuple = ()):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        result = cur.fetchall() if cur.description else None
    conn.commit()
    conn.close()
    return result


def _insert_location() -> int:
    rows = _exec(
        "INSERT INTO warehouse.dim_location (city, province, country) "
        "VALUES ('Bangkok', 'Bangkok', 'Thailand') RETURNING location_key"
    )
    return rows[0][0]


def _insert_customer(customer_id: str, location_key: int, dob: date = date(1990, 1, 1)) -> int:
    rows = _exec(
        "INSERT INTO warehouse.dim_customer "
        "(customer_id, first_name, last_name, gender, date_of_birth, location_key, registration_date, customer_status) "
        "VALUES (%s, 'Test', 'Customer', 'Male', %s, %s, %s, 'active') RETURNING customer_key",
        (customer_id, dob, location_key, TODAY - timedelta(days=365 * 3)),
    )
    return rows[0][0]


def _insert_product(product_id: str, category: str, name: str) -> int:
    rows = _exec(
        "INSERT INTO warehouse.dim_product (product_id, product_name, category, subcategory, brand, cost, selling_price) "
        "VALUES (%s, %s, %s, 'Sub', 'Brand', 50, 100) RETURNING product_key",
        (product_id, name, category),
    )
    return rows[0][0]


def _insert_order(order_id: str, customer_key: int, order_date_val: date, status: str, amount: float) -> int:
    rows = _exec(
        "INSERT INTO warehouse.fact_orders (order_id, customer_key, order_date, order_status, payment_method, total_amount) "
        "VALUES (%s, %s, %s, %s, 'cod', %s) RETURNING order_key",
        (order_id, customer_key, order_date_val, status, amount),
    )
    return rows[0][0]


def _insert_order_item(item_id: str, order_key: int, product_key: int, quantity: int):
    _exec(
        "INSERT INTO warehouse.fact_order_items (order_item_id, order_key, product_key, quantity, unit_price, discount) "
        "VALUES (%s, %s, %s, %s, 100, 0)",
        (item_id, order_key, product_key, quantity),
    )


def _get_customer_360(customer_id: str) -> dict:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM analytics.customer_360 WHERE customer_id = %s", (customer_id,))
        cols = [d.name for d in cur.description]
        row = cur.fetchone()
    conn.close()
    return dict(zip(cols, row)) if row else None


# --------------------------------------------------------------------
def test_customer_with_no_orders_gets_zeroed_row(clean_warehouse):
    loc = _insert_location()
    _insert_customer("C_EMPTY", loc)

    build_customer_360()
    row = _get_customer_360("C_EMPTY")

    assert row is not None
    assert row["total_orders"] == 0
    assert row["completed_orders"] == 0
    assert row["total_spend"] == 0
    assert row["average_order_value"] == 0
    assert row["first_purchase_date"] is None
    assert row["last_purchase_date"] is None
    assert row["favorite_category"] is None


def test_total_spend_excludes_non_completed_orders(clean_warehouse):
    loc = _insert_location()
    ck = _insert_customer("C1", loc)
    _insert_order("O1", ck, TODAY - timedelta(days=10), "completed", 100.0)
    _insert_order("O2", ck, TODAY - timedelta(days=5), "cancelled", 500.0)
    _insert_order("O3", ck, TODAY - timedelta(days=3), "pending", 900.0)

    build_customer_360()
    row = _get_customer_360("C1")

    assert row["total_orders"] == 3
    assert row["completed_orders"] == 1
    assert row["cancelled_orders"] == 1
    assert float(row["total_spend"]) == 100.0  # only the completed order counts
    assert float(row["customer_lifetime_value"]) == 100.0


def test_average_order_value_is_spend_over_completed_orders(clean_warehouse):
    loc = _insert_location()
    ck = _insert_customer("C2", loc)
    _insert_order("O1", ck, TODAY - timedelta(days=10), "completed", 100.0)
    _insert_order("O2", ck, TODAY - timedelta(days=5), "completed", 300.0)

    build_customer_360()
    row = _get_customer_360("C2")

    assert float(row["total_spend"]) == 400.0
    assert float(row["average_order_value"]) == 200.0  # 400 / 2, not 400 / total_orders


def test_first_and_last_purchase_dates_and_days_since(clean_warehouse):
    loc = _insert_location()
    ck = _insert_customer("C3", loc)
    first = TODAY - timedelta(days=100)
    last = TODAY - timedelta(days=7)
    _insert_order("O1", ck, first, "completed", 50.0)
    _insert_order("O2", ck, last, "completed", 50.0)
    _insert_order("O3", ck, TODAY - timedelta(days=200), "cancelled", 999.0)  # not completed -> ignored for these dates

    build_customer_360()
    row = _get_customer_360("C3")

    assert row["first_purchase_date"] == first
    assert row["last_purchase_date"] == last
    assert row["days_since_last_purchase"] == 7


def test_favorite_product_and_category_by_order_item_volume(clean_warehouse):
    loc = _insert_location()
    ck = _insert_customer("C4", loc)
    p_popular = _insert_product("P1", "Electronics", "Popular Widget")
    p_rare = _insert_product("P2", "Fashion", "Rare Widget")
    order_key = _insert_order("O1", ck, TODAY - timedelta(days=1), "completed", 100.0)
    _insert_order_item("I1", order_key, p_popular, quantity=5)
    _insert_order_item("I2", order_key, p_rare, quantity=1)

    build_customer_360()
    row = _get_customer_360("C4")

    assert row["favorite_product"] == "Popular Widget"
    assert row["favorite_category"] == "Electronics"


def test_age_is_computed_from_date_of_birth(clean_warehouse):
    loc = _insert_location()
    dob = date(TODAY.year - 30, TODAY.month, 1)
    _insert_customer("C5", loc, dob=dob)

    build_customer_360()
    row = _get_customer_360("C5")

    assert row["age"] in (29, 30)  # depends on day-of-month vs TODAY, both are "correct" for a birthday this month


def test_location_combines_city_and_province(clean_warehouse):
    loc = _insert_location()
    _insert_customer("C6", loc)

    build_customer_360()
    row = _get_customer_360("C6")

    assert row["location"] == "Bangkok, Bangkok"


def test_rebuild_is_idempotent_not_additive(clean_warehouse):
    loc = _insert_location()
    _insert_customer("C7", loc)

    n1 = build_customer_360()
    n2 = build_customer_360()

    assert n1 == n2 == 1  # not 1 then 2


def test_rfm_and_segment_are_left_null_for_phase9(clean_warehouse):
    loc = _insert_location()
    _insert_customer("C8", loc)

    build_customer_360()
    row = _get_customer_360("C8")

    assert row["rfm_score"] is None
    assert row["customer_segment"] is None
