"""
test_rfm_clv_retention.py — Integration tests for Phase 9: RFM
scoring/segments, retention/cohort metrics, and Estimated CLV.

Uses hand-crafted warehouse + customer_360 fixtures with exactly-known
expected outcomes, against the real local Postgres instance (skipped
gracefully if unreachable, same pattern as the other DB-backed suites).

Run with:  pytest tests/test_rfm_clv_retention.py -v
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from spark.jobs.build_clv import build_clv, get_connection
from spark.jobs.build_customer_360 import build_customer_360
from spark.jobs.build_retention import build_retention
from spark.jobs.build_rfm import build_rfm


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
def clean_all():
    def _wipe():
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE warehouse.dim_customer, warehouse.dim_product, "
                "warehouse.dim_location, warehouse.dim_campaign, warehouse.dim_device "
                "RESTART IDENTITY CASCADE"
            )
            for t in ["analytics.customer_360", "analytics.customer_rfm", "analytics.customer_segments",
                      "analytics.customer_monthly_metrics", "analytics.customer_cohort", "analytics.customer_clv"]:
                cur.execute(f"TRUNCATE TABLE {t}")
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
    return _exec(
        "INSERT INTO warehouse.dim_location (city, province, country) "
        "VALUES ('Bangkok', 'Bangkok', 'Thailand') RETURNING location_key"
    )[0][0]


def _insert_customer(customer_id: str, location_key: int, registration_date=None) -> int:
    reg = registration_date or (TODAY - timedelta(days=365 * 3))
    return _exec(
        "INSERT INTO warehouse.dim_customer "
        "(customer_id, first_name, last_name, gender, date_of_birth, location_key, registration_date, customer_status) "
        "VALUES (%s, 'Test', 'Customer', 'Male', '1990-01-01', %s, %s, 'active') RETURNING customer_key",
        (customer_id, location_key, reg),
    )[0][0]


def _insert_order(order_id: str, customer_key: int, order_date_val: date, status: str = "completed", amount: float = 100.0) -> int:
    return _exec(
        "INSERT INTO warehouse.fact_orders (order_id, customer_key, order_date, order_status, payment_method, total_amount) "
        "VALUES (%s, %s, %s, %s, 'cod', %s) RETURNING order_key",
        (order_id, customer_key, order_date_val, status, amount),
    )[0][0]


def _get_rfm(customer_id: str) -> dict:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM analytics.customer_rfm WHERE customer_id = %s", (customer_id,))
        cols = [d.name for d in cur.description]
        row = cur.fetchone()
    conn.close()
    return dict(zip(cols, row)) if row else None


def _get_segment(customer_id: str) -> str | None:
    rows = _exec("SELECT customer_segment FROM analytics.customer_segments WHERE customer_id = %s", (customer_id,))
    return rows[0][0] if rows else None


def _build_chain():
    """Run the full Phase 8+9 chain in dependency order."""
    build_customer_360()
    build_rfm()
    build_retention()
    build_clv()


# --------------------------------------------------------------------
# RFM
# --------------------------------------------------------------------
def test_never_purchased_customer_gets_worst_rfm_and_lost_segment(clean_all):
    loc = _insert_location()
    _insert_customer("C_NEVER", loc, registration_date=TODAY - timedelta(days=400))

    _build_chain()
    rfm = _get_rfm("C_NEVER")

    assert rfm["frequency"] == 0
    assert rfm["r_score"] == 1 and rfm["f_score"] == 1 and rfm["m_score"] == 1
    assert _get_segment("C_NEVER") == "Lost Customers"


def test_recently_registered_never_purchased_is_new_not_lost(clean_all):
    loc = _insert_location()
    _insert_customer("C_BRAND_NEW", loc, registration_date=TODAY - timedelta(days=5))

    _build_chain()
    assert _get_segment("C_BRAND_NEW") == "New Customers"


def test_best_customer_scores_highest_and_is_champion(clean_all):
    """One standout customer among a real spread of buyers (not just
    padding with zero-order customers) should land in the top R/F/M
    quintile and be a Champion. Needs enough buyers (>= ~25) for
    NTILE(5) to actually spread across all 5 buckets — see the
    "Limitation" note in build_rfm.py's module docstring; with too
    few buyers Postgres front-loads low bucket numbers regardless of
    value, which is correct NTILE behavior, not a bug to work around
    here by lowering the threshold in production code."""
    loc = _insert_location()
    star = _insert_customer("C_STAR", loc)
    for i in range(10):
        _insert_order(f"O_STAR_{i}", star, TODAY - timedelta(days=i + 1), amount=10000.0)

    # 29 modest buyers so NTILE(5) has 30 total buyers to spread across quintiles
    for i in range(29):
        ck = _insert_customer(f"C_OTHER_{i}", loc)
        _insert_order(f"O_OTHER_{i}", ck, TODAY - timedelta(days=200 + i), amount=50.0)

    _build_chain()
    rfm = _get_rfm("C_STAR")

    assert rfm["r_score"] == 5
    assert rfm["f_score"] == 5
    assert rfm["m_score"] == 5
    assert _get_segment("C_STAR") == "Champions"


def test_rfm_score_string_matches_component_scores(clean_all):
    loc = _insert_location()
    star = _insert_customer("C_CONCAT", loc)
    _insert_order("O1", star, TODAY - timedelta(days=1), amount=500.0)

    _build_chain()
    rfm = _get_rfm("C_CONCAT")

    expected = f"{rfm['r_score']}{rfm['f_score']}{rfm['m_score']}"
    assert rfm["rfm_score"] == expected
    assert rfm["rfm_sum"] == rfm["r_score"] + rfm["f_score"] + rfm["m_score"]


def test_customer_360_is_backfilled_with_rfm_and_segment(clean_all):
    loc = _insert_location()
    _insert_customer("C_BACKFILL", loc)

    _build_chain()

    row = _exec("SELECT rfm_score, customer_segment FROM analytics.customer_360 WHERE customer_id = %s", ("C_BACKFILL",))[0]
    assert row[0] is not None
    assert row[1] is not None


# --------------------------------------------------------------------
# Retention / Cohort
# --------------------------------------------------------------------
def test_customer_active_in_consecutive_months_counts_as_retained(clean_all):
    loc = _insert_location()
    ck = _insert_customer("C_LOYAL", loc)
    month1 = date(2024, 1, 15)
    month2 = date(2024, 2, 10)
    _insert_order("O1", ck, month1)
    _insert_order("O2", ck, month2)

    build_customer_360()
    build_retention()

    rows = _exec(
        "SELECT active_customers, new_customers, retention_rate FROM analytics.customer_monthly_metrics "
        "WHERE metric_month = %s", (date(2024, 2, 1),)
    )
    active, new, retention = rows[0]
    assert active == 1
    assert new == 0  # first purchase was in January, not February
    assert float(retention) == 1.0  # the one customer active in Jan is also active in Feb


def test_customer_who_does_not_return_shows_full_churn(clean_all):
    loc = _insert_location()
    ck = _insert_customer("C_ONE_TIME", loc)
    _insert_order("O1", ck, date(2024, 3, 5))
    # no order in April

    build_customer_360()
    build_retention()

    rows = _exec(
        "SELECT retention_rate, churn_rate FROM analytics.customer_monthly_metrics WHERE metric_month = %s",
        (date(2024, 4, 1),),
    )
    retention, churn = rows[0]
    assert float(retention) == 0.0
    assert float(churn) == 1.0


def test_cohort_size_matches_period_zero_active_count(clean_all):
    loc = _insert_location()
    ck1 = _insert_customer("C_COHORT_1", loc)
    ck2 = _insert_customer("C_COHORT_2", loc)
    _insert_order("O1", ck1, date(2024, 5, 10))
    _insert_order("O2", ck2, date(2024, 5, 20))
    _insert_order("O3", ck1, date(2024, 6, 15))  # only customer 1 returns

    build_customer_360()
    build_retention()

    period0 = _exec(
        "SELECT active_customers, cohort_size FROM analytics.customer_cohort "
        "WHERE cohort_month = %s AND period_number = 0", (date(2024, 5, 1),)
    )[0]
    assert period0 == (2, 2)

    period1 = _exec(
        "SELECT active_customers, cohort_size, retention_rate FROM analytics.customer_cohort "
        "WHERE cohort_month = %s AND period_number = 1", (date(2024, 5, 1),)
    )[0]
    assert period1[0] == 1  # only C_COHORT_1 returned
    assert period1[1] == 2
    assert float(period1[2]) == 0.5


# --------------------------------------------------------------------
# CLV
# --------------------------------------------------------------------
def test_zero_activity_customer_has_zero_estimated_clv(clean_all):
    loc = _insert_location()
    _insert_customer("C_NO_CLV", loc)

    _build_chain()

    row = _exec("SELECT estimated_clv, average_order_value, purchase_frequency_monthly FROM analytics.customer_clv WHERE customer_id = %s", ("C_NO_CLV",))[0]
    assert float(row[0]) == 0.0


def test_clv_formula_is_aov_times_frequency_times_lifespan(clean_all):
    loc = _insert_location()
    ck = _insert_customer("C_FORMULA", loc)
    _insert_order("O1", ck, TODAY - timedelta(days=10), amount=200.0)

    _build_chain()

    row = _exec(
        "SELECT average_order_value, purchase_frequency_monthly, estimated_lifespan_months, estimated_clv "
        "FROM analytics.customer_clv WHERE customer_id = %s", ("C_FORMULA",)
    )[0]
    aov, freq, lifespan, clv = [float(v) for v in row]
    assert clv == pytest.approx(aov * freq * lifespan, rel=0.01)


def test_historical_clv_matches_customer_360(clean_all):
    loc = _insert_location()
    ck = _insert_customer("C_HIST", loc)
    _insert_order("O1", ck, TODAY - timedelta(days=5), amount=777.0)

    _build_chain()

    c360_clv = _exec("SELECT customer_lifetime_value FROM analytics.customer_360 WHERE customer_id = %s", ("C_HIST",))[0][0]
    clv_table_hist = _exec("SELECT historical_clv FROM analytics.customer_clv WHERE customer_id = %s", ("C_HIST",))[0][0]
    assert float(c360_clv) == float(clv_table_hist) == 777.0
