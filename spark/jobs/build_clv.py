"""
build_clv.py — Computes Estimated (forward-looking) Customer Lifetime
Value (Section 14), stored in `analytics.customer_clv`.

Model:
    estimated_clv = average_order_value * purchase_frequency_monthly * estimated_lifespan_months

    estimated_lifespan_months = 1 / average_monthly_churn_rate

This is the standard "CLV = AOV x Purchase Frequency x Customer
Lifespan" formula, with lifespan derived from the churn rate this
project already computes in Phase 9's retention step (1 / churn rate
is the expected number of months a customer stays before churning,
under the simplifying assumption of a constant monthly churn
probability — i.e. treating churn as a geometric distribution).

Depends on `build_retention.py` having already populated
`analytics.customer_monthly_metrics` (its average churn_rate feeds
this model) and `build_customer_360.py` (its average_order_value /
purchase_frequency per customer feed this model).

**Explicit model limitations** (Section 14 asks for these to be
documented, not hidden):
  - A single global average lifespan is applied to every customer.
    Real customers churn at different rates (a Champion is not
    equally likely to leave as someone At Risk) — a per-segment or
    per-customer survival model would be more accurate, at the cost
    of needing much more historical data than this project's
    synthetic history to fit reliably.
  - The constant-churn-probability assumption (implying an
    exponential/geometric customer lifetime) is a simplification;
    real churn hazard is rarely constant over a customer's tenure
    (it's typically highest shortly after acquisition).
  - This is a *point estimate* computed from the current data
    snapshot — it does not account for seasonality, pricing changes,
    or future behavior shifts, and should be refreshed regularly
    rather than treated as a fixed number.
  - Customers with zero completed orders get `estimated_clv = 0`
    (their average_order_value and purchase_frequency are both 0) —
    correct under this formula, but worth remembering that a
    genuinely promising brand-new customer will show 0 until their
    first purchase lands.

Usage:
    python -m spark.jobs.build_clv
"""
from __future__ import annotations

import logging
import os

import psycopg2

logger = logging.getLogger("build_clv")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")

# Sanity bound: an extremely small or zero average churn rate would
# make 1/churn_rate blow up to an implausible lifespan (e.g. hundreds
# of years) — cap it so the model degrades gracefully instead of
# producing a headline-grabbing but meaningless CLV number.
MAX_LIFESPAN_MONTHS = 60  # 5 years, a generous but bounded assumption


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "customer360"),
        user=os.environ.get("POSTGRES_USER", "customer360_app"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


def _get_average_churn_rate(conn) -> float:
    with conn.cursor() as cur:
        cur.execute("SELECT AVG(churn_rate) FROM analytics.customer_monthly_metrics WHERE churn_rate IS NOT NULL AND churn_rate > 0")
        (avg_churn,) = cur.fetchone()
    if not avg_churn:
        logger.warning("No usable churn_rate history found — falling back to the lifespan cap of %s months", MAX_LIFESPAN_MONTHS)
        return 1.0 / MAX_LIFESPAN_MONTHS
    return float(avg_churn)


def build_clv() -> dict[str, float]:
    conn = get_connection()
    try:
        avg_churn_rate = _get_average_churn_rate(conn)
        lifespan_months = min(1.0 / avg_churn_rate, MAX_LIFESPAN_MONTHS)
        logger.info("Average monthly churn rate: %.4f -> estimated lifespan: %.1f months (capped at %s)", avg_churn_rate, lifespan_months, MAX_LIFESPAN_MONTHS)

        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE analytics.customer_clv")
            cur.execute(
                """
                INSERT INTO analytics.customer_clv
                    (customer_id, historical_clv, average_order_value, purchase_frequency_monthly,
                     estimated_lifespan_months, estimated_clv)
                SELECT
                    customer_id,
                    customer_lifetime_value AS historical_clv,
                    average_order_value,
                    purchase_frequency,
                    %s AS estimated_lifespan_months,
                    ROUND(average_order_value * purchase_frequency * %s, 2) AS estimated_clv
                FROM analytics.customer_360
                """,
                (lifespan_months, lifespan_months),
            )
            row_count = cur.rowcount
        conn.commit()

        logger.info("=== CLV built: %s rows (lifespan=%.1f months) ===", row_count, lifespan_months)
        return {"rows": row_count, "lifespan_months": lifespan_months, "avg_churn_rate": avg_churn_rate}
    finally:
        conn.close()


if __name__ == "__main__":
    build_clv()
