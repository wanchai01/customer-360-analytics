"""
build_retention.py — Computes Section 15's retention/churn time series
(`analytics.customer_monthly_metrics`) and cohort retention matrix
(`analytics.customer_cohort`), both derived from each customer's set
of distinct calendar months with at least one COMPLETED order.

Definitions used (again, made explicit since the spec names the
metrics but not the exact formulas):
  - active_customers(M)     : distinct customers with >=1 completed
                               order in month M
  - new_customers(M)        : distinct customers whose FIRST-ever
                               completed order falls in month M
  - returning_customers(M)  : active_customers(M) - new_customers(M)
                               (active this month, but not for the
                               first time)
  - retention_rate(M)       : of customers active in month M-1, the
                               fraction who are ALSO active in month M
                               — the standard month-over-month
                               retention definition (not to be
                               confused with repeat_purchase_rate).
  - churn_rate(M)           : 1 - retention_rate(M) (of last month's
                               active customers, the fraction who did
                               NOT come back this month)
  - repeat_purchase_rate(M) : of customers active in month M, the
                               fraction who are repeat buyers
                               (returning_customers(M) / active_customers(M))
                               — distinct from retention_rate: this is
                               "how many of this month's buyers have
                               bought before, ever", not "did last
                               month's buyers come back".

Cohort matrix: cohort_month = the month of a customer's first
purchase; period_number = months elapsed since then; retention_rate
here = fraction of that cohort's original size still active in that
period — the classic cohort-retention-curve shape.

Usage:
    python -m spark.jobs.build_retention
"""
from __future__ import annotations

import logging
import os

import psycopg2

logger = logging.getLogger("build_retention")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "customer360"),
        user=os.environ.get("POSTGRES_USER", "customer360_app"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


BUILD_MONTHLY_METRICS_SQL = """
INSERT INTO analytics.customer_monthly_metrics
    (metric_month, active_customers, new_customers, returning_customers, retention_rate, churn_rate, repeat_purchase_rate, total_revenue)
WITH customer_months AS (
    SELECT DISTINCT customer_key, DATE_TRUNC('month', order_date)::date AS activity_month
    FROM warehouse.fact_orders
    WHERE order_status = 'completed'
),
first_month AS (
    SELECT customer_key, MIN(activity_month) AS cohort_month
    FROM customer_months
    GROUP BY customer_key
),
month_spine AS (
    -- Extends to GREATEST(last activity month, current month) — not
    -- just the last month with activity. Stopping at the last active
    -- month would make it structurally impossible to ever see a
    -- customer's churn: the month after their final purchase would
    -- have no row at all, so "were they active in a month where they
    -- in fact went quiet" could never be answered. Found via a test
    -- with a customer whose only order was one month in the past —
    -- the expected "next month, 0% retention" row simply didn't
    -- exist.
    SELECT GENERATE_SERIES(
        (SELECT MIN(activity_month) FROM customer_months),
        GREATEST(
            (SELECT MAX(activity_month) FROM customer_months),
            DATE_TRUNC('month', CURRENT_DATE)::date
        ),
        INTERVAL '1 month'
    )::date AS metric_month
),
monthly_revenue AS (
    SELECT DATE_TRUNC('month', order_date)::date AS metric_month, SUM(total_amount) AS total_revenue
    FROM warehouse.fact_orders
    WHERE order_status = 'completed'
    GROUP BY 1
),
active AS (
    SELECT
        ms.metric_month,
        COUNT(DISTINCT cm.customer_key) AS active_customers,
        COUNT(DISTINCT cm.customer_key) FILTER (WHERE fm.cohort_month = ms.metric_month) AS new_customers
    FROM month_spine ms
    LEFT JOIN customer_months cm ON cm.activity_month = ms.metric_month
    LEFT JOIN first_month fm ON fm.customer_key = cm.customer_key
    GROUP BY ms.metric_month
),
retained AS (
    -- customers active in month M who were ALSO active in month M-1
    SELECT
        cm_curr.activity_month AS metric_month,
        COUNT(DISTINCT cm_curr.customer_key) AS retained_customers
    FROM customer_months cm_curr
    JOIN customer_months cm_prev
        ON cm_prev.customer_key = cm_curr.customer_key
        AND cm_prev.activity_month = (cm_curr.activity_month - INTERVAL '1 month')::date
    GROUP BY cm_curr.activity_month
)
SELECT
    a.metric_month,
    a.active_customers,
    a.new_customers,
    (a.active_customers - a.new_customers) AS returning_customers,
    CASE WHEN prev.active_customers > 0
         THEN ROUND(COALESCE(r.retained_customers, 0)::numeric / prev.active_customers, 4)
         ELSE NULL END AS retention_rate,
    CASE WHEN prev.active_customers > 0
         THEN ROUND(1 - (COALESCE(r.retained_customers, 0)::numeric / prev.active_customers), 4)
         ELSE NULL END AS churn_rate,
    CASE WHEN a.active_customers > 0
         THEN ROUND((a.active_customers - a.new_customers)::numeric / a.active_customers, 4)
         ELSE NULL END AS repeat_purchase_rate,
    COALESCE(mr.total_revenue, 0) AS total_revenue
FROM active a
LEFT JOIN active prev ON prev.metric_month = (a.metric_month - INTERVAL '1 month')::date
LEFT JOIN retained r ON r.metric_month = a.metric_month
LEFT JOIN monthly_revenue mr ON mr.metric_month = a.metric_month
ORDER BY a.metric_month;
"""

BUILD_COHORT_SQL = """
INSERT INTO analytics.customer_cohort (cohort_month, period_number, active_customers, cohort_size, retention_rate)
WITH customer_months AS (
    SELECT DISTINCT customer_key, DATE_TRUNC('month', order_date)::date AS activity_month
    FROM warehouse.fact_orders
    WHERE order_status = 'completed'
),
first_month AS (
    SELECT customer_key, MIN(activity_month) AS cohort_month
    FROM customer_months
    GROUP BY customer_key
),
cohort_activity AS (
    SELECT
        fm.cohort_month,
        (DATE_PART('year', AGE(cm.activity_month, fm.cohort_month)) * 12
            + DATE_PART('month', AGE(cm.activity_month, fm.cohort_month)))::int AS period_number,
        cm.customer_key
    FROM customer_months cm
    JOIN first_month fm ON fm.customer_key = cm.customer_key
),
cohort_sizes AS (
    SELECT cohort_month, COUNT(DISTINCT customer_key) AS cohort_size
    FROM cohort_activity WHERE period_number = 0
    GROUP BY cohort_month
)
SELECT
    ca.cohort_month,
    ca.period_number,
    COUNT(DISTINCT ca.customer_key) AS active_customers,
    cs.cohort_size,
    ROUND(COUNT(DISTINCT ca.customer_key)::numeric / cs.cohort_size, 4) AS retention_rate
FROM cohort_activity ca
JOIN cohort_sizes cs ON cs.cohort_month = ca.cohort_month
GROUP BY ca.cohort_month, ca.period_number, cs.cohort_size
ORDER BY ca.cohort_month, ca.period_number;
"""


def build_retention() -> dict[str, int]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE analytics.customer_monthly_metrics")
            cur.execute(BUILD_MONTHLY_METRICS_SQL)
            monthly_count = cur.rowcount

            cur.execute("TRUNCATE TABLE analytics.customer_cohort")
            cur.execute(BUILD_COHORT_SQL)
            cohort_count = cur.rowcount
        conn.commit()

        logger.info("=== Retention: %s monthly rows, %s cohort rows ===", monthly_count, cohort_count)
        return {"monthly_metrics": monthly_count, "cohort": cohort_count}
    finally:
        conn.close()


if __name__ == "__main__":
    build_retention()
