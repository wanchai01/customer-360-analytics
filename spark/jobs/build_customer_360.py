"""
build_customer_360.py — Computes the Gold-layer `analytics.customer_360`
table (Section 12) from the `warehouse.*` Star Schema built in Phase 7.

Business definitions chosen (documented here since the spec leaves
several of these to judgment, and a real analytics team would need
this written down somewhere):
  - total_spend / average_order_value are based on COMPLETED orders
    only — a cancelled or refunded order was never actually realized
    revenue, so including it would overstate how much a customer
    has genuinely spent.
  - purchase_frequency = completed_orders / months_since_first_purchase
    (minimum 1 month, to avoid a division blow-up for a customer whose
    first purchase was this month).
  - favorite_category / favorite_product = the category/product with
    the most order_items line entries for that customer (ties broken
    by total quantity, then arbitrarily but deterministically by name).
  - customer_lifetime_value here is *historical* CLV (= total_spend):
    actual realized revenue to date. Phase 9 adds a separate
    *estimated* (forward-looking) CLV model alongside this — the two
    are deliberately kept distinct rather than overwriting this column,
    since conflating "what they've spent" with "what we predict they
    will spend" is a common and misleading mistake in CLV reporting.
  - rfm_score / customer_segment are intentionally left NULL here —
    Phase 9 (build_rfm.py) computes and back-fills them from
    `analytics.customer_rfm` once RFM scoring exists.

Usage:
    python -m spark.jobs.build_customer_360
"""
from __future__ import annotations

import logging
import os

import psycopg2

logger = logging.getLogger("build_customer_360")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "customer360"),
        user=os.environ.get("POSTGRES_USER", "customer360_app"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


# One comprehensive query rather than N round-trips: every metric is a
# LEFT JOIN against a per-customer aggregate CTE, so a customer with
# zero orders/events/tickets still gets a row (with zeros/NULLs), not
# silently dropped — that "zero-activity customer" case matters for
# segmentation later (they'd be a clear "New Customer" or "Lost").
BUILD_SQL = """
INSERT INTO analytics.customer_360 (
    customer_id, customer_name, age, gender, location, registration_date,
    total_orders, completed_orders, cancelled_orders, total_spend, average_order_value,
    first_purchase_date, last_purchase_date, days_since_last_purchase, purchase_frequency,
    favorite_category, favorite_product, website_sessions, website_events,
    support_tickets, resolved_tickets, average_resolution_time,
    campaign_interactions, customer_lifetime_value
)
WITH order_stats AS (
    SELECT
        customer_key,
        COUNT(*) AS total_orders,
        COUNT(*) FILTER (WHERE order_status = 'completed') AS completed_orders,
        COUNT(*) FILTER (WHERE order_status = 'cancelled') AS cancelled_orders,
        COALESCE(SUM(total_amount) FILTER (WHERE order_status = 'completed'), 0) AS total_spend,
        MIN(order_date) FILTER (WHERE order_status = 'completed') AS first_purchase_date,
        MAX(order_date) FILTER (WHERE order_status = 'completed') AS last_purchase_date
    FROM warehouse.fact_orders
    GROUP BY customer_key
),
item_ranks AS (
    SELECT
        fo.customer_key,
        dp.category,
        dp.product_name,
        COUNT(*) AS line_count,
        SUM(foi.quantity) AS total_quantity,
        RANK() OVER (PARTITION BY fo.customer_key ORDER BY COUNT(*) DESC, SUM(foi.quantity) DESC, dp.product_name) AS product_rank
    FROM warehouse.fact_order_items foi
    JOIN warehouse.fact_orders fo ON fo.order_key = foi.order_key
    JOIN warehouse.dim_product dp ON dp.product_key = foi.product_key
    GROUP BY fo.customer_key, dp.category, dp.product_name
),
favorite_product AS (
    SELECT customer_key, product_name AS favorite_product, category AS favorite_category
    FROM item_ranks WHERE product_rank = 1
),
category_totals AS (
    SELECT
        fo.customer_key,
        dp.category,
        SUM(foi.quantity) AS qty,
        RANK() OVER (PARTITION BY fo.customer_key ORDER BY SUM(foi.quantity) DESC, dp.category) AS category_rank
    FROM warehouse.fact_order_items foi
    JOIN warehouse.fact_orders fo ON fo.order_key = foi.order_key
    JOIN warehouse.dim_product dp ON dp.product_key = foi.product_key
    GROUP BY fo.customer_key, dp.category
),
favorite_category AS (
    SELECT customer_key, category AS favorite_category FROM category_totals WHERE category_rank = 1
),
event_stats AS (
    SELECT
        customer_key,
        COUNT(DISTINCT session_id) AS website_sessions,
        COUNT(*) AS website_events
    FROM warehouse.fact_website_events
    GROUP BY customer_key
),
ticket_stats AS (
    SELECT
        customer_key,
        COUNT(*) AS support_tickets,
        COUNT(*) FILTER (WHERE status IN ('resolved', 'closed')) AS resolved_tickets,
        AVG(resolution_time) AS average_resolution_time
    FROM warehouse.fact_support_tickets
    GROUP BY customer_key
),
campaign_stats AS (
    SELECT customer_key, COUNT(*) AS campaign_interactions
    FROM warehouse.fact_campaign_interactions
    GROUP BY customer_key
)
SELECT
    c.customer_id,
    c.full_name AS customer_name,
    DATE_PART('year', AGE(CURRENT_DATE, c.date_of_birth))::INTEGER AS age,
    c.gender,
    NULLIF(TRIM(CONCAT_WS(', ', l.city, l.province)), '') AS location,
    c.registration_date,
    COALESCE(os.total_orders, 0) AS total_orders,
    COALESCE(os.completed_orders, 0) AS completed_orders,
    COALESCE(os.cancelled_orders, 0) AS cancelled_orders,
    COALESCE(os.total_spend, 0) AS total_spend,
    CASE WHEN COALESCE(os.completed_orders, 0) > 0
         THEN ROUND(os.total_spend / os.completed_orders, 2)
         ELSE 0 END AS average_order_value,
    os.first_purchase_date,
    os.last_purchase_date,
    CASE WHEN os.last_purchase_date IS NOT NULL
         THEN (CURRENT_DATE - os.last_purchase_date::date)
         ELSE NULL END AS days_since_last_purchase,
    CASE WHEN os.first_purchase_date IS NOT NULL AND COALESCE(os.completed_orders, 0) > 0
         THEN ROUND(
              os.completed_orders / GREATEST(
                  DATE_PART('year', AGE(CURRENT_DATE, os.first_purchase_date)) * 12
                  + DATE_PART('month', AGE(CURRENT_DATE, os.first_purchase_date)),
                  1
              )::numeric, 4)
         ELSE 0 END AS purchase_frequency,
    fc.favorite_category,
    fp.favorite_product,
    COALESCE(es.website_sessions, 0) AS website_sessions,
    COALESCE(es.website_events, 0) AS website_events,
    COALESCE(ts.support_tickets, 0) AS support_tickets,
    COALESCE(ts.resolved_tickets, 0) AS resolved_tickets,
    ROUND(ts.average_resolution_time::numeric, 2) AS average_resolution_time,
    COALESCE(cs.campaign_interactions, 0) AS campaign_interactions,
    COALESCE(os.total_spend, 0) AS customer_lifetime_value  -- historical CLV; see module docstring
FROM warehouse.dim_customer c
LEFT JOIN warehouse.dim_location l ON l.location_key = c.location_key
LEFT JOIN order_stats os ON os.customer_key = c.customer_key
LEFT JOIN favorite_product fp ON fp.customer_key = c.customer_key
LEFT JOIN favorite_category fc ON fc.customer_key = c.customer_key
LEFT JOIN event_stats es ON es.customer_key = c.customer_key
LEFT JOIN ticket_stats ts ON ts.customer_key = c.customer_key
LEFT JOIN campaign_stats cs ON cs.customer_key = c.customer_key;
"""


def build_customer_360() -> int:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE analytics.customer_360")
            cur.execute(BUILD_SQL)
            row_count = cur.rowcount
        conn.commit()
        logger.info("=== customer_360 built: %s rows ===", row_count)
        return row_count
    finally:
        conn.close()


if __name__ == "__main__":
    build_customer_360()
