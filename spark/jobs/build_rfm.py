"""
build_rfm.py — Computes RFM (Recency/Frequency/Monetary) scores and
customer segments (Section 13), then back-fills `rfm_score` /
`customer_segment` into `analytics.customer_360`.

RFM source data is read from the already-built `analytics.customer_360`
(days_since_last_purchase, completed_orders, total_spend) rather than
re-deriving it from the warehouse — Phase 8 already computed these
correctly (completed-orders-only, etc.), and re-deriving them here
independently would risk the two tables silently disagreeing.

Scoring method: NTILE(5) quintiles, not fixed thresholds. Fixed
thresholds (e.g. "frequency >= 10 -> score 5") assume a stable
absolute scale that doesn't hold across different data volumes or
business stages; quintiles always produce a full 1-5 spread relative
to *this* customer base, which is what makes the resulting segments
comparable dashboard-to-dashboard even as the underlying data changes.

**Limitation**: NTILE needs enough rows to actually spread across 5
buckets — with very few buyers (e.g. under ~25), Postgres front-loads
low bucket numbers first regardless of value (a single buyer among a
handful of others lands in bucket 1, not 5, even if they're clearly
the best customer by every measure). This project's `test` scale
preset (a few hundred customers) sits close to that edge; `dev` and
`large` scale have comfortably enough buyers for quintiles to behave
as intended.

Segment logic (Section 13's nine named segments), applied as an
ordered CASE — most specific/extreme rules first:
  Champions          : recent, frequent, AND high-value (R,F,M all >= 4)
  Loyal Customers     : frequent and reasonably recent (F >= 4, R >= 3)
  Can't Lose Them     : USED to buy a lot but has gone quiet (F >= 4, R <= 2)
                         — the highest-priority win-back target: a
                         previously high-value customer going cold.
  At Risk             : moderately frequent but recency has slipped (F 2-3, R <= 2)
  Potential Loyalists : recent with growing frequency (R >= 4, F 2-3)
  New Customers       : recent, first (or near-first) purchase (R >= 4, F <= 1)
  Promising           : average recency, low frequency so far (R = 3, F <= 2)
  Need Attention      : average recency but was frequent (R = 3, F >= 3)
  Lost Customers      : long gone and was never frequent (R <= 2, F <= 1)
  (never-purchased customers are handled separately — see NEVER_PURCHASED_SQL)

Usage:
    python -m spark.jobs.build_rfm
"""
from __future__ import annotations

import logging
import os

import psycopg2

logger = logging.getLogger("build_rfm")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "customer360"),
        user=os.environ.get("POSTGRES_USER", "customer360_app"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


# Never-purchased customers (frequency=0) can't be quintile-scored
# meaningfully alongside buyers — they get the fixed worst scores
# (1,1,1) and are segmented separately below by how recently they
# registered, rather than by a recency-of-purchase that doesn't exist.
BUILD_RFM_SQL = """
INSERT INTO analytics.customer_rfm (customer_id, recency_days, frequency, monetary, r_score, f_score, m_score, rfm_score, rfm_sum)
WITH buyers AS (
    SELECT customer_id, days_since_last_purchase AS recency_days, completed_orders AS frequency, total_spend AS monetary
    FROM analytics.customer_360
    WHERE completed_orders > 0
),
scored AS (
    SELECT
        customer_id, recency_days, frequency, monetary,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,  -- larger recency_days = less recent = worse; DESC+NTILE puts smallest recency (best) in group 5
        NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
    FROM buyers
),
never_purchased AS (
    SELECT customer_id, NULL::INTEGER AS recency_days, 0 AS frequency, 0::NUMERIC AS monetary,
           1::SMALLINT AS r_score, 1::SMALLINT AS f_score, 1::SMALLINT AS m_score
    FROM analytics.customer_360
    WHERE completed_orders = 0
)
SELECT customer_id, recency_days, frequency, monetary, r_score, f_score, m_score,
       (r_score::text || f_score::text || m_score::text) AS rfm_score,
       (r_score + f_score + m_score) AS rfm_sum
FROM scored
UNION ALL
SELECT customer_id, recency_days, frequency, monetary, r_score, f_score, m_score,
       (r_score::text || f_score::text || m_score::text) AS rfm_score,
       (r_score + f_score + m_score) AS rfm_sum
FROM never_purchased;
"""

# Never-purchased customers: 'New Customers' if they registered
# recently (still plausibly about to make a first purchase),
# otherwise 'Lost Customers' (registered a long time ago, never
# converted).
BUILD_SEGMENTS_SQL = """
INSERT INTO analytics.customer_segments (customer_id, customer_segment)
SELECT
    r.customer_id,
    CASE
        WHEN r.frequency = 0 AND c.registration_date >= CURRENT_DATE - INTERVAL '90 days' THEN 'New Customers'
        WHEN r.frequency = 0 THEN 'Lost Customers'
        WHEN r.r_score >= 4 AND r.f_score >= 4 AND r.m_score >= 4 THEN 'Champions'
        WHEN r.f_score >= 4 AND r.r_score >= 3 THEN 'Loyal Customers'
        WHEN r.f_score >= 4 AND r.r_score <= 2 THEN 'Can''t Lose Them'
        WHEN r.f_score BETWEEN 2 AND 3 AND r.r_score <= 2 THEN 'At Risk'
        WHEN r.r_score >= 4 AND r.f_score BETWEEN 2 AND 3 THEN 'Potential Loyalists'
        WHEN r.r_score >= 4 AND r.f_score <= 1 THEN 'New Customers'
        WHEN r.r_score = 3 AND r.f_score <= 2 THEN 'Promising'
        WHEN r.r_score = 3 AND r.f_score >= 3 THEN 'Need Attention'
        WHEN r.r_score <= 2 AND r.f_score <= 1 THEN 'Lost Customers'
        ELSE 'Need Attention'  -- deterministic catch-all for any combination not covered above
    END AS customer_segment
FROM analytics.customer_rfm r
JOIN warehouse.dim_customer c ON c.customer_id = r.customer_id;
"""

BACKFILL_CUSTOMER_360_SQL = """
UPDATE analytics.customer_360 c
SET rfm_score = r.rfm_score,
    customer_segment = s.customer_segment
FROM analytics.customer_rfm r
JOIN analytics.customer_segments s ON s.customer_id = r.customer_id
WHERE c.customer_id = r.customer_id;
"""


def build_rfm() -> dict[str, int]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE analytics.customer_rfm")
            cur.execute(BUILD_RFM_SQL)
            rfm_count = cur.rowcount

            cur.execute("TRUNCATE TABLE analytics.customer_segments")
            cur.execute(BUILD_SEGMENTS_SQL)
            segment_count = cur.rowcount

            cur.execute(BACKFILL_CUSTOMER_360_SQL)
            backfill_count = cur.rowcount
        conn.commit()

        logger.info("=== RFM: %s scored, %s segmented, %s customer_360 rows back-filled ===", rfm_count, segment_count, backfill_count)
        return {"rfm": rfm_count, "segments": segment_count, "backfilled": backfill_count}
    finally:
        conn.close()


if __name__ == "__main__":
    build_rfm()
