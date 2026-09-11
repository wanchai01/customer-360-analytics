-- =========================================================
-- Q01: Top 10 customers by revenue
-- Technique: simple aggregation via customer_360 (already
-- pre-aggregated in Phase 8), ORDER BY + LIMIT.
-- =========================================================
SELECT
    customer_id,
    customer_name,
    customer_segment,
    total_orders,
    total_spend AS revenue
FROM analytics.customer_360
ORDER BY total_spend DESC
LIMIT 10;
