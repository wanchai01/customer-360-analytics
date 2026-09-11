-- =========================================================
-- Q03: Revenue and customer count by segment
-- Technique: GROUP BY + aggregation on the Gold customer_360 table.
-- =========================================================
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    SUM(total_spend) AS total_revenue,
    ROUND(AVG(total_spend), 2) AS avg_revenue_per_customer
FROM analytics.customer_360
GROUP BY customer_segment
ORDER BY total_revenue DESC;
