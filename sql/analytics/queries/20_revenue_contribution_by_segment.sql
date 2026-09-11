-- =========================================================
-- Q20: Revenue contribution by customer segment (Pareto view)
-- Technique: window functions for share-of-total AND a running
-- cumulative share, ordered to show how concentrated revenue is
-- in the top segments (the "80/20" business insight).
-- =========================================================
WITH segment_revenue AS (
    SELECT
        customer_segment,
        COUNT(*) AS customer_count,
        SUM(total_spend) AS revenue
    FROM analytics.customer_360
    GROUP BY customer_segment
)
SELECT
    customer_segment,
    customer_count,
    ROUND(100.0 * customer_count / SUM(customer_count) OVER (), 2) AS pct_of_customers,
    revenue,
    ROUND(100.0 * revenue / SUM(revenue) OVER (), 2) AS pct_of_revenue,
    ROUND(
        100.0 * SUM(revenue) OVER (ORDER BY revenue DESC ROWS UNBOUNDED PRECEDING)
        / SUM(revenue) OVER (), 2
    ) AS cumulative_pct_of_revenue
FROM segment_revenue
ORDER BY revenue DESC;
