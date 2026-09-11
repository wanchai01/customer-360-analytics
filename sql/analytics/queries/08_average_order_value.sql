-- =========================================================
-- Q08: Average order value — overall and monthly trend
-- Technique: aggregation + date function + window function
-- (running average across the whole trend).
-- =========================================================
WITH monthly_aov AS (
    SELECT
        DATE_TRUNC('month', order_date)::date AS month,
        ROUND(AVG(total_amount), 2) AS avg_order_value,
        COUNT(*) AS order_count
    FROM warehouse.fact_orders
    WHERE order_status = 'completed'
    GROUP BY 1
)
SELECT
    month,
    avg_order_value,
    order_count,
    ROUND(AVG(avg_order_value) OVER (ORDER BY month ROWS UNBOUNDED PRECEDING), 2) AS running_avg_aov
FROM monthly_aov
ORDER BY month;
