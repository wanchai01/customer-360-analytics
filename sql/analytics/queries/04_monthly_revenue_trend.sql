-- =========================================================
-- Q04: Monthly revenue trend
-- Technique: date function (DATE_TRUNC), aggregation, window
-- function for month-over-month growth (LAG).
-- =========================================================
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', order_date)::date AS month,
        SUM(total_amount) AS revenue
    FROM warehouse.fact_orders
    WHERE order_status = 'completed'
    GROUP BY 1
)
SELECT
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
    ROUND(
        100.0 * (revenue - LAG(revenue) OVER (ORDER BY month))
        / NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 2
    ) AS mom_growth_pct
FROM monthly
ORDER BY month;
