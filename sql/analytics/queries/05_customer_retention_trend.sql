-- =========================================================
-- Q05: Customer retention trend (last 12 months on record)
-- Technique: reads the pre-built Phase 9 monthly metrics table;
-- window function for a 3-month moving average to smooth
-- month-to-month volatility.
-- =========================================================
SELECT
    metric_month,
    active_customers,
    retention_rate,
    ROUND(AVG(retention_rate) OVER (
        ORDER BY metric_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 4) AS retention_rate_3mo_avg
FROM analytics.customer_monthly_metrics
ORDER BY metric_month DESC
LIMIT 12;
