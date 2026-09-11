-- =========================================================
-- Q06: Churned customers (segment = 'Lost Customers')
-- Technique: JOIN + CASE to classify how they went quiet.
-- =========================================================
SELECT
    c.customer_id,
    c.customer_name,
    c.total_orders,
    c.total_spend,
    c.last_purchase_date,
    c.days_since_last_purchase,
    CASE
        WHEN c.total_orders = 0 THEN 'Never purchased'
        WHEN c.days_since_last_purchase > 365 THEN 'Inactive > 1 year'
        ELSE 'Inactive 90-365 days'
    END AS churn_reason
FROM analytics.customer_360 c
WHERE c.customer_segment = 'Lost Customers'
ORDER BY c.total_spend DESC;
