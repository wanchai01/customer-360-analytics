-- =========================================================
-- Q12: Customer acquisition trend (new registrations per month)
-- Technique: date function + window function (cumulative SUM
-- for running total of customers acquired).
-- =========================================================
WITH monthly_signups AS (
    SELECT
        DATE_TRUNC('month', registration_date)::date AS month,
        COUNT(*) AS new_customers
    FROM warehouse.dim_customer
    GROUP BY 1
)
SELECT
    month,
    new_customers,
    SUM(new_customers) OVER (ORDER BY month ROWS UNBOUNDED PRECEDING) AS cumulative_customers
FROM monthly_signups
ORDER BY month;
