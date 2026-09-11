-- =========================================================
-- Q09: Overall repeat purchase rate
-- Technique: subqueries for numerator/denominator, CASE-free
-- ratio calculation.
-- =========================================================
SELECT
    (SELECT COUNT(*) FROM analytics.customer_360 WHERE completed_orders >= 2) AS repeat_customers,
    (SELECT COUNT(*) FROM analytics.customer_360 WHERE completed_orders >= 1) AS all_purchasers,
    ROUND(
        (SELECT COUNT(*) FROM analytics.customer_360 WHERE completed_orders >= 2)::numeric
        / NULLIF((SELECT COUNT(*) FROM analytics.customer_360 WHERE completed_orders >= 1), 0),
        4
    ) AS repeat_purchase_rate;
