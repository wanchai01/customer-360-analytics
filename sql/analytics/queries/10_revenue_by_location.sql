-- =========================================================
-- Q10: Revenue by location (province)
-- Technique: JOIN across fact_orders -> dim_customer -> dim_location,
-- aggregation, window function for share-of-total.
-- =========================================================
SELECT
    l.province,
    COUNT(DISTINCT fo.customer_key) AS customers,
    SUM(fo.total_amount) AS revenue,
    ROUND(100.0 * SUM(fo.total_amount) / SUM(SUM(fo.total_amount)) OVER (), 2) AS pct_of_total_revenue
FROM warehouse.fact_orders fo
JOIN warehouse.dim_customer c ON c.customer_key = fo.customer_key
JOIN warehouse.dim_location l ON l.location_key = c.location_key
WHERE fo.order_status = 'completed'
GROUP BY l.province
ORDER BY revenue DESC;
