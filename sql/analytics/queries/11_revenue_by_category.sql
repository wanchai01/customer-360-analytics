-- =========================================================
-- Q11: Revenue by product category
-- Technique: JOIN + aggregation + RANK() window function.
-- =========================================================
SELECT
    p.category,
    SUM(foi.line_amount) AS revenue,
    RANK() OVER (ORDER BY SUM(foi.line_amount) DESC) AS revenue_rank
FROM warehouse.fact_order_items foi
JOIN warehouse.fact_orders fo ON fo.order_key = foi.order_key
JOIN warehouse.dim_product p ON p.product_key = foi.product_key
WHERE fo.order_status = 'completed'
GROUP BY p.category
ORDER BY revenue DESC;
