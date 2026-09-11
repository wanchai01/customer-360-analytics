-- =========================================================
-- Q02: Top 10 products by revenue
-- Technique: JOIN across fact_order_items -> fact_orders (to
-- filter completed) -> dim_product, GROUP BY + aggregation.
-- =========================================================
SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(foi.quantity) AS units_sold,
    SUM(foi.line_amount) AS revenue
FROM warehouse.fact_order_items foi
JOIN warehouse.fact_orders fo ON fo.order_key = foi.order_key
JOIN warehouse.dim_product p ON p.product_key = foi.product_key
WHERE fo.order_status = 'completed'
GROUP BY p.product_id, p.product_name, p.category
ORDER BY revenue DESC
LIMIT 10;
