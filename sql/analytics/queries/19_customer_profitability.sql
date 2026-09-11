-- =========================================================
-- Q19: Customer profitability (revenue minus product cost)
-- Technique: JOIN across order_items -> orders -> product for
-- cost basis, aggregation, RANK() window function.
-- =========================================================
WITH customer_profit AS (
    SELECT
        c.customer_id,
        c.full_name AS customer_name,
        -- COALESCE to 0: a row with a cleaned (nulled) unit_price
        -- makes its line_amount NULL, which would otherwise make the
        -- whole SUM NULL for any customer whose *entire* order
        -- history hit that data-quality edge case — and Postgres
        -- sorts NULL first in a DESC RANK(), which silently put a
        -- near-zero-revenue customer in the #1 profitability spot.
        -- Found by inspecting the actual top-10 output, not assumed.
        COALESCE(SUM(foi.line_amount), 0) AS revenue,
        COALESCE(SUM(foi.quantity * p.cost), 0) AS cost_of_goods,
        COALESCE(SUM(foi.line_amount), 0) - COALESCE(SUM(foi.quantity * p.cost), 0) AS gross_profit
    FROM warehouse.fact_order_items foi
    JOIN warehouse.fact_orders fo ON fo.order_key = foi.order_key
    JOIN warehouse.dim_product p ON p.product_key = foi.product_key
    JOIN warehouse.dim_customer c ON c.customer_key = fo.customer_key
    WHERE fo.order_status = 'completed'
    GROUP BY c.customer_id, c.full_name
)
SELECT
    customer_id,
    customer_name,
    revenue,
    cost_of_goods,
    gross_profit,
    ROUND(100.0 * gross_profit / NULLIF(revenue, 0), 2) AS gross_margin_pct,
    RANK() OVER (ORDER BY gross_profit DESC) AS profitability_rank
FROM customer_profit
ORDER BY gross_profit DESC
LIMIT 10;
