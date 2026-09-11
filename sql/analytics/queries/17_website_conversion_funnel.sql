-- =========================================================
-- Q17: Website conversion funnel (sessions through to purchase)
-- Technique: CTE + conditional aggregation to build a funnel,
-- CASE-based stage-to-stage conversion rates.
-- =========================================================
WITH session_funnel AS (
    SELECT
        session_id,
        MAX((event_type = 'product_view')::int) AS viewed_product,
        MAX((event_type = 'add_to_cart')::int) AS added_to_cart,
        MAX((event_type = 'checkout_start')::int) AS started_checkout,
        MAX((event_type = 'purchase')::int) AS purchased
    FROM warehouse.fact_website_events
    GROUP BY session_id
)
SELECT
    COUNT(*) AS total_sessions,
    SUM(viewed_product) AS viewed_product,
    SUM(added_to_cart) AS added_to_cart,
    SUM(started_checkout) AS started_checkout,
    SUM(purchased) AS purchased,
    ROUND(100.0 * SUM(added_to_cart) / NULLIF(SUM(viewed_product), 0), 2) AS view_to_cart_pct,
    ROUND(100.0 * SUM(purchased) / NULLIF(SUM(started_checkout), 0), 2) AS checkout_to_purchase_pct,
    ROUND(100.0 * SUM(purchased) / COUNT(*), 2) AS overall_conversion_pct
FROM session_funnel;
