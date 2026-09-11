-- =========================================================
-- Q07: At-risk customers (was valuable, recency slipping)
-- Technique: JOIN customer_360 + customer_rfm, subquery for
-- the "average spend" benchmark used to flag high-value risk.
-- =========================================================
SELECT
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    r.recency_days,
    r.frequency,
    r.monetary,
    c.total_spend > (SELECT AVG(total_spend) FROM analytics.customer_360 WHERE total_orders > 0) AS above_average_spender
FROM analytics.customer_360 c
JOIN analytics.customer_rfm r ON r.customer_id = c.customer_id
WHERE c.customer_segment IN ('At Risk', 'Can''t Lose Them')
ORDER BY r.monetary DESC;
