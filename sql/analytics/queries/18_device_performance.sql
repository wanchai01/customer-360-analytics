-- =========================================================
-- Q18: Device performance (sessions and conversion rate by device)
-- Technique: JOIN to dim_device + conditional aggregation.
-- =========================================================
SELECT
    d.device_name,
    COUNT(DISTINCT e.session_id) AS sessions,
    COUNT(*) AS total_events,
    COUNT(DISTINCT e.session_id) FILTER (WHERE e.event_type = 'purchase') AS converting_sessions,
    ROUND(
        100.0 * COUNT(DISTINCT e.session_id) FILTER (WHERE e.event_type = 'purchase')
        / NULLIF(COUNT(DISTINCT e.session_id), 0), 2
    ) AS conversion_rate_pct
FROM warehouse.fact_website_events e
JOIN warehouse.dim_device d ON d.device_key = e.device_key
GROUP BY d.device_name
ORDER BY sessions DESC;
