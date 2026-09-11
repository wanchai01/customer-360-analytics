-- =========================================================
-- Q16: Support ticket analysis by issue type and priority
-- Technique: GROUP BY multiple dimensions, conditional
-- aggregation for resolution rate.
-- =========================================================
SELECT
    issue_type,
    priority,
    COUNT(*) AS ticket_count,
    COUNT(*) FILTER (WHERE status IN ('resolved', 'closed')) AS resolved_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE status IN ('resolved', 'closed')) / COUNT(*), 2
    ) AS resolution_rate_pct,
    ROUND(AVG(resolution_time) FILTER (WHERE resolution_time IS NOT NULL), 2) AS avg_resolution_hours
FROM warehouse.fact_support_tickets
GROUP BY issue_type, priority
ORDER BY ticket_count DESC;
