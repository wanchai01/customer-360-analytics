-- =========================================================
-- Q13: Estimated CLV distribution across customers
-- Technique: CASE-based bucketing, aggregation.
-- =========================================================
SELECT
    CASE
        WHEN estimated_clv = 0 THEN '0 (no purchases)'
        WHEN estimated_clv < 1000 THEN '1 - 999'
        WHEN estimated_clv < 5000 THEN '1,000 - 4,999'
        WHEN estimated_clv < 20000 THEN '5,000 - 19,999'
        ELSE '20,000+'
    END AS clv_bucket,
    COUNT(*) AS customer_count,
    ROUND(AVG(estimated_clv), 2) AS avg_clv_in_bucket
FROM analytics.customer_clv
GROUP BY 1
ORDER BY MIN(estimated_clv);
