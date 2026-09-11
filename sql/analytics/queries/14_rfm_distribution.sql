-- =========================================================
-- Q14: RFM score distribution by segment
-- Technique: JOIN + aggregation across all three RFM components.
-- =========================================================
SELECT
    s.customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(r.r_score), 2) AS avg_r_score,
    ROUND(AVG(r.f_score), 2) AS avg_f_score,
    ROUND(AVG(r.m_score), 2) AS avg_m_score,
    ROUND(AVG(r.rfm_sum), 2) AS avg_rfm_sum
FROM analytics.customer_rfm r
JOIN analytics.customer_segments s ON s.customer_id = r.customer_id
GROUP BY s.customer_segment
ORDER BY avg_rfm_sum DESC;
