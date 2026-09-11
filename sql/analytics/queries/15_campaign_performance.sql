-- =========================================================
-- Q15: Campaign performance (impressions -> clicks -> conversions)
-- Technique: conditional aggregation (FILTER), CASE-based rate
-- calculation, JOIN to dim_campaign.
-- =========================================================
SELECT
    camp.campaign_name,
    camp.channel,
    camp.budget,
    COUNT(*) FILTER (WHERE ci.interaction_type = 'impression') AS impressions,
    COUNT(*) FILTER (WHERE ci.interaction_type = 'click') AS clicks,
    COUNT(*) FILTER (WHERE ci.interaction_type = 'conversion') AS conversions,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE ci.interaction_type = 'click')
        / NULLIF(COUNT(*) FILTER (WHERE ci.interaction_type = 'impression'), 0), 2
    ) AS click_through_rate_pct,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE ci.interaction_type = 'conversion')
        / NULLIF(COUNT(*) FILTER (WHERE ci.interaction_type = 'click'), 0), 2
    ) AS conversion_rate_pct
FROM warehouse.dim_campaign camp
LEFT JOIN warehouse.fact_campaign_interactions ci ON ci.campaign_key = camp.campaign_key
GROUP BY camp.campaign_key, camp.campaign_name, camp.channel, camp.budget
ORDER BY conversions DESC;
