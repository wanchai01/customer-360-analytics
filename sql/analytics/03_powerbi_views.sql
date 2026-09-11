-- =========================================================
-- 03_powerbi_views.sql
--
-- One convenience view for the Power BI "Customer 360" page
-- (Section 17, Page 2): combines customer_360 and customer_clv into
-- a single flat source, so that page's visuals don't need a second
-- relationship in the model just to show both historical and
-- estimated CLV side by side.
--
-- This does NOT replace the relationships documented in
-- powerbi/data_model.md — customer_rfm and customer_segments still
-- relate to dim_customer independently for Page 3's segment-level
-- analysis, which needs them as separate tables to group and filter
-- by segment properly. This view exists purely for Page 2's
-- per-customer profile card, where a single flat row is simpler.
-- =========================================================

SET search_path TO analytics;

DROP VIEW IF EXISTS vw_powerbi_customer_360;
CREATE VIEW vw_powerbi_customer_360 AS
SELECT
    c.customer_id,
    c.customer_name,
    c.age,
    c.gender,
    c.location,
    c.registration_date,
    c.total_orders,
    c.completed_orders,
    c.cancelled_orders,
    c.total_spend,
    c.average_order_value,
    c.first_purchase_date,
    c.last_purchase_date,
    c.days_since_last_purchase,
    c.purchase_frequency,
    c.favorite_category,
    c.favorite_product,
    c.website_sessions,
    c.website_events,
    c.support_tickets,
    c.resolved_tickets,
    c.average_resolution_time,
    c.campaign_interactions,
    c.rfm_score,
    c.customer_segment,
    c.customer_lifetime_value AS historical_clv,
    clv.estimated_clv,
    clv.estimated_lifespan_months
FROM analytics.customer_360 c
LEFT JOIN analytics.customer_clv clv ON clv.customer_id = c.customer_id;
