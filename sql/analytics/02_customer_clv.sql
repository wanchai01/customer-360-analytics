-- =========================================================
-- 02_customer_clv.sql — Estimated Customer Lifetime Value (Section 14)
--
-- Kept as its own table rather than a column on customer_360:
-- customer_360.customer_lifetime_value is *historical* CLV (=
-- total_spend to date, Phase 8). This table holds the *estimated*
-- (forward-looking) CLV model's inputs and output side by side, so
-- the model's assumptions are visible and auditable rather than
-- a single opaque number sitting next to a differently-defined one.
-- =========================================================

SET search_path TO analytics;

DROP TABLE IF EXISTS customer_clv CASCADE;
CREATE TABLE customer_clv (
    customer_id                 TEXT PRIMARY KEY,
    historical_clv               NUMERIC(14,2),   -- = customer_360.customer_lifetime_value, duplicated here for a single query surface over the whole CLV picture
    average_order_value          NUMERIC(12,2),
    purchase_frequency_monthly   NUMERIC(10,4),    -- completed orders per month (same figure as customer_360.purchase_frequency)
    estimated_lifespan_months    NUMERIC(10,2),    -- see build_clv.py: 1 / average monthly churn rate, applied uniformly
    estimated_clv                NUMERIC(14,2),    -- average_order_value * purchase_frequency_monthly * estimated_lifespan_months
    calculated_at                TIMESTAMP NOT NULL DEFAULT now()
);
