-- =========================================================
-- 01_gold_tables.sql — Analytics (Gold) layer.
--
-- These tables are populated by PySpark jobs in Phase 8-9
-- (build_customer_360.py, build_rfm.py) reading from the
-- `warehouse` star schema — they are read-optimized, wide,
-- denormalized tables meant for Power BI and ad-hoc SQL, not
-- for further joining logic.
-- =========================================================

SET search_path TO analytics;

-- ---------------------------------------------------------
-- customer_360 — one row per customer (Section 12)
-- ---------------------------------------------------------
DROP TABLE IF EXISTS customer_360 CASCADE;
CREATE TABLE customer_360 (
    customer_id                TEXT PRIMARY KEY,
    customer_name               TEXT,
    age                         INTEGER,
    gender                      TEXT,
    location                    TEXT,               -- "City, Province"
    registration_date           DATE,
    total_orders                INTEGER,
    completed_orders            INTEGER,
    cancelled_orders            INTEGER,
    total_spend                 NUMERIC(14,2),
    average_order_value         NUMERIC(12,2),
    first_purchase_date         DATE,
    last_purchase_date          DATE,
    days_since_last_purchase    INTEGER,
    purchase_frequency          NUMERIC(10,4),       -- orders per month active
    favorite_category           TEXT,
    favorite_product            TEXT,
    website_sessions             INTEGER,
    website_events               INTEGER,
    support_tickets              INTEGER,
    resolved_tickets             INTEGER,
    average_resolution_time      NUMERIC(10,2),
    campaign_interactions        INTEGER,
    customer_lifetime_value      NUMERIC(14,2),
    rfm_score                    TEXT,
    customer_segment             TEXT,
    _built_at                    TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_c360_segment  ON customer_360 (customer_segment);
CREATE INDEX idx_c360_last_pur ON customer_360 (last_purchase_date);

-- ---------------------------------------------------------
-- customer_rfm — Recency/Frequency/Monetary detail (Section 13)
-- ---------------------------------------------------------
DROP TABLE IF EXISTS customer_rfm;
CREATE TABLE customer_rfm (
    customer_id     TEXT PRIMARY KEY,
    recency_days    INTEGER,          -- days since last purchase (as of run date)
    frequency       INTEGER,          -- count of completed orders
    monetary        NUMERIC(14,2),    -- total spend
    r_score         SMALLINT,         -- 1 (worst) - 5 (best)
    f_score         SMALLINT,
    m_score         SMALLINT,
    rfm_score       TEXT,             -- concatenation, e.g. '454'
    rfm_sum         SMALLINT,         -- r_score + f_score + m_score, for quick sorting
    calculated_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------
-- customer_segments — segment assignment + the RFM-sum rule
-- that produced it (kept separate from customer_rfm so the
-- segmentation *logic* can change/version without recomputing
-- raw RFM numbers).
-- ---------------------------------------------------------
DROP TABLE IF EXISTS customer_segments;
CREATE TABLE customer_segments (
    customer_id         TEXT PRIMARY KEY,
    customer_segment    TEXT NOT NULL,
    segment_logic_version TEXT NOT NULL DEFAULT 'v1',
    assigned_at         TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_segments_segment ON customer_segments (customer_segment);

-- ---------------------------------------------------------
-- customer_monthly_metrics — cohort/retention time series
-- (Section 15: Monthly Active, New, Returning, Retention,
-- Churn, Repeat Purchase Rate)
-- ---------------------------------------------------------
DROP TABLE IF EXISTS customer_monthly_metrics;
CREATE TABLE customer_monthly_metrics (
    metric_month            DATE PRIMARY KEY,   -- first day of month
    active_customers        INTEGER,
    new_customers           INTEGER,
    returning_customers     INTEGER,
    retention_rate          NUMERIC(6,4),
    churn_rate              NUMERIC(6,4),
    repeat_purchase_rate    NUMERIC(6,4),
    total_revenue           NUMERIC(16,2),
    _built_at               TIMESTAMP NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------
-- customer_cohort — cohort retention matrix (Section 15)
-- ---------------------------------------------------------
DROP TABLE IF EXISTS customer_cohort;
CREATE TABLE customer_cohort (
    cohort_month        DATE NOT NULL,   -- month the customer first purchased
    period_number       INTEGER NOT NULL, -- 0, 1, 2, 3 ... months since cohort_month
    active_customers    INTEGER NOT NULL,
    cohort_size         INTEGER NOT NULL,
    retention_rate      NUMERIC(6,4),
    _built_at           TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (cohort_month, period_number)
);

-- ---------------------------------------------------------
-- data_quality_results (Section 10)
-- ---------------------------------------------------------
DROP TABLE IF EXISTS data_quality_results;
CREATE TABLE data_quality_results (
    check_id            SERIAL PRIMARY KEY,
    dataset              TEXT NOT NULL,
    check_name           TEXT NOT NULL,
    total_records         INTEGER NOT NULL,
    failed_records        INTEGER NOT NULL,
    pass_rate             NUMERIC(6,4) GENERATED ALWAYS AS (
                              CASE WHEN total_records > 0
                                   THEN round(((total_records - failed_records)::numeric / total_records), 4)
                                   ELSE NULL END
                          ) STORED,
    status                TEXT NOT NULL,        -- 'PASS' / 'FAIL'
    execution_time        NUMERIC(10,3),        -- seconds
    batch_date            DATE NOT NULL,
    checked_at            TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_dq_dataset_batch ON data_quality_results (dataset, batch_date);
CREATE INDEX idx_dq_status        ON data_quality_results (status);
