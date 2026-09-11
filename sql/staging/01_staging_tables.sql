-- =========================================================
-- 01_staging_tables.sql
-- One staging table per raw source dataset (Section 4 of the
-- spec). Deliberately permissive:
--   - Business-key columns are TEXT, not enforced UNIQUE/NOT
--     NULL here, because the raw layer is allowed to contain
--     nulls/duplicates/orphans — that is exactly what the
--     Phase 6 Data Quality framework is built to detect
--     before promoting data into `warehouse`/`analytics`.
--   - No foreign keys between staging tables, for the same
--     reason (referential integrity is a DQ *check*, not a
--     DB *constraint*, at this layer).
--   - Every table carries lineage columns (_batch_date,
--     _loaded_at, _source_file) so any row can be traced back
--     to the pipeline run that produced it.
-- =========================================================

SET search_path TO staging;

DROP TABLE IF EXISTS stg_customers;
CREATE TABLE stg_customers (
    customer_id         TEXT,
    first_name          TEXT,
    last_name           TEXT,
    email               TEXT,
    phone               TEXT,
    gender              TEXT,
    date_of_birth       DATE,
    city                TEXT,
    province            TEXT,
    country             TEXT,
    registration_date   DATE,
    customer_status     TEXT,
    _batch_date         DATE        NOT NULL,
    _loaded_at          TIMESTAMP   NOT NULL DEFAULT now(),
    _source_file        TEXT
);
CREATE INDEX idx_stg_customers_batch ON stg_customers (_batch_date);
CREATE INDEX idx_stg_customers_id    ON stg_customers (customer_id);

DROP TABLE IF EXISTS stg_products;
CREATE TABLE stg_products (
    product_id      TEXT,
    product_name    TEXT,
    category        TEXT,
    subcategory     TEXT,
    brand           TEXT,
    cost            NUMERIC(12,2),
    selling_price   NUMERIC(12,2),
    _batch_date     DATE        NOT NULL,
    _loaded_at      TIMESTAMP   NOT NULL DEFAULT now(),
    _source_file    TEXT
);
CREATE INDEX idx_stg_products_batch ON stg_products (_batch_date);
CREATE INDEX idx_stg_products_id    ON stg_products (product_id);

DROP TABLE IF EXISTS stg_orders;
CREATE TABLE stg_orders (
    order_id        TEXT,
    customer_id     TEXT,
    order_date      TIMESTAMP,
    order_status    TEXT,
    payment_method  TEXT,
    total_amount    NUMERIC(14,2),
    _batch_date     DATE        NOT NULL,
    _loaded_at      TIMESTAMP   NOT NULL DEFAULT now(),
    _source_file    TEXT
);
CREATE INDEX idx_stg_orders_batch    ON stg_orders (_batch_date);
CREATE INDEX idx_stg_orders_id       ON stg_orders (order_id);
CREATE INDEX idx_stg_orders_cust     ON stg_orders (customer_id);
CREATE INDEX idx_stg_orders_date     ON stg_orders (order_date);

DROP TABLE IF EXISTS stg_order_items;
CREATE TABLE stg_order_items (
    order_item_id   TEXT,
    order_id        TEXT,
    product_id      TEXT,
    quantity        INTEGER,
    unit_price      NUMERIC(12,2),
    discount        NUMERIC(5,4),
    _batch_date     DATE        NOT NULL,
    _loaded_at      TIMESTAMP   NOT NULL DEFAULT now(),
    _source_file    TEXT
);
CREATE INDEX idx_stg_items_batch  ON stg_order_items (_batch_date);
CREATE INDEX idx_stg_items_order  ON stg_order_items (order_id);
CREATE INDEX idx_stg_items_prod   ON stg_order_items (product_id);

DROP TABLE IF EXISTS stg_payments;
CREATE TABLE stg_payments (
    payment_id      TEXT,
    order_id        TEXT,
    payment_date    TIMESTAMP,
    payment_method  TEXT,
    payment_status  TEXT,
    amount          NUMERIC(14,2),
    _batch_date     DATE        NOT NULL,
    _loaded_at      TIMESTAMP   NOT NULL DEFAULT now(),
    _source_file    TEXT
);
CREATE INDEX idx_stg_payments_batch ON stg_payments (_batch_date);
CREATE INDEX idx_stg_payments_order ON stg_payments (order_id);

DROP TABLE IF EXISTS stg_website_events;
CREATE TABLE stg_website_events (
    event_id            TEXT,
    customer_id         TEXT,
    session_id          TEXT,
    event_timestamp     TIMESTAMP,
    event_type          TEXT,
    page                TEXT,
    product_id          TEXT,
    device              TEXT,
    traffic_source      TEXT,
    _batch_date         DATE        NOT NULL,
    _loaded_at          TIMESTAMP   NOT NULL DEFAULT now(),
    _source_file        TEXT
);
CREATE INDEX idx_stg_events_batch ON stg_website_events (_batch_date);
CREATE INDEX idx_stg_events_cust  ON stg_website_events (customer_id);
CREATE INDEX idx_stg_events_sess  ON stg_website_events (session_id);
CREATE INDEX idx_stg_events_ts    ON stg_website_events (event_timestamp);

DROP TABLE IF EXISTS stg_customer_support;
CREATE TABLE stg_customer_support (
    ticket_id           TEXT,
    customer_id         TEXT,
    created_at          TIMESTAMP,
    issue_type          TEXT,
    priority            TEXT,
    status              TEXT,
    resolution_time     NUMERIC(10,2),
    _batch_date         DATE        NOT NULL,
    _loaded_at          TIMESTAMP   NOT NULL DEFAULT now(),
    _source_file        TEXT
);
CREATE INDEX idx_stg_support_batch ON stg_customer_support (_batch_date);
CREATE INDEX idx_stg_support_cust  ON stg_customer_support (customer_id);

DROP TABLE IF EXISTS stg_marketing_campaigns;
CREATE TABLE stg_marketing_campaigns (
    campaign_id     TEXT,
    campaign_name   TEXT,
    channel         TEXT,
    start_date      DATE,
    end_date        DATE,
    budget          NUMERIC(14,2),
    _batch_date     DATE        NOT NULL,
    _loaded_at      TIMESTAMP   NOT NULL DEFAULT now(),
    _source_file    TEXT
);
CREATE INDEX idx_stg_campaigns_batch ON stg_marketing_campaigns (_batch_date);
CREATE INDEX idx_stg_campaigns_id    ON stg_marketing_campaigns (campaign_id);

DROP TABLE IF EXISTS stg_campaign_interactions;
CREATE TABLE stg_campaign_interactions (
    interaction_id          TEXT,
    campaign_id             TEXT,
    customer_id             TEXT,
    interaction_type        TEXT,
    interaction_timestamp   TIMESTAMP,
    _batch_date             DATE        NOT NULL,
    _loaded_at              TIMESTAMP   NOT NULL DEFAULT now(),
    _source_file            TEXT
);
CREATE INDEX idx_stg_interactions_batch ON stg_campaign_interactions (_batch_date);
CREATE INDEX idx_stg_interactions_camp  ON stg_campaign_interactions (campaign_id);
CREATE INDEX idx_stg_interactions_cust  ON stg_campaign_interactions (customer_id);
