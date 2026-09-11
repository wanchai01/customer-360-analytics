-- =========================================================
-- 02_facts.sql — Star Schema fact tables.
--
-- Grain of each fact:
--   fact_orders               : one row per order
--   fact_order_items          : one row per order line item
--   fact_payments             : one row per payment
--   fact_website_events       : one row per clickstream event
--   fact_support_tickets      : one row per support ticket
--   fact_campaign_interactions: one row per campaign touchpoint
--     (not in the spec's minimum fact list, but a natural
--      bridge for dim_campaign — needed to answer "campaign
--      performance" business questions later.)
--
-- Performance notes:
--   - `fact_website_events` is, by design, the largest table
--     (Section 5: 2M in dev, 50M+ at large scale) and is the
--     one place we apply native PostgreSQL declarative RANGE
--     partitioning on event_timestamp. This lets the planner
--     use partition pruning — a query filtered to one month
--     only scans that month's partition, not the whole table
--     — and lets old partitions be dropped/archived cheaply
--     instead of a slow DELETE. Every other fact table stays
--     as a single table; at this project's data volumes,
--     partitioning them would add operational complexity
--     (constraint management, per-partition indexes) without
--     a matching performance benefit.
-- =========================================================

SET search_path TO warehouse;

-- ---------------------------------------------------------
-- fact_orders
-- ---------------------------------------------------------
DROP TABLE IF EXISTS fact_orders CASCADE;
CREATE TABLE fact_orders (
    order_key       SERIAL PRIMARY KEY,
    order_id        TEXT NOT NULL UNIQUE,
    customer_key    INTEGER NOT NULL REFERENCES dim_customer (customer_key),
    order_date_key  INTEGER REFERENCES dim_date (date_key),
    order_date      TIMESTAMP NOT NULL,
    order_status    TEXT,
    payment_method  TEXT,
    total_amount    NUMERIC(14,2),
    _loaded_at      TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_fact_orders_customer ON fact_orders (customer_key);
CREATE INDEX idx_fact_orders_date     ON fact_orders (order_date_key);
CREATE INDEX idx_fact_orders_status   ON fact_orders (order_status);

-- ---------------------------------------------------------
-- fact_order_items
-- ---------------------------------------------------------
DROP TABLE IF EXISTS fact_order_items CASCADE;
CREATE TABLE fact_order_items (
    order_item_key  SERIAL PRIMARY KEY,
    order_item_id   TEXT NOT NULL UNIQUE,
    order_key       INTEGER NOT NULL REFERENCES fact_orders (order_key),
    product_key     INTEGER REFERENCES dim_product (product_key),
    quantity        INTEGER,
    unit_price      NUMERIC(12,2),
    discount        NUMERIC(5,4),
    line_amount     NUMERIC(14,2) GENERATED ALWAYS AS (
                        round((quantity * unit_price * (1 - discount))::numeric, 2)
                    ) STORED,
    _loaded_at      TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_fact_items_order   ON fact_order_items (order_key);
CREATE INDEX idx_fact_items_product ON fact_order_items (product_key);

-- ---------------------------------------------------------
-- fact_payments
-- ---------------------------------------------------------
DROP TABLE IF EXISTS fact_payments CASCADE;
CREATE TABLE fact_payments (
    payment_key         SERIAL PRIMARY KEY,
    payment_id          TEXT NOT NULL UNIQUE,
    order_key           INTEGER NOT NULL REFERENCES fact_orders (order_key),
    payment_date_key    INTEGER REFERENCES dim_date (date_key),
    payment_date        TIMESTAMP,
    payment_method      TEXT,
    payment_status      TEXT,
    amount              NUMERIC(14,2),
    _loaded_at          TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_fact_payments_order  ON fact_payments (order_key);
CREATE INDEX idx_fact_payments_status ON fact_payments (payment_status);

-- ---------------------------------------------------------
-- fact_support_tickets
-- ---------------------------------------------------------
DROP TABLE IF EXISTS fact_support_tickets CASCADE;
CREATE TABLE fact_support_tickets (
    ticket_key          SERIAL PRIMARY KEY,
    ticket_id           TEXT NOT NULL UNIQUE,
    customer_key        INTEGER REFERENCES dim_customer (customer_key),
    created_date_key    INTEGER REFERENCES dim_date (date_key),
    created_at          TIMESTAMP,
    issue_type          TEXT,
    priority            TEXT,
    status              TEXT,
    resolution_time     NUMERIC(10,2),
    _loaded_at          TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_fact_tickets_customer ON fact_support_tickets (customer_key);
CREATE INDEX idx_fact_tickets_status   ON fact_support_tickets (status);

-- ---------------------------------------------------------
-- fact_campaign_interactions
-- ---------------------------------------------------------
DROP TABLE IF EXISTS fact_campaign_interactions CASCADE;
CREATE TABLE fact_campaign_interactions (
    interaction_key         SERIAL PRIMARY KEY,
    interaction_id          TEXT NOT NULL UNIQUE,
    campaign_key            INTEGER REFERENCES dim_campaign (campaign_key),
    customer_key            INTEGER REFERENCES dim_customer (customer_key),
    interaction_type        TEXT,
    interaction_timestamp   TIMESTAMP,
    _loaded_at              TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_fact_interactions_campaign ON fact_campaign_interactions (campaign_key);
CREATE INDEX idx_fact_interactions_customer ON fact_campaign_interactions (customer_key);

-- ---------------------------------------------------------
-- fact_website_events — partitioned by RANGE on event_timestamp
-- ---------------------------------------------------------
DROP TABLE IF EXISTS fact_website_events CASCADE;
CREATE TABLE fact_website_events (
    event_key           BIGSERIAL,
    event_id            TEXT NOT NULL,
    customer_key        INTEGER REFERENCES dim_customer (customer_key),
    session_id          TEXT,
    event_date_key      INTEGER REFERENCES dim_date (date_key),
    event_timestamp     TIMESTAMP NOT NULL,
    event_type          TEXT,
    page                TEXT,
    product_key         INTEGER REFERENCES dim_product (product_key),
    device_key          INTEGER REFERENCES dim_device (device_key),
    traffic_source      TEXT,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (event_key, event_timestamp)
) PARTITION BY RANGE (event_timestamp);

-- Yearly partitions covering the generator's date range (2019-01-01
-- to 2026-09-06, see data_generator/config.py), plus one extra year
-- of headroom and a DEFAULT catch-all so an out-of-range timestamp
-- fails safe into a queryable partition instead of an insert error.
CREATE TABLE fact_website_events_y2019 PARTITION OF fact_website_events
    FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE TABLE fact_website_events_y2020 PARTITION OF fact_website_events
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE fact_website_events_y2021 PARTITION OF fact_website_events
    FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE fact_website_events_y2022 PARTITION OF fact_website_events
    FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE fact_website_events_y2023 PARTITION OF fact_website_events
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE fact_website_events_y2024 PARTITION OF fact_website_events
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE fact_website_events_y2025 PARTITION OF fact_website_events
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE fact_website_events_y2026 PARTITION OF fact_website_events
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE fact_website_events_default PARTITION OF fact_website_events DEFAULT;

CREATE INDEX idx_fact_events_customer ON fact_website_events (customer_key);
CREATE INDEX idx_fact_events_session  ON fact_website_events (session_id);
CREATE INDEX idx_fact_events_type     ON fact_website_events (event_type);
