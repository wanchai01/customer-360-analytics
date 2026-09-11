-- =========================================================
-- 01_dimensions.sql — Star Schema dimension tables.
--
-- Design notes:
--   - Every dimension has a surrogate integer key (e.g.
--     customer_key) used by fact tables, plus the natural
--     business key (e.g. customer_id) kept UNIQUE. This is
--     standard warehouse practice: surrogate keys are stable
--     even if a source system ID format ever changes, and
--     they're smaller/faster to join on than TEXT ids.
--   - dim_customer is implemented as Type-1 (overwrite on
--     change) for this portfolio project — the `_loaded_at`/
--     `_updated_at` columns are kept so it can be upgraded to
--     Type-2 (row versioning with effective_start/end dates)
--     later without a schema rewrite. That trade-off is
--     intentional and explained in docs/data_model.md.
--   - dim_location is a small snowflaked dimension referenced
--     by dim_customer, avoiding repeating city/province/country
--     text on every customer row and giving a single place to
--     fix geography data.
-- =========================================================

SET search_path TO warehouse;

-- ---------------------------------------------------------
-- dim_date — standard calendar dimension, pre-populated by
-- 03_populate_dim_date.sql via generate_series (no gaps,
-- covers every day the pipeline could ever reference).
-- ---------------------------------------------------------
DROP TABLE IF EXISTS dim_date CASCADE;
CREATE TABLE dim_date (
    date_key        INTEGER PRIMARY KEY,         -- YYYYMMDD, e.g. 20260906
    full_date       DATE NOT NULL UNIQUE,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      TEXT NOT NULL,
    day             SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,            -- 0=Sunday .. 6=Saturday
    day_name        TEXT NOT NULL,
    week_of_year    SMALLINT NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

-- ---------------------------------------------------------
-- dim_location
-- ---------------------------------------------------------
DROP TABLE IF EXISTS dim_location CASCADE;
CREATE TABLE dim_location (
    location_key    SERIAL PRIMARY KEY,
    city            TEXT NOT NULL,
    province        TEXT NOT NULL,
    country         TEXT NOT NULL,
    UNIQUE (city, province, country)
);

-- ---------------------------------------------------------
-- dim_device — tiny, mostly-static dimension
-- ---------------------------------------------------------
DROP TABLE IF EXISTS dim_device CASCADE;
CREATE TABLE dim_device (
    device_key      SERIAL PRIMARY KEY,
    device_name     TEXT NOT NULL UNIQUE
);

-- ---------------------------------------------------------
-- dim_customer
-- ---------------------------------------------------------
DROP TABLE IF EXISTS dim_customer CASCADE;
CREATE TABLE dim_customer (
    customer_key        SERIAL PRIMARY KEY,
    customer_id         TEXT NOT NULL UNIQUE,      -- natural/business key
    first_name          TEXT,
    last_name            TEXT,
    full_name           TEXT GENERATED ALWAYS AS (trim(coalesce(first_name,'') || ' ' || coalesce(last_name,''))) STORED,
    email               TEXT,
    phone               TEXT,
    gender              TEXT,
    date_of_birth       DATE,
    location_key        INTEGER REFERENCES dim_location (location_key),
    registration_date   DATE,
    customer_status     TEXT,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT now(),
    _updated_at         TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_dim_customer_status ON dim_customer (customer_status);
CREATE INDEX idx_dim_customer_loc    ON dim_customer (location_key);

-- ---------------------------------------------------------
-- dim_product
-- ---------------------------------------------------------
DROP TABLE IF EXISTS dim_product CASCADE;
CREATE TABLE dim_product (
    product_key     SERIAL PRIMARY KEY,
    product_id      TEXT NOT NULL UNIQUE,
    product_name    TEXT,
    category        TEXT,
    subcategory     TEXT,
    brand           TEXT,
    cost            NUMERIC(12,2),
    selling_price   NUMERIC(12,2),
    margin_pct      NUMERIC(6,4) GENERATED ALWAYS AS (
                        CASE WHEN selling_price > 0
                             THEN round(((selling_price - cost) / selling_price)::numeric, 4)
                             ELSE NULL END
                    ) STORED,
    _loaded_at      TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX idx_dim_product_category ON dim_product (category, subcategory);

-- ---------------------------------------------------------
-- dim_campaign
-- ---------------------------------------------------------
DROP TABLE IF EXISTS dim_campaign CASCADE;
CREATE TABLE dim_campaign (
    campaign_key    SERIAL PRIMARY KEY,
    campaign_id     TEXT NOT NULL UNIQUE,
    campaign_name   TEXT,
    channel         TEXT,
    start_date      DATE,
    end_date        DATE,
    budget          NUMERIC(14,2),
    _loaded_at      TIMESTAMP NOT NULL DEFAULT now()
);
