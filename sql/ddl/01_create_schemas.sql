-- =========================================================
-- 01_create_schemas.sql
-- Creates the three logical schemas inside the analytics
-- database, mirroring the medallion-style layers used in the
-- data lake (raw -> processed -> curated):
--
--   staging   : one table per source dataset, loosely typed,
--               loaded directly from the processed (Parquet)
--               data lake layer. Allowed to contain the
--               occasional bad row — that's what Phase 6
--               Data Quality checks are for.
--   warehouse : the Star Schema (fact_* / dim_*) used for
--               reliable, query-friendly analytics.
--   analytics : Gold-layer / derived tables — customer_360,
--               RFM, CLV, segments, and the data quality
--               results log.
-- =========================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS analytics;

COMMENT ON SCHEMA staging   IS 'Raw-ish, loosely-typed landing tables loaded from the data lake processed layer.';
COMMENT ON SCHEMA warehouse IS 'Star Schema analytics warehouse: fact_* and dim_* tables.';
COMMENT ON SCHEMA analytics IS 'Gold layer: customer_360, RFM, CLV, segments, data quality results.';
