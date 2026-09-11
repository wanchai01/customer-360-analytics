-- =========================================================
-- 03_populate_dim_date.sql
-- Populates dim_date for every day from 2019-01-01 through
-- 2027-12-31 using generate_series — covers the full range
-- the data generator can ever produce (2019-01-01 to the
-- pipeline's "as_of" date) plus headroom, with no gaps.
--
-- Uses INSERT ... ON CONFLICT DO NOTHING rather than TRUNCATE:
-- once fact tables hold foreign keys into dim_date (even with
-- zero rows), a plain TRUNCATE on dim_date fails with
-- "cannot truncate a table referenced in a foreign key
-- constraint" — discovered while testing this script against
-- a live database after the fact tables were created. Since
-- calendar dimension rows never change after being written,
-- an idempotent upsert is both simpler and safer than
-- TRUNCATE ... CASCADE, which would also wipe every fact table.
-- =========================================================

SET search_path TO warehouse;

INSERT INTO dim_date (
    date_key, full_date, year, quarter, month, month_name,
    day, day_of_week, day_name, week_of_year, is_weekend
)
SELECT
    (to_char(d, 'YYYYMMDD'))::INTEGER                    AS date_key,
    d::DATE                                               AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT                        AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT                     AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT                       AS month,
    to_char(d, 'Month')                                   AS month_name,
    EXTRACT(DAY FROM d)::SMALLINT                         AS day,
    EXTRACT(DOW FROM d)::SMALLINT                         AS day_of_week,
    to_char(d, 'Day')                                     AS day_name,
    EXTRACT(WEEK FROM d)::SMALLINT                        AS week_of_year,
    EXTRACT(DOW FROM d) IN (0, 6)                         AS is_weekend
FROM generate_series(
    '2019-01-01'::DATE,
    '2027-12-31'::DATE,
    INTERVAL '1 day'
) AS d
ON CONFLICT (date_key) DO NOTHING;
