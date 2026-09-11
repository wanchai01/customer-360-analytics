"""
transform_events.py — Cleans raw `website_events` and derives two
transformed outputs using window functions (Section 9's "Window
Functions, Ranking, Aggregation" requirement):

  1. `events_clean`   — the cleaned, row-level event stream
  2. `session_summary`— one row per session: event count, session
                        duration, device/traffic_source, and whether
                        the session ended in a purchase — computed
                        with a window PARTITION BY session_id
  3. `daily_top_products` — the top-5 most-viewed products per day,
                        computed with a window PARTITION BY event_date
                        ORDER BY view_count DESC and RANK()

Usage:
    python -m spark.jobs.transform_events --batch-date 2026-09-06
"""
from __future__ import annotations

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from spark.utils.cleaning import (
    anti_join_orphans,
    deduplicate,
    drop_null_key,
    logger,
    read_raw_json,
    write_processed_parquet,
)
from spark.utils.schemas import WEBSITE_EVENTS_SCHEMA
from spark.utils.spark_session import get_spark_session

ALLOWED_EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout_start", "purchase", "search"]


def clean_events(spark, raw_path: str, clean_customers_path: str):
    customers = spark.read.parquet(clean_customers_path).select("customer_id")

    df = read_raw_json(spark, raw_path, WEBSITE_EVENTS_SCHEMA).cache()
    raw_count = df.count()
    logger.info("[website_events] read %s raw rows from %s", raw_count, raw_path)

    df = drop_null_key(df, "event_id", "website_events")
    df = deduplicate(df, ["event_id"], "website_events")
    df = df.withColumn(
        "event_type",
        F.when(F.col("event_type").isin(ALLOWED_EVENT_TYPES), F.col("event_type")).otherwise(F.lit(None)),
    )
    # customers can reach ~1M rows at `large` scale -> not broadcast,
    # same rule as every other fact-vs-customers join in this module.
    df, _orphans = anti_join_orphans(df, customers, "customer_id", "website_events", use_broadcast=False)
    df = df.withColumn("_processed_at", F.current_timestamp()).cache()

    clean_count = df.count()
    logger.info("[website_events] %s clean rows out of %s raw (%.1f%% retained)", clean_count, raw_count, 100 * clean_count / max(raw_count, 1))
    return df


def build_session_summary(events_clean):
    """
    One row per session_id, using a window function PARTITION BY
    session_id to compute session-level aggregates without a full
    groupBy shuffle-then-join back to row-level data — first/last
    event timestamp, event count, and "did this session convert" all
    fall out of the same window in one pass.
    """
    session_window = Window.partitionBy("session_id")

    with_session_stats = (
        events_clean
        .withColumn("session_event_count", F.count("event_id").over(session_window))
        .withColumn("session_start", F.min("event_timestamp").over(session_window))
        .withColumn("session_end", F.max("event_timestamp").over(session_window))
        .withColumn("session_has_purchase", F.max((F.col("event_type") == "purchase").cast("int")).over(session_window))
    )

    session_summary = (
        with_session_stats
        .groupBy("session_id", "customer_id", "device", "traffic_source")
        .agg(
            F.first("session_event_count").alias("event_count"),
            F.first("session_start").alias("session_start"),
            F.first("session_end").alias("session_end"),
            F.first("session_has_purchase").alias("converted"),
        )
        .withColumn("session_duration_seconds", F.col("session_end").cast("long") - F.col("session_start").cast("long"))
    )
    return session_summary


def build_daily_top_products(events_clean, top_n: int = 5):
    """
    Top-N most-viewed products per calendar day, using RANK() over a
    window PARTITION BY event_date ORDER BY view_count DESC — the
    canonical "top-N per group" pattern, more efficient than a
    groupBy + collect_list + Python-side sort because the ranking
    happens inside Spark's distributed execution instead of pulling
    data to the driver.
    """
    product_views = events_clean.filter(
        (F.col("event_type") == "product_view") & F.col("product_id").isNotNull()
    ).withColumn("event_date", F.to_date("event_timestamp"))

    daily_counts = (
        product_views
        .groupBy("event_date", "product_id")
        .agg(F.count("*").alias("view_count"))
    )

    rank_window = Window.partitionBy("event_date").orderBy(F.desc("view_count"))
    ranked = daily_counts.withColumn("rank", F.rank().over(rank_window))

    return ranked.filter(F.col("rank") <= top_n).orderBy("event_date", "rank")


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean and transform raw website_events data.")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--base-dir", default="data")
    args = parser.parse_args()

    spark = get_spark_session("transform_events")
    try:
        bd, base = args.batch_date, args.base_dir
        events_clean = clean_events(
            spark,
            raw_path=f"{base}/raw/website_events/batch_date={bd}/website_events.json",
            clean_customers_path=f"{base}/processed/customers/batch_date={bd}/",
        )
        session_summary = build_session_summary(events_clean)
        daily_top_products = build_daily_top_products(events_clean)

        write_processed_parquet(events_clean, f"{base}/processed/events/batch_date={bd}/", target_files=1)
        write_processed_parquet(session_summary, f"{base}/processed/session_summary/batch_date={bd}/", target_files=1)
        write_processed_parquet(daily_top_products, f"{base}/processed/daily_top_products/batch_date={bd}/", target_files=1)

        logger.info("[website_events] session_summary rows: %s", session_summary.count())
        logger.info("[website_events] daily_top_products rows: %s", daily_top_products.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
