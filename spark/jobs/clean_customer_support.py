"""
clean_customer_support.py — Cleans raw `customer_support` tickets,
validates against cleaned `customers`, writes processed Parquet.

Usage:
    python -m spark.jobs.clean_customer_support --batch-date 2026-09-06
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from spark.utils.cleaning import (
    anti_join_orphans,
    deduplicate,
    drop_null_key,
    logger,
    read_raw_csv,
    validate_allowed_values,
    write_processed_parquet,
)
from spark.utils.schemas import CUSTOMER_SUPPORT_SCHEMA
from spark.utils.spark_session import get_spark_session

ALLOWED_TICKET_STATUSES = ["open", "in_progress", "resolved", "closed"]


def clean_customer_support(spark, raw_path: str, clean_customers_path: str):
    # products stays small at every scale (Section 5), but so does the
    # *distinct customer count referenced by tickets* relative to the
    # ticket volume itself — still, customers can reach ~1M rows at
    # `large` scale, so this join is not broadcast, matching the same
    # rule applied to orders/payments.
    customers = spark.read.parquet(clean_customers_path).select("customer_id")

    df = read_raw_csv(spark, raw_path, CUSTOMER_SUPPORT_SCHEMA).cache()
    raw_count = df.count()
    logger.info("[customer_support] read %s raw rows from %s", raw_count, raw_path)

    df = drop_null_key(df, "ticket_id", "customer_support")
    df = deduplicate(df, ["ticket_id"], "customer_support")
    df = validate_allowed_values(df, "status", ALLOWED_TICKET_STATUSES, "customer_support")
    df, _orphans = anti_join_orphans(df, customers, "customer_id", "customer_support", use_broadcast=False)
    df = df.withColumn("_processed_at", F.current_timestamp())

    clean_count = df.count()
    logger.info("[customer_support] %s clean rows out of %s raw (%.1f%% retained)", clean_count, raw_count, 100 * clean_count / max(raw_count, 1))
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean raw customer_support data.")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--base-dir", default="data")
    args = parser.parse_args()

    spark = get_spark_session("clean_customer_support")
    try:
        bd, base = args.batch_date, args.base_dir
        df = clean_customer_support(
            spark,
            raw_path=f"{base}/raw/customer_support/batch_date={bd}/customer_support.csv",
            clean_customers_path=f"{base}/processed/customers/batch_date={bd}/",
        )
        write_processed_parquet(df, f"{base}/processed/customer_support/batch_date={bd}/", target_files=1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
