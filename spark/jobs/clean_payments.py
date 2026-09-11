"""
clean_payments.py — Cleans raw `payments`, validates against cleaned
`orders`, writes processed Parquet.

Usage:
    python -m spark.jobs.clean_payments --batch-date 2026-09-06
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from spark.utils.cleaning import (
    anti_join_orphans,
    clip_negative_amount,
    deduplicate,
    drop_null_key,
    logger,
    read_raw_csv,
    validate_allowed_values,
    write_processed_parquet,
)
from spark.utils.schemas import PAYMENTS_SCHEMA
from spark.utils.spark_session import get_spark_session

ALLOWED_PAYMENT_STATUSES = ["success", "failed", "pending", "refunded"]


def clean_payments(spark, raw_path: str, clean_orders_path: str):
    orders = spark.read.parquet(clean_orders_path).select("order_id")

    df = read_raw_csv(spark, raw_path, PAYMENTS_SCHEMA).cache()
    raw_count = df.count()
    logger.info("[payments] read %s raw rows from %s", raw_count, raw_path)

    df = drop_null_key(df, "payment_id", "payments")
    df = deduplicate(df, ["payment_id"], "payments")
    df = clip_negative_amount(df, "amount")
    df = validate_allowed_values(df, "payment_status", ALLOWED_PAYMENT_STATUSES, "payments")
    # payments is one-to-one with orders and orders is not small at
    # `large` scale, so — same reasoning as clean_orders.py — this
    # join is not broadcast.
    df, _orphans = anti_join_orphans(df, orders, "order_id", "payments", use_broadcast=False)
    df = df.withColumn("_processed_at", F.current_timestamp())

    clean_count = df.count()
    logger.info("[payments] %s clean rows out of %s raw (%.1f%% retained)", clean_count, raw_count, 100 * clean_count / max(raw_count, 1))
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean raw payments data.")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--base-dir", default="data")
    args = parser.parse_args()

    spark = get_spark_session("clean_payments")
    try:
        bd, base = args.batch_date, args.base_dir
        df = clean_payments(
            spark,
            raw_path=f"{base}/raw/payments/batch_date={bd}/payments.csv",
            clean_orders_path=f"{base}/processed/orders/batch_date={bd}/",
        )
        write_processed_parquet(df, f"{base}/processed/payments/batch_date={bd}/", target_files=1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
