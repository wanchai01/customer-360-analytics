"""
clean_customers.py — Cleans raw `customers` and writes processed Parquet.

Cleaning steps (Section 9 of the spec):
  - Completeness : drop rows with a NULL customer_id
  - Uniqueness   : drop duplicate rows on customer_id
  - Validity     : null out malformed emails, dates outside a
                   plausible range, and customer_status values
                   outside the known set

Usage:
    python -m spark.jobs.clean_customers --batch-date 2026-09-06
"""
from __future__ import annotations

import argparse

from datetime import date

from pyspark.sql import functions as F

from spark.utils.cleaning import (
    deduplicate,
    drop_null_key,
    filter_valid_date_range,
    flag_invalid_email,
    logger,
    read_raw_csv,
    validate_allowed_values,
    write_processed_parquet,
)
from spark.utils.schemas import CUSTOMERS_SCHEMA
from spark.utils.spark_session import get_spark_session

ALLOWED_STATUSES = ["active", "inactive", "churned"]


def clean_customers(spark, raw_path: str, target_files: int = 1):
    df = read_raw_csv(spark, raw_path, CUSTOMERS_SCHEMA)

    # `cache()` because this DataFrame is scanned multiple times below
    # (each cleaning step's before/after .count() plus the final
    # write) — without caching, Spark would re-read and re-parse the
    # raw CSV from scratch for every single one of those actions.
    df = df.cache()
    raw_count = df.count()
    logger.info("[customers] read %s raw rows from %s", raw_count, raw_path)

    df = drop_null_key(df, "customer_id", "customers")
    df = deduplicate(df, ["customer_id"], "customers")
    df = flag_invalid_email(df, "email")
    # date_of_birth needs its own plausible range (a customer born
    # 1930-2015), distinct from the transactional-date default range
    # used for registration_date/order_date/etc — reusing one global
    # window for both would wrongly null out every legitimate
    # birthdate, which is exactly what happened the first time this
    # job ran against real data (found via the row-count sanity check
    # below, not guessed).
    df = filter_valid_date_range(df, "date_of_birth", "customers", min_date=date(1930, 1, 1), max_date=date(2015, 1, 1))
    df = filter_valid_date_range(df, "registration_date", "customers")
    df = validate_allowed_values(df, "customer_status", ALLOWED_STATUSES, "customers")

    df = df.withColumn("_processed_at", F.current_timestamp())

    clean_count = df.count()
    logger.info("[customers] %s clean rows out of %s raw (%.1f%% retained)", clean_count, raw_count, 100 * clean_count / max(raw_count, 1))
    return df, target_files


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean raw customers data.")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--base-dir", default="data")
    parser.add_argument("--target-files", type=int, default=1)
    args = parser.parse_args()

    spark = get_spark_session("clean_customers")
    try:
        raw_path = f"{args.base_dir}/raw/customers/batch_date={args.batch_date}/customers.csv"
        out_path = f"{args.base_dir}/processed/customers/batch_date={args.batch_date}/"
        df, target_files = clean_customers(spark, raw_path)
        write_processed_parquet(df, out_path, target_files=target_files or args.target_files)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
