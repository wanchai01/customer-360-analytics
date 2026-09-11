"""
clean_products.py — Cleans raw `products` and writes processed Parquet.

Cleaning steps:
  - Completeness : drop rows with a NULL product_id
  - Uniqueness   : drop duplicate rows on product_id
  - Accuracy     : null out rows where cost > selling_price (a
                   product that costs more than it sells for is a
                   data error at generation/ingestion, not a real
                   loss-leader scenario — real loss-leaders are rare
                   and would need a separate flag, not silent
                   inclusion in margin calculations)

Usage:
    python -m spark.jobs.clean_products --batch-date 2026-09-06
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from spark.utils.cleaning import (
    deduplicate,
    drop_null_key,
    logger,
    read_raw_csv,
    write_processed_parquet,
)
from spark.utils.schemas import PRODUCTS_SCHEMA
from spark.utils.spark_session import get_spark_session


def clean_products(spark, raw_path: str):
    df = read_raw_csv(spark, raw_path, PRODUCTS_SCHEMA).cache()
    raw_count = df.count()
    logger.info("[products] read %s raw rows from %s", raw_count, raw_path)

    df = drop_null_key(df, "product_id", "products")
    df = deduplicate(df, ["product_id"], "products")

    # Materialize the flag as its own column BEFORE nulling anything:
    # `invalid_margin` compares cost/selling_price, and nulling `cost`
    # via the first withColumn below would make the SAME lazy
    # expression re-evaluate as `NULL > selling_price` (= NULL, i.e.
    # falsy) on the second withColumn — silently leaving
    # selling_price un-nulled for exactly the rows this is supposed to
    # catch. Found by a test that checked both columns, not just cost.
    df = df.withColumn(
        "_invalid_margin",
        (F.col("cost").isNotNull()) & (F.col("selling_price").isNotNull()) & (F.col("cost") > F.col("selling_price")),
    )
    bad_margin = df.filter(F.col("_invalid_margin")).count()
    if bad_margin:
        logger.info("[products] %s rows have cost > selling_price (nulling both, margin uncomputable)", bad_margin)
    df = df.withColumn("cost", F.when(F.col("_invalid_margin"), F.lit(None)).otherwise(F.col("cost")))
    df = df.withColumn("selling_price", F.when(F.col("_invalid_margin"), F.lit(None)).otherwise(F.col("selling_price")))
    df = df.drop("_invalid_margin")

    df = df.withColumn("_processed_at", F.current_timestamp())
    clean_count = df.count()
    logger.info("[products] %s clean rows out of %s raw (%.1f%% retained)", clean_count, raw_count, 100 * clean_count / max(raw_count, 1))
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean raw products data.")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--base-dir", default="data")
    args = parser.parse_args()

    spark = get_spark_session("clean_products")
    try:
        raw_path = f"{args.base_dir}/raw/products/batch_date={args.batch_date}/products.csv"
        out_path = f"{args.base_dir}/processed/products/batch_date={args.batch_date}/"
        df = clean_products(spark, raw_path)
        write_processed_parquet(df, out_path, target_files=1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
