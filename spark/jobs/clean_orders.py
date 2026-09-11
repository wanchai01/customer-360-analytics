"""
clean_orders.py — Cleans raw `orders` and `order_items`, validates
referential integrity against already-cleaned `customers`/`products`,
and writes both to processed Parquet.

Cleaning steps:
  - Completeness      : drop rows with a NULL order_id / order_item_id
  - Uniqueness        : drop duplicate rows on the business key
  - Validity          : null out negative total_amount / unit_price,
                        null out order_status values outside the
                        known set
  - Referential        : drop orders whose customer_id doesn't exist
    integrity            in cleaned customers (NOT broadcast — at
                          `large` scale customers is ~1M rows, too
                          big to ship to every executor cheaply);
                          drop order_items whose order_id doesn't
                          exist in cleaned orders (NOT broadcast, same
                          reasoning) or whose product_id doesn't exist
                          in cleaned products (broadcast — products
                          stays small even at `large` scale)

This job depends on clean_customers.py and clean_products.py having
already run for the same batch_date (it reads their processed Parquet
output as the source of truth for "does this ID exist").

Usage:
    python -m spark.jobs.clean_orders --batch-date 2026-09-06
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
from spark.utils.schemas import ORDER_ITEMS_SCHEMA, ORDERS_SCHEMA
from spark.utils.spark_session import get_spark_session

ALLOWED_ORDER_STATUSES = ["completed", "cancelled", "pending", "refunded"]


def clean_orders(spark, raw_orders_path: str, raw_items_path: str, clean_customers_path: str, clean_products_path: str):
    customers = spark.read.parquet(clean_customers_path).select("customer_id").cache()
    products = spark.read.parquet(clean_products_path).select("product_id").cache()

    # ---- orders ----
    orders = read_raw_csv(spark, raw_orders_path, ORDERS_SCHEMA).cache()
    raw_order_count = orders.count()
    logger.info("[orders] read %s raw rows from %s", raw_order_count, raw_orders_path)

    orders = drop_null_key(orders, "order_id", "orders")
    orders = deduplicate(orders, ["order_id"], "orders")
    orders = clip_negative_amount(orders, "total_amount")
    orders = validate_allowed_values(orders, "order_status", ALLOWED_ORDER_STATUSES, "orders")
    orders, _order_orphans = anti_join_orphans(orders, customers, "customer_id", "orders", use_broadcast=False)
    orders = orders.withColumn("_processed_at", F.current_timestamp())
    orders = orders.cache()

    clean_order_count = orders.count()
    logger.info("[orders] %s clean rows out of %s raw (%.1f%% retained)", clean_order_count, raw_order_count, 100 * clean_order_count / max(raw_order_count, 1))

    # ---- order_items ----
    items = read_raw_csv(spark, raw_items_path, ORDER_ITEMS_SCHEMA).cache()
    raw_item_count = items.count()
    logger.info("[order_items] read %s raw rows from %s", raw_item_count, raw_items_path)

    items = drop_null_key(items, "order_item_id", "order_items")
    items = deduplicate(items, ["order_item_id"], "order_items")
    items = clip_negative_amount(items, "unit_price")
    items, _item_order_orphans = anti_join_orphans(items, orders.select("order_id"), "order_id", "order_items", use_broadcast=False)
    items, _item_product_orphans = anti_join_orphans(items, products, "product_id", "order_items", use_broadcast=True)
    items = items.withColumn("_processed_at", F.current_timestamp())

    clean_item_count = items.count()
    logger.info("[order_items] %s clean rows out of %s raw (%.1f%% retained)", clean_item_count, raw_item_count, 100 * clean_item_count / max(raw_item_count, 1))

    return orders, items


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean raw orders and order_items data.")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--base-dir", default="data")
    args = parser.parse_args()

    spark = get_spark_session("clean_orders")
    try:
        bd, base = args.batch_date, args.base_dir
        orders, items = clean_orders(
            spark,
            raw_orders_path=f"{base}/raw/orders/batch_date={bd}/orders.csv",
            raw_items_path=f"{base}/raw/order_items/batch_date={bd}/order_items.csv",
            clean_customers_path=f"{base}/processed/customers/batch_date={bd}/",
            clean_products_path=f"{base}/processed/products/batch_date={bd}/",
        )
        write_processed_parquet(orders, f"{base}/processed/orders/batch_date={bd}/", target_files=1)
        write_processed_parquet(items, f"{base}/processed/order_items/batch_date={bd}/", target_files=1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
