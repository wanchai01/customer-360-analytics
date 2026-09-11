"""
run_all.py — Runs every Spark cleaning/transform job for one batch_date,
in dependency order. This is what the future Airflow `spark_transform`
task (Phase 7) will call — one entry point instead of six separate
DAG tasks with fragile ordering to get right by hand.

Dependency order (why it's not alphabetical):
  1. clean_customers, clean_products   — no dependencies on other
                                          cleaned datasets
  2. clean_orders                      — validates customer_id/product_id
                                          against the *cleaned* outputs
                                          of step 1
  3. clean_payments, clean_customer_support,
     clean_marketing, transform_events — validate against cleaned
                                          customers and/or orders from
                                          steps 1-2

A single SparkSession is created once and reused across every job
(each job's own __main__ creates and stops its own session when run
standalone — this orchestrator instead calls each job's importable
function directly to avoid seven JVM startup/teardown cycles).

Usage:
    python -m spark.jobs.run_all --batch-date 2026-09-06
"""
from __future__ import annotations

import argparse
import time

from spark.jobs.clean_customer_support import clean_customer_support
from spark.jobs.clean_customers import clean_customers
from spark.jobs.clean_marketing import clean_marketing
from spark.jobs.clean_orders import clean_orders
from spark.jobs.clean_payments import clean_payments
from spark.jobs.clean_products import clean_products
from spark.jobs.transform_events import build_daily_top_products, build_session_summary, clean_events
from spark.utils.cleaning import logger, write_processed_parquet
from spark.utils.spark_session import get_spark_session


def run_all(batch_date: str, base_dir: str = "data") -> dict[str, int]:
    spark = get_spark_session("run_all")
    row_counts: dict[str, int] = {}
    started = time.time()

    def p(layer: str, dataset: str) -> str:
        return f"{base_dir}/{layer}/{dataset}/batch_date={batch_date}/"

    try:
        logger.info("=== Spark ETL run_all: batch_date=%s ===", batch_date)

        # ---- step 1: no cross-dataset dependencies ----
        customers, _ = clean_customers(spark, f"{base_dir}/raw/customers/batch_date={batch_date}/customers.csv")
        write_processed_parquet(customers, p("processed", "customers"))
        row_counts["customers"] = customers.count()

        products = clean_products(spark, f"{base_dir}/raw/products/batch_date={batch_date}/products.csv")
        write_processed_parquet(products, p("processed", "products"))
        row_counts["products"] = products.count()

        # ---- step 2: depends on cleaned customers + products ----
        orders, items = clean_orders(
            spark,
            raw_orders_path=f"{base_dir}/raw/orders/batch_date={batch_date}/orders.csv",
            raw_items_path=f"{base_dir}/raw/order_items/batch_date={batch_date}/order_items.csv",
            clean_customers_path=p("processed", "customers"),
            clean_products_path=p("processed", "products"),
        )
        write_processed_parquet(orders, p("processed", "orders"))
        write_processed_parquet(items, p("processed", "order_items"))
        row_counts["orders"] = orders.count()
        row_counts["order_items"] = items.count()

        # ---- step 3: depends on cleaned customers and/or orders ----
        payments = clean_payments(
            spark,
            raw_path=f"{base_dir}/raw/payments/batch_date={batch_date}/payments.csv",
            clean_orders_path=p("processed", "orders"),
        )
        write_processed_parquet(payments, p("processed", "payments"))
        row_counts["payments"] = payments.count()

        tickets = clean_customer_support(
            spark,
            raw_path=f"{base_dir}/raw/customer_support/batch_date={batch_date}/customer_support.csv",
            clean_customers_path=p("processed", "customers"),
        )
        write_processed_parquet(tickets, p("processed", "customer_support"))
        row_counts["customer_support"] = tickets.count()

        campaigns, interactions = clean_marketing(
            spark,
            raw_campaigns_path=f"{base_dir}/raw/marketing_campaigns/batch_date={batch_date}/marketing_campaigns.csv",
            raw_interactions_path=f"{base_dir}/raw/campaign_interactions/batch_date={batch_date}/campaign_interactions.csv",
            clean_customers_path=p("processed", "customers"),
        )
        write_processed_parquet(campaigns, p("processed", "marketing_campaigns"))
        write_processed_parquet(interactions, p("processed", "campaign_interactions"))
        row_counts["marketing_campaigns"] = campaigns.count()
        row_counts["campaign_interactions"] = interactions.count()

        events = clean_events(
            spark,
            raw_path=f"{base_dir}/raw/website_events/batch_date={batch_date}/website_events.json",
            clean_customers_path=p("processed", "customers"),
        )
        session_summary = build_session_summary(events)
        daily_top_products = build_daily_top_products(events)
        write_processed_parquet(events, p("processed", "events"))
        write_processed_parquet(session_summary, p("processed", "session_summary"))
        write_processed_parquet(daily_top_products, p("processed", "daily_top_products"))
        row_counts["website_events"] = events.count()
        row_counts["session_summary"] = session_summary.count()
        row_counts["daily_top_products"] = daily_top_products.count()

        elapsed = time.time() - started
        logger.info("=== Spark ETL run_all complete in %.1fs. Row counts: %s ===", elapsed, row_counts)
        return row_counts
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full Spark cleaning/transform pipeline for one batch_date.")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--base-dir", default="data")
    args = parser.parse_args()
    run_all(args.batch_date, args.base_dir)


if __name__ == "__main__":
    main()
