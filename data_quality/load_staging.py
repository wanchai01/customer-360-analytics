"""
load_staging.py — Loads raw (dirty, as-generated) data into the
`staging.*` Postgres tables, exactly as it appears in
`data/raw/<dataset>/batch_date=.../` — nulls, duplicates, orphan
foreign keys, negative amounts, and all.

This is deliberately NOT the same code path as the Spark cleaning
jobs (Phase 5): staging is supposed to hold the dirty data so the
Data Quality checks in this phase have something real to measure.
Cleaning happens downstream of staging, not before it.

Loading uses PostgreSQL's COPY (via psycopg2's copy_expert), the
fastest bulk-load path Postgres offers — far faster than row-by-row
INSERTs for the row counts this project targets.

Usage:
    python -m data_quality.load_staging --batch-date 2026-09-06
"""
from __future__ import annotations

import argparse
import io
import logging
import os

import pandas as pd
import psycopg2

from data_lake.lake_paths import RAW_DATASETS, RAW_FORMAT, local_raw_path

logger = logging.getLogger("data_quality")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")

# Maps dataset name -> (staging table, ordered column list matching the
# staging DDL in sql/staging/01_staging_tables.sql, excluding the
# _batch_date/_loaded_at audit columns which this loader fills in itself).
STAGING_TABLE = {
    "customers": ("staging.stg_customers", [
        "customer_id", "first_name", "last_name", "email", "phone", "gender",
        "date_of_birth", "city", "province", "country", "registration_date", "customer_status",
    ]),
    "products": ("staging.stg_products", [
        "product_id", "product_name", "category", "subcategory", "brand", "cost", "selling_price",
    ]),
    "orders": ("staging.stg_orders", [
        "order_id", "customer_id", "order_date", "order_status", "payment_method", "total_amount",
    ]),
    "order_items": ("staging.stg_order_items", [
        "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount",
    ]),
    "payments": ("staging.stg_payments", [
        "payment_id", "order_id", "payment_date", "payment_method", "payment_status", "amount",
    ]),
    "website_events": ("staging.stg_website_events", [
        "event_id", "customer_id", "session_id", "event_timestamp", "event_type",
        "page", "product_id", "device", "traffic_source",
    ]),
    "customer_support": ("staging.stg_customer_support", [
        "ticket_id", "customer_id", "created_at", "issue_type", "priority", "status", "resolution_time",
    ]),
    "marketing_campaigns": ("staging.stg_marketing_campaigns", [
        "campaign_id", "campaign_name", "channel", "start_date", "end_date", "budget",
    ]),
    "campaign_interactions": ("staging.stg_campaign_interactions", [
        "interaction_id", "campaign_id", "customer_id", "interaction_type", "interaction_timestamp",
    ]),
}


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "customer360"),
        user=os.environ.get("POSTGRES_USER", "customer360_app"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


def _read_raw(dataset: str, base_dir: str, batch_date: str) -> pd.DataFrame:
    fmt = RAW_FORMAT[dataset]
    path = local_raw_path(base_dir, dataset, batch_date)
    if fmt == "csv":
        return pd.read_csv(path)
    return pd.read_json(path, lines=True)


def load_dataset(conn, dataset: str, base_dir: str, batch_date: str) -> int:
    """Load one dataset's raw file into its staging table for `batch_date`.
    Idempotent: deletes any existing rows for this batch_date first, so
    re-running the same batch overwrites rather than duplicates."""
    table, columns = STAGING_TABLE[dataset]
    source_path = local_raw_path(base_dir, dataset, batch_date)
    df = _read_raw(dataset, base_dir, batch_date)
    df = df[columns].copy()
    df["_batch_date"] = batch_date
    # _source_file: which raw file this row came from — the DDL
    # (sql/staging/01_staging_tables.sql) already had this column, but
    # nothing populated it, so every row's provenance was silently
    # NULL. Filling it in properly rather than dropping the column,
    # since "which file did this row come from" is a real, useful
    # question during a data-quality investigation.
    df["_source_file"] = source_path

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)

    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table} WHERE _batch_date = %s", (batch_date,))
        cur.copy_expert(
            f"COPY {table} ({', '.join(columns)}, _batch_date, _source_file) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
            buf,
        )
    conn.commit()

    logger.info("Loaded %s rows into %s for batch_date=%s", len(df), table, batch_date)
    return len(df)


def load_all(base_dir: str, batch_date: str) -> dict[str, int]:
    conn = get_connection()
    row_counts: dict[str, int] = {}
    try:
        for dataset in RAW_DATASETS:
            row_counts[dataset] = load_dataset(conn, dataset, base_dir, batch_date)
    finally:
        conn.close()
    logger.info("=== Staging load complete for batch_date=%s: %s ===", batch_date, row_counts)
    return row_counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw data into Postgres staging tables.")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--base-dir", default="data")
    args = parser.parse_args()
    load_all(args.base_dir, args.batch_date)


if __name__ == "__main__":
    main()
