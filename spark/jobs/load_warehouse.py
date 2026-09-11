"""
load_warehouse.py — Loads the Phase 5 cleaned `data/processed/*`
Parquet output into the Postgres Star Schema (`warehouse.dim_*` /
`warehouse.fact_*`), completing the pipeline: raw -> Spark clean ->
processed Parquet -> **this** -> warehouse -> SQL Analytics (Phase 10).

Full-refresh model (documented in README's Phase 3 section): every
run TRUNCATEs the dimensions (CASCADE, which also clears every fact
table that references them) and reloads everything from the current
processed Parquet. This project's data_generator regenerates a full
synthetic history each run rather than "just today's new rows", so a
full refresh is the correct match for the data's actual shape — an
incremental upsert would be solving a problem this pipeline doesn't
have, at the cost of real complexity (SCD logic, merge conflict
handling) that would need testing of its own.

Load order matters: dimensions first (so their surrogate keys exist
to look up), then facts in an order where each fact's own FK targets
(including other already-loaded facts, e.g. order_items -> orders)
are already in place.

Usage:
    python -m spark.jobs.load_warehouse --base-dir data
"""
from __future__ import annotations

import argparse
import io
import logging
import os

import pandas as pd
import psycopg2

logger = logging.getLogger("load_warehouse")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")

STATIC_DEVICES = ["mobile", "desktop", "tablet", "Unknown"]


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "customer360"),
        user=os.environ.get("POSTGRES_USER", "customer360_app"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


def _map_key(series: pd.Series, lookup: dict) -> pd.Series:
    """Map a natural-key column to its surrogate key via `lookup`,
    returning pandas' nullable Int64 dtype.

    Plain `.map()` against an int-valued dict silently upcasts the
    whole column to float64 the moment any value is missing (NaN) —
    turning `108` into the string `"108.0"` once written to CSV,
    which COPY then rejects for an integer column. Int64 (capital I,
    pandas' nullable integer type) keeps whole numbers whole while
    still allowing NULLs, which plain int64 can't do at all.
    """
    return series.map(lookup).astype("Int64")


def _copy_dataframe(conn, df: pd.DataFrame, table: str, columns: list[str]) -> None:
    if df.empty:
        logger.info("Skipping %s: nothing to load", table)
        return
    buf = io.StringIO()
    df[columns].to_csv(buf, index=False, header=False, na_rep="\\N")
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    conn.commit()
    logger.info("Loaded %s rows -> %s", len(df), table)


def _read(base_dir: str, dataset: str) -> pd.DataFrame:
    return pd.read_parquet(f"{base_dir}/processed/{dataset}/")


def _fetch_lookup(conn, table: str, natural_key: str, surrogate_key: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {natural_key}, {surrogate_key} FROM {table}")
        return dict(cur.fetchall())


def truncate_warehouse(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE TABLE warehouse.dim_customer, warehouse.dim_product, "
            "warehouse.dim_campaign, warehouse.dim_location, warehouse.dim_device "
            "RESTART IDENTITY CASCADE"
        )
    conn.commit()
    logger.info("Truncated warehouse dimensions (and, via CASCADE, all dependent fact tables)")


def load_dimensions(conn, base_dir: str) -> dict[str, dict]:
    customers = _read(base_dir, "customers")
    products = _read(base_dir, "products")
    campaigns = _read(base_dir, "marketing_campaigns")

    # ---- dim_location: distinct city/province/country combos from customers ----
    locations = customers[["city", "province", "country"]].dropna().drop_duplicates()
    _copy_dataframe(conn, locations, "warehouse.dim_location", ["city", "province", "country"])
    location_lookup = {
        (city, province, country): key
        for city, province, country, key in _select_all(conn, "SELECT city, province, country, location_key FROM warehouse.dim_location")
    }

    # ---- dim_device: small static set ----
    device_df = pd.DataFrame({"device_name": STATIC_DEVICES})
    _copy_dataframe(conn, device_df, "warehouse.dim_device", ["device_name"])
    device_lookup = _fetch_lookup(conn, "warehouse.dim_device", "device_name", "device_key")

    # ---- dim_customer ----
    customers = customers.copy()
    customers["location_key"] = customers.apply(
        lambda r: location_lookup.get((r["city"], r["province"], r["country"])), axis=1
    ).astype("Int64")
    _copy_dataframe(
        conn, customers, "warehouse.dim_customer",
        ["customer_id", "first_name", "last_name", "email", "phone", "gender",
         "date_of_birth", "location_key", "registration_date", "customer_status"],
    )
    customer_lookup = _fetch_lookup(conn, "warehouse.dim_customer", "customer_id", "customer_key")

    # ---- dim_product ----
    _copy_dataframe(
        conn, products, "warehouse.dim_product",
        ["product_id", "product_name", "category", "subcategory", "brand", "cost", "selling_price"],
    )
    product_lookup = _fetch_lookup(conn, "warehouse.dim_product", "product_id", "product_key")

    # ---- dim_campaign ----
    _copy_dataframe(
        conn, campaigns, "warehouse.dim_campaign",
        ["campaign_id", "campaign_name", "channel", "start_date", "end_date", "budget"],
    )
    campaign_lookup = _fetch_lookup(conn, "warehouse.dim_campaign", "campaign_id", "campaign_key")

    return {
        "customer": customer_lookup,
        "product": product_lookup,
        "campaign": campaign_lookup,
        "device": device_lookup,
    }


def _select_all(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def _date_key(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series).dt.strftime("%Y%m%d").astype("Int64")


def load_facts(conn, base_dir: str, lookups: dict[str, dict]) -> None:
    customer_key = lookups["customer"]
    product_key = lookups["product"]
    campaign_key = lookups["campaign"]
    device_key = lookups["device"]

    # ---- fact_orders ----
    orders = _read(base_dir, "orders").copy()
    orders["customer_key"] = _map_key(orders["customer_id"], customer_key)
    orders["order_date_key"] = _date_key(orders["order_date"])
    before = len(orders)
    orders = orders.dropna(subset=["customer_key"])  # NOT NULL FK — should be a no-op given Phase 5 already validated this
    if len(orders) != before:
        logger.warning("[fact_orders] dropped %s rows with unmapped customer_key (unexpected — check Phase 5 output)", before - len(orders))
    _copy_dataframe(
        conn, orders, "warehouse.fact_orders",
        ["order_id", "customer_key", "order_date_key", "order_date", "order_status", "payment_method", "total_amount"],
    )
    order_key_lookup = _fetch_lookup(conn, "warehouse.fact_orders", "order_id", "order_key")

    # ---- fact_order_items ----
    items = _read(base_dir, "order_items").copy()
    items["order_key"] = _map_key(items["order_id"], order_key_lookup)
    items["product_key"] = _map_key(items["product_id"], product_key)
    items = items.dropna(subset=["order_key"])
    _copy_dataframe(
        conn, items, "warehouse.fact_order_items",
        ["order_item_id", "order_key", "product_key", "quantity", "unit_price", "discount"],
    )

    # ---- fact_payments ----
    payments = _read(base_dir, "payments").copy()
    payments["order_key"] = _map_key(payments["order_id"], order_key_lookup)
    payments["payment_date_key"] = _date_key(payments["payment_date"])
    payments = payments.dropna(subset=["order_key"])
    _copy_dataframe(
        conn, payments, "warehouse.fact_payments",
        ["payment_id", "order_key", "payment_date_key", "payment_date", "payment_method", "payment_status", "amount"],
    )

    # ---- fact_support_tickets ----
    tickets = _read(base_dir, "customer_support").copy()
    tickets["customer_key"] = _map_key(tickets["customer_id"], customer_key)
    tickets["created_date_key"] = _date_key(tickets["created_at"])
    _copy_dataframe(
        conn, tickets, "warehouse.fact_support_tickets",
        ["ticket_id", "customer_key", "created_date_key", "created_at", "issue_type", "priority", "status", "resolution_time"],
    )

    # ---- fact_campaign_interactions ----
    interactions = _read(base_dir, "campaign_interactions").copy()
    interactions["campaign_key"] = _map_key(interactions["campaign_id"], campaign_key)
    interactions["customer_key"] = _map_key(interactions["customer_id"], customer_key)
    _copy_dataframe(
        conn, interactions, "warehouse.fact_campaign_interactions",
        ["interaction_id", "campaign_key", "customer_key", "interaction_type", "interaction_timestamp"],
    )

    # ---- fact_website_events ----
    events = _read(base_dir, "events").copy()
    events["customer_key"] = _map_key(events["customer_id"], customer_key)
    events["product_key"] = _map_key(events["product_id"], product_key)
    events["device_key"] = _map_key(events["device"], device_key).fillna(device_key.get("Unknown")).astype("Int64")
    events["event_date_key"] = _date_key(events["event_timestamp"])
    events = events.dropna(subset=["event_timestamp"])
    _copy_dataframe(
        conn, events, "warehouse.fact_website_events",
        ["event_id", "customer_key", "session_id", "event_date_key", "event_timestamp",
         "event_type", "page", "product_key", "device_key", "traffic_source"],
    )


def load_warehouse(base_dir: str = "data") -> None:
    conn = get_connection()
    try:
        truncate_warehouse(conn)
        lookups = load_dimensions(conn, base_dir)
        load_facts(conn, base_dir, lookups)
        logger.info("=== Warehouse load complete ===")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load processed Parquet into the Postgres warehouse (full refresh).")
    parser.add_argument("--base-dir", default="data")
    args = parser.parse_args()
    load_warehouse(args.base_dir)


if __name__ == "__main__":
    main()
