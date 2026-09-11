"""
generate_all.py — CLI entry point that generates every dataset in the
correct dependency order and writes it to `data/raw/<dataset>/batch_date=...`,
mirroring the S3 raw layer layout described in docs/architecture.md.

Usage:
    python -m data_generator.generate_all --scale test
    python -m data_generator.generate_all --scale dev --dirty-rate 0.03
    python -m data_generator.generate_all --scale large --out-dir /opt/data

Re-running with the same --batch-date overwrites only that partition,
so the pipeline is idempotent (Section 8 requirement: re-runnable
without duplicating data).
"""
from __future__ import annotations

import argparse
from datetime import date, datetime

from data_generator import config as cfg
from data_generator.customer_support import generate_customer_support
from data_generator.customers import generate_customers
from data_generator.marketing import generate_campaign_interactions, generate_campaigns
from data_generator.orders import generate_orders_and_items
from data_generator.payments import generate_payments
from data_generator.products import generate_products
from data_generator.utils import get_output_dir, logger, set_global_seed, write_dataframe
from data_generator.website_events import generate_website_events

# Raw-layer format per dataset (Section 7 of the spec: raw layer is CSV/JSON)
_RAW_FORMAT = {
    "customers": "csv",
    "orders": "csv",
    "order_items": "csv",
    "products": "csv",
    "payments": "csv",
    "website_events": "json",       # clickstream is naturally record-oriented JSON
    "customer_support": "csv",
    "marketing_campaigns": "csv",
    "campaign_interactions": "csv",
}


def run(scale_name: str, out_dir: str, batch_date: str, seed: int, dirty_rate: float) -> dict[str, int]:
    """Generate all datasets for a given scale and write them to the raw layer."""
    scale = cfg.SCALE_PRESETS[scale_name]
    set_global_seed(seed)
    as_of = datetime.fromisoformat(batch_date)

    logger.info("=== Generating scale='%s' -> %s (batch_date=%s) ===", scale_name, out_dir, batch_date)

    row_counts: dict[str, int] = {}

    def _save(dataset: str, df):
        dest = get_output_dir(out_dir, "raw", dataset, batch_date)
        write_dataframe(df, dest, dataset, fmt=_RAW_FORMAT[dataset])
        row_counts[dataset] = len(df)

    customers_df = generate_customers(scale.n_customers, seed=seed, dirty_rate=dirty_rate)
    _save("customers", customers_df)

    products_df = generate_products(scale.n_products, seed=seed, dirty_rate=dirty_rate)
    _save("products", products_df)

    orders_df, order_items_df = generate_orders_and_items(
        customers_df, products_df, scale.n_orders, seed=seed, dirty_rate=dirty_rate, as_of=as_of
    )
    _save("orders", orders_df)
    _save("order_items", order_items_df)

    payments_df = generate_payments(orders_df, seed=seed, dirty_rate=dirty_rate)
    _save("payments", payments_df)

    events_df = generate_website_events(
        customers_df, products_df, scale.n_website_events, seed=seed, dirty_rate=dirty_rate, as_of=as_of
    )
    _save("website_events", events_df)

    tickets_df = generate_customer_support(
        customers_df, scale.n_support_tickets, seed=seed, dirty_rate=dirty_rate, as_of=as_of
    )
    _save("customer_support", tickets_df)

    campaigns_df = generate_campaigns(scale.n_campaigns, seed=seed)
    _save("marketing_campaigns", campaigns_df)

    interactions_df = generate_campaign_interactions(
        campaigns_df, customers_df, scale.n_campaign_interactions, seed=seed, dirty_rate=dirty_rate
    )
    _save("campaign_interactions", interactions_df)

    logger.info("=== Done. Row counts: %s ===", row_counts)
    return row_counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic Customer 360 datasets.")
    parser.add_argument("--scale", choices=list(cfg.SCALE_PRESETS.keys()), default="test",
                        help="Volume preset (see config.py SCALE_PRESETS).")
    parser.add_argument("--out-dir", default="data", help="Base output directory (default: ./data).")
    parser.add_argument("--batch-date", default=date.today().isoformat(),
                        help="Partition date, YYYY-MM-DD (default: today). Re-running the same "
                             "batch-date overwrites only that partition (idempotent).")
    parser.add_argument("--seed", type=int, default=cfg.RANDOM_SEED, help="Random seed for reproducibility.")
    parser.add_argument("--dirty-rate", type=float, default=cfg.DEFAULT_DIRTY_RATE,
                        help="Fraction of records with intentional data-quality issues (0 disables).")
    args = parser.parse_args()

    run(args.scale, args.out_dir, args.batch_date, args.seed, args.dirty_rate)


if __name__ == "__main__":
    main()
