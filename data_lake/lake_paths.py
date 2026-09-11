"""
lake_paths.py — Single source of truth for the Data Lake's S3 key
structure (Section 6 of the spec):

    s3://customer-360-data/
        raw/<dataset>/batch_date=<YYYY-MM-DD>/<dataset>.<csv|json>
        processed/<dataset>/batch_date=<YYYY-MM-DD>/...          (Parquet, Phase 5)
        curated/<dataset>/...                                     (Parquet, Phase 8-9)

Centralizing key-building here means the local generator (Phase 2),
the lake sync script (this phase), and the future Spark jobs (Phase 5)
all agree on the exact same paths — one function to change if the
layout ever moves, instead of hunting for hard-coded prefixes across
the codebase.
"""
from __future__ import annotations

RAW_DATASETS = [
    "customers", "products", "orders", "order_items", "payments",
    "website_events", "customer_support", "marketing_campaigns",
    "campaign_interactions",
]

PROCESSED_DATASETS = ["customers", "orders", "events"]

CURATED_DATASETS = ["customer_360", "customer_rfm", "customer_segments", "customer_metrics"]

RAW_FORMAT = {
    "customers": "csv", "products": "csv", "orders": "csv", "order_items": "csv",
    "payments": "csv", "customer_support": "csv", "marketing_campaigns": "csv",
    "campaign_interactions": "csv", "website_events": "json",
}


def raw_prefix(dataset: str, batch_date: str) -> str:
    """S3 key prefix for one dataset's raw-layer partition."""
    return f"raw/{dataset}/batch_date={batch_date}/"


def raw_key(dataset: str, batch_date: str) -> str:
    """Full S3 key for a raw-layer file (matches data_generator's single-file-per-partition output)."""
    fmt = RAW_FORMAT[dataset]
    return f"{raw_prefix(dataset, batch_date)}{dataset}.{fmt}"


def processed_prefix(dataset: str, batch_date: str) -> str:
    """S3 key prefix for one dataset's processed-layer (Parquet) partition."""
    return f"processed/{dataset}/batch_date={batch_date}/"


def curated_prefix(dataset: str) -> str:
    """S3 key prefix for a curated (Gold) dataset. Not batch-partitioned —
    curated tables are full-refresh snapshots of "the current truth"."""
    return f"curated/{dataset}/"


def local_raw_path(base_data_dir: str, dataset: str, batch_date: str) -> str:
    """Local filesystem path matching data_generator/utils.get_output_dir's layout."""
    fmt = RAW_FORMAT[dataset]
    return f"{base_data_dir}/raw/{dataset}/batch_date={batch_date}/{dataset}.{fmt}"
