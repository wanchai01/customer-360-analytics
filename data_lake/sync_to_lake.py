"""
sync_to_lake.py — Uploads a local `data/raw/<dataset>/batch_date=.../`
partition (produced by data_generator/generate_all.py) to the S3 Data
Lake's raw/ prefix, preserving the same key structure.

Usage:
    python -m data_lake.sync_to_lake --batch-date 2026-09-06
    python -m data_lake.sync_to_lake --batch-date 2026-09-06 --base-dir data

Uploading is idempotent by design: re-running for the same
--batch-date simply overwrites the same S3 keys (S3 PutObject has no
"already exists" failure mode), matching the same idempotent-by-
partition model used by the data generator and the Postgres DDL.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from data_lake.lake_paths import RAW_DATASETS, local_raw_path, raw_key
from data_lake.s3_client import ensure_bucket, get_bucket_name, get_s3_client, logger


def sync_batch(batch_date: str, base_dir: str = "data") -> dict[str, str]:
    """Upload every raw dataset's file for `batch_date` to the lake. Returns {dataset: s3_key} for what was uploaded."""
    client = get_s3_client()
    bucket = get_bucket_name()
    ensure_bucket(client, bucket)

    uploaded: dict[str, str] = {}
    for dataset in RAW_DATASETS:
        local_path = Path(local_raw_path(base_dir, dataset, batch_date))
        if not local_path.exists():
            logger.warning("Skipping '%s': no local file at %s (did generate_all.py run for this batch_date?)", dataset, local_path)
            continue

        key = raw_key(dataset, batch_date)
        client.upload_file(str(local_path), bucket, key)
        size_mb = local_path.stat().st_size / (1024 * 1024)
        logger.info("Uploaded %s (%.2f MB) -> s3://%s/%s", local_path.name, size_mb, bucket, key)
        uploaded[dataset] = key

    logger.info("=== Synced %s/%s datasets for batch_date=%s to s3://%s/ ===", len(uploaded), len(RAW_DATASETS), batch_date, bucket)
    return uploaded


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync local raw data to the S3 Data Lake.")
    parser.add_argument("--batch-date", required=True, help="Partition date to sync, YYYY-MM-DD.")
    parser.add_argument("--base-dir", default="data", help="Local base data directory (default: ./data).")
    args = parser.parse_args()

    sync_batch(args.batch_date, args.base_dir)


if __name__ == "__main__":
    main()
