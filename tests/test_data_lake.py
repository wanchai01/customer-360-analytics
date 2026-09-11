"""
test_data_lake.py — Tests for the data_lake package using `moto` to
mock AWS S3 (no real AWS account, MinIO container, or network access
needed to run these — they exercise the actual boto3 call sequence
against a realistic in-memory S3 implementation).

Run with:  pytest tests/test_data_lake.py -v
"""
from __future__ import annotations

import os

import pytest
from moto import mock_aws

from data_lake import lake_paths
from data_lake.s3_client import ensure_bucket, get_s3_client, list_objects
from data_lake.sync_to_lake import sync_batch


@pytest.fixture(autouse=True)
def aws_env(monkeypatch):
    """Fake AWS credentials — moto intercepts calls before they hit
    the network, but boto3 still requires *some* credentials to be
    present to construct a client."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-1")
    monkeypatch.setenv("AWS_S3_BUCKET", "customer-360-data-test")
    monkeypatch.delenv("AWS_S3_ENDPOINT_URL", raising=False)


@pytest.fixture
def local_batch(tmp_path):
    """Write a tiny fake raw-layer batch to a temp dir, matching the
    real data_generator's output layout, for sync_to_lake to upload."""
    batch_date = "2026-09-06"
    for dataset in lake_paths.RAW_DATASETS:
        fmt = lake_paths.RAW_FORMAT[dataset]
        part_dir = tmp_path / "raw" / dataset / f"batch_date={batch_date}"
        part_dir.mkdir(parents=True)
        (part_dir / f"{dataset}.{fmt}").write_text('{"dummy": "row"}\n' if fmt == "json" else "col1,col2\n1,2\n")
    return str(tmp_path), batch_date


# --------------------------------------------------------------------
# lake_paths — pure functions, no AWS needed
# --------------------------------------------------------------------
def test_raw_key_uses_correct_format_per_dataset():
    assert lake_paths.raw_key("customers", "2026-09-06") == "raw/customers/batch_date=2026-09-06/customers.csv"
    assert lake_paths.raw_key("website_events", "2026-09-06") == "raw/website_events/batch_date=2026-09-06/website_events.json"


def test_processed_and_curated_prefixes():
    assert lake_paths.processed_prefix("orders", "2026-09-06") == "processed/orders/batch_date=2026-09-06/"
    assert lake_paths.curated_prefix("customer_360") == "curated/customer_360/"


# --------------------------------------------------------------------
# s3_client — against moto's mocked S3
# --------------------------------------------------------------------
@mock_aws
def test_ensure_bucket_is_idempotent():
    client = get_s3_client()
    ensure_bucket(client, "my-test-bucket")
    # calling it again must not raise
    ensure_bucket(client, "my-test-bucket")
    buckets = {b["Name"] for b in client.list_buckets()["Buckets"]}
    assert "my-test-bucket" in buckets


@mock_aws
def test_list_objects_empty_bucket_returns_empty_list():
    client = get_s3_client()
    ensure_bucket(client, "empty-bucket")
    assert list_objects(client, "empty-bucket", prefix="raw/") == []


# --------------------------------------------------------------------
# sync_to_lake — the actual upload workflow
# --------------------------------------------------------------------
@mock_aws
def test_sync_batch_uploads_every_dataset(local_batch):
    base_dir, batch_date = local_batch
    uploaded = sync_batch(batch_date, base_dir=base_dir)
    assert set(uploaded.keys()) == set(lake_paths.RAW_DATASETS)

    client = get_s3_client()
    keys = list_objects(client, "customer-360-data-test", prefix="raw/")
    assert len(keys) == len(lake_paths.RAW_DATASETS)
    assert "raw/customers/batch_date=2026-09-06/customers.csv" in keys
    assert "raw/website_events/batch_date=2026-09-06/website_events.json" in keys


@mock_aws
def test_sync_batch_is_idempotent_on_rerun(local_batch):
    base_dir, batch_date = local_batch
    sync_batch(batch_date, base_dir=base_dir)
    # re-running for the same batch_date should succeed and not duplicate keys
    uploaded_again = sync_batch(batch_date, base_dir=base_dir)
    assert len(uploaded_again) == len(lake_paths.RAW_DATASETS)

    client = get_s3_client()
    keys = list_objects(client, "customer-360-data-test", prefix="raw/")
    assert len(keys) == len(lake_paths.RAW_DATASETS)  # no duplicates


@mock_aws
def test_sync_batch_skips_missing_dataset_gracefully(tmp_path):
    """If only some datasets were generated locally, sync should upload
    what exists and skip the rest without crashing."""
    batch_date = "2026-09-06"
    part_dir = tmp_path / "raw" / "customers" / f"batch_date={batch_date}"
    part_dir.mkdir(parents=True)
    (part_dir / "customers.csv").write_text("col1\n1\n")

    uploaded = sync_batch(batch_date, base_dir=str(tmp_path))
    assert uploaded == {"customers": "raw/customers/batch_date=2026-09-06/customers.csv"}
