"""
s3_client.py — boto3 client factory for the Data Lake.

The single design goal here: code written against this client should
run unchanged in local development (MinIO) and in AWS (real S3). The
only difference between the two environments is the presence of
AWS_S3_ENDPOINT_URL — when set, boto3 talks to MinIO; when unset, it
talks to real AWS S3 using standard AWS endpoint resolution.

This is what lets Section 2 of the spec ("build locally, deploy to
AWS later") actually work in practice rather than just being a
README claim.
"""
from __future__ import annotations

import logging
import os

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

logger = logging.getLogger("data_lake")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")


def get_s3_client():
    """
    Build a boto3 S3 client from environment variables.

    - AWS_S3_ENDPOINT_URL set  -> MinIO (or any S3-compatible store):
      uses path-style addressing, which MinIO requires.
    - AWS_S3_ENDPOINT_URL unset -> real AWS S3: standard virtual-hosted
      addressing, region resolved from AWS_DEFAULT_REGION.
    """
    endpoint_url = os.environ.get("AWS_S3_ENDPOINT_URL") or None
    region = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")

    kwargs = {
        "service_name": "s3",
        "region_name": region,
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID") or None,
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY") or None,
    }

    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        kwargs["config"] = Config(s3={"addressing_style": "path"})

    return boto3.client(**kwargs)


def get_bucket_name() -> str:
    return os.environ.get("AWS_S3_BUCKET", "customer-360-data")


def list_objects(client=None, bucket: str | None = None, prefix: str = "") -> list[str]:
    """List all object keys under a prefix (handles pagination)."""
    client = client or get_s3_client()
    bucket = bucket or get_bucket_name()
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def ensure_bucket(client=None, bucket: str | None = None) -> None:
    """
    Create the bucket if it doesn't already exist.

    Safe to call every run (idempotent): MinIO/S3 both return a
    "bucket already owned by you" style error on a duplicate create,
    which we treat as success. In real AWS, bucket creation is
    normally a one-time Terraform/console step, not something the
    application does on every run — but keeping this idempotent
    check means local dev (MinIO, no pre-provisioning) and prod
    behave the same way from the application's point of view.
    """
    client = client or get_s3_client()
    bucket = bucket or get_bucket_name()
    try:
        client.head_bucket(Bucket=bucket)
        logger.info("Bucket '%s' already exists.", bucket)
    except ClientError:
        logger.info("Bucket '%s' not found — creating it.", bucket)
        region = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")
        create_kwargs = {"Bucket": bucket}
        # us-east-1 is the one AWS region that rejects an explicit
        # LocationConstraint on bucket creation; every other region
        # (including MinIO, which ignores this anyway) requires it.
        if region != "us-east-1":
            create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
        client.create_bucket(**create_kwargs)
        logger.info("Created bucket '%s'.", bucket)
